# === server.py ===
from __future__ import annotations
import os, threading, time, json, asyncio, logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from core_trader import (
    RiskConfig, PaperBroker, PaperBrokerConfig, getOandaBrokerFromEnv,
    getOandaPriceFeedFromEnv, PaperPriceFeed, StateStore, TriangularGridStrategy,
    _parseBoolEnv, loadEnvVariables, synthCnhJpy, fetchOandaDailyMidSeries, LevelState,
    optimizeGridDensity
)

# ---------- Event bus (SSE push) ----------
class EventBus:
    def __init__(self):
        self.subscribers: set[asyncio.Queue[str]] = set()
        self.lock = asyncio.Lock()
        self.loop: Optional[asyncio.AbstractEventLoop] = None

    async def subscribe(self) -> asyncio.Queue[str]:
        q: asyncio.Queue[str] = asyncio.Queue()
        async with self.lock:
            self.subscribers.add(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue[str]):
        async with self.lock:
            self.subscribers.discard(q)

    async def _publishAsync(self, data: str):
        async with self.lock:
            for q in list(self.subscribers):
                await q.put(data)

    def publish(self, data: str):
        if self.loop:
            asyncio.run_coroutine_threadsafe(self._publishAsync(data), self.loop)

eventBus = EventBus()

# ---------- Trading service ----------
class TraderService:
    def __init__(self, dbPath: str, usePaperBroker: bool, pollIntervalSec: float, historyDays: int, riskConfig: RiskConfig, haltOnInconsistency: bool = True):
        self.dbPath = dbPath
        self.usePaperBroker = usePaperBroker
        self.pollIntervalSec = pollIntervalSec
        self.historyDays = historyDays
        self.riskConfig = riskConfig
        self.haltOnInconsistency = haltOnInconsistency

        self.stateStore = StateStore(dbPath)
        self.executionBroker = PaperBroker(PaperBrokerConfig()) if usePaperBroker else getOandaBrokerFromEnv()
        self.priceFeed = PaperPriceFeed() if usePaperBroker else getOandaPriceFeedFromEnv()
        self.strategy = TriangularGridStrategy(riskConfig=riskConfig, executionBroker=self.executionBroker, stateStore=self.stateStore)

        self._thread: Optional[threading.Thread] = None
        self._stopFlag = threading.Event()
        self.lock = threading.RLock()

        self.syncStatus: str = "OK"
        self.syncDetail: str = ""

        self._ensureGridAndHistory()
        self._reconcileExternalClosures()

    # ---- bootstrap/history ----
    def _ensureGridAndHistory(self):
        levels = self.stateStore.loadGridLevels()
        if levels:
            self.strategy.gridLevels = levels
            self.strategy.strategyState = self.stateStore.loadStrategyState()
            return

        if self.usePaperBroker:
            now = datetime.now(timezone.utc)
            usdcnh, usdjpy = 6.5, 130.0
            for i in range(self.historyDays):
                t = now - timedelta(days=self.historyDays - i)
                usdcnh = max(usdcnh + 0.001, 5.5)
                usdjpy = max(usdjpy + 0.05, 90)
                cnhjpy = usdjpy / usdcnh
                self.stateStore.saveHistoryPoint(t, usdcnh, usdjpy, cnhjpy)
            history = self.stateStore.loadHistorySeries(self.historyDays)
            self.strategy.bootstrapGrid(history)
            self.strategy.strategyState.previousSyntheticPrice = history[-1][1]
        else:
            usdjpySeries = fetchOandaDailyMidSeries(
                self.priceFeed.accountId, self.priceFeed.apiToken, self.priceFeed.isPractice, "USD_JPY", self.historyDays
            )
            usdcnhSeries = fetchOandaDailyMidSeries(
                self.priceFeed.accountId, self.priceFeed.apiToken, self.priceFeed.isPractice, "USD_CNH", self.historyDays
            )
            byJ = {t.date(): v for t, v in usdjpySeries}
            byC = {t.date(): v for t, v in usdcnhSeries}
            series = []
            for d in sorted(set(byJ) & set(byC)):
                series.append((datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc), synthCnhJpy(byC[d], byJ[d])))
            if len(series) < 2:
                raise RuntimeError("Not enough history to bootstrap grid")
            self.strategy.bootstrapGrid(series)
            for t, v in series:
                self.stateStore.saveHistoryPoint(t, byC[t.date()], byJ[t.date()], v)
            self.strategy.strategyState.previousSyntheticPrice = series[-1][1]

    # ---- account snapshot ----
    def _saveAccountSnapshot(self):
        try:
            snap = self.executionBroker.getAccountSummary()
            if snap:
                self.stateStore.saveAccountSnapshot(
                    datetime.now(timezone.utc),
                    float(snap.get("balance", 0.0)),
                    float(snap.get("NAV", snap.get("balance", 0.0))),
                    float(snap.get("unrealizedPL", 0.0)),
                    str(snap.get("currency", "USD")),
                )
        except Exception:
            pass

    # ---- reconciliation (handles broker flat & partial closes) ----
    def _reconcileExternalClosures(self):
        netJ, netC = self.executionBroker.netUnits()
        heldLevels = [l for l in self.strategy.gridLevels if l.levelState == LevelState.HELD]

        if netJ == 0 and netC == 0 and heldLevels:
            for lvl in heldLevels:
                lvl.levelState = LevelState.IDLE
                lvl.levelEntrySyntheticPrice = None
                lvl.levelTakeProfitSyntheticPrice = None
                self.strategy.strategyState.activeCapitalUsdUnlevered = max(
                    self.strategy.strategyState.activeCapitalUsdUnlevered - self.riskConfig.buyWagerUsd, 0.0
                )
                self.stateStore.recordExecution(
                    datetime.now(timezone.utc), "CLOSE_EXTERNAL",
                    lvl.levelPrice, -int(round(lvl.usdNotionalPerLeg)), +int(round(lvl.usdNotionalPerLeg)), "external"
                )
            self.stateStore.saveGridLevels(heldLevels)
            self.stateStore.saveStrategyState(self.strategy.strategyState)
            self._saveAccountSnapshot()
            return

        if not heldLevels:
            return

        perLeg = int(round(max(l.usdNotionalPerLeg for l in heldLevels)))
        if perLeg <= 0:
            return

        expectedJ = sum(int(round(+l.usdNotionalPerLeg)) for l in heldLevels)
        expectedC = sum(int(round(-l.usdNotionalPerLeg)) for l in heldLevels)
        diffJ = expectedJ - netJ
        diffC = expectedC - netC
        nClosed = min(max(abs(diffJ) // perLeg, 0), max(abs(diffC) // perLeg, 0))
        if nClosed <= 0:
            return

        toClose = [l for l in sorted(heldLevels, key=lambda x: x.levelPrice)][:nClosed]
        for lvl in toClose:
            lvl.levelState = LevelState.IDLE
            lvl.levelEntrySyntheticPrice = None
            lvl.levelTakeProfitSyntheticPrice = None
            self.strategy.strategyState.activeCapitalUsdUnlevered = max(
                self.strategy.strategyState.activeCapitalUsdUnlevered - self.riskConfig.buyWagerUsd, 0.0
            )
            self.stateStore.recordExecution(
                datetime.now(timezone.utc), "CLOSE_EXTERNAL",
                lvl.levelPrice, -int(round(lvl.usdNotionalPerLeg)), +int(round(lvl.usdNotionalPerLeg)), "external"
            )
        self.stateStore.saveGridLevels(toClose)
        self.stateStore.saveStrategyState(self.strategy.strategyState)

    # ---- audit consistency (can halt if configured) ----
    def _auditConsistency(self):
        netJ, netC = self.executionBroker.netUnits()
        heldLevels = [l for l in self.strategy.gridLevels if l.levelState == LevelState.HELD]
        expectedJ = sum(int(round(+l.usdNotionalPerLeg)) for l in heldLevels)
        expectedC = sum(int(round(-l.usdNotionalPerLeg)) for l in heldLevels)

        self.syncStatus, self.syncDetail = "OK", ""
        if netJ == 0 and netC == 0 and len(heldLevels) > 0:
            self.syncStatus = "RECOVERING"
            self.syncDetail = "Broker flat; local HELD > 0, awaiting reconcile"
            return

        perLeg = int(round(max((l.usdNotionalPerLeg for l in heldLevels), default=0)))
        tol = max(1, perLeg // 2) if perLeg > 0 else 1
        mismatchJ = expectedJ - netJ
        mismatchC = expectedC - netC
        if abs(mismatchJ) > tol or abs(mismatchC) > tol:
            self.syncStatus = "WARN"
            self.syncDetail = f"broker J={netJ} C={netC} vs local J={expectedJ} C={expectedC}"
            if self.haltOnInconsistency:
                self._stopFlag.set()

    # ---- lifecycle ----
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stopFlag.clear()
        self._thread = threading.Thread(target=self._runLoop, daemon=True)
        self._thread.start()

    def stop(self):
        if self._thread and self._thread.is_alive():
            self._stopFlag.set()
            self._thread.join(timeout=1.0)

    def _runLoop(self):
        while not self._stopFlag.is_set():
            ts, usdcnh, usdjpy = self.priceFeed.getQuote()
            cnhjpy = synthCnhJpy(usdcnh, usdjpy)
            with self.lock:
                self.stateStore.saveHistoryPoint(ts, usdcnh, usdjpy, cnhjpy)
                self._reconcileExternalClosures()
                self.strategy.onTick(usdcnh, usdjpy, ts)
                self._auditConsistency()
            time.sleep(self.pollIntervalSec)

    def status(self) -> dict:
        with self.lock:
            held = sum(1 for l in self.strategy.gridLevels if l.levelState.name == "HELD")
            st = self.strategy.strategyState
            closedBot = self.stateStore.countExecutionsByAction("CLOSE")
            closedExternal = self.stateStore.countExecutionsByAction("CLOSE_EXTERNAL")

            # Margin estimates for header
            perLevelMargin = self.riskConfig.buyWagerUsd * self.riskConfig.leverageMultiplier * self.riskConfig.marginRateApprox
            estMarginUsed = held * perLevelMargin
            estMarginLimit = self.riskConfig.totalCapitalUsd * self.riskConfig.marginUtilizationLimit
            estMarginUsedPct = (estMarginUsed / estMarginLimit) if estMarginLimit > 0 else 0.0

            risk = {
                "totalCapitalUsd": self.riskConfig.totalCapitalUsd,
                "takeProfitPct": self.riskConfig.takeProfitPct,
                "leverageMultiplier": self.riskConfig.leverageMultiplier,
                "buyWagerUsd": self.riskConfig.buyWagerUsd,
                "numBuyWagers": self.riskConfig.numBuyWagers,
                "marginRateApprox": self.riskConfig.marginRateApprox,
                "marginUtilizationLimit": self.riskConfig.marginUtilizationLimit,
                "enforceEquityCap": self.riskConfig.enforceEquityCap,
                "perLevelMarginUsd": perLevelMargin,
            }
            hs = {
                "high": self.strategy.historicLossHigh,
                "low": self.strategy.historicLossLow,
                "buyIncrement": self.strategy.buyIncrement,
                "timeHigh": None if self.strategy.historicLossHighTime is None else self.strategy.historicLossHighTime.isoformat(),
                "timeLow": None if self.strategy.historicLossLowTime is None else self.strategy.historicLossLowTime.isoformat(),
            }
            strategyDict = {
                "activeCapitalUsdUnlevered": st.activeCapitalUsdUnlevered,
                "tradesOpenedCount": st.tradesOpenedCount,
                "tradesClosedCount": closedBot,
                "tradesClosedExternalCount": closedExternal,
                "heldLevels": held,
                "previousSyntheticPrice": st.previousSyntheticPrice,
                "estMarginUsedUsd": estMarginUsed,
                "estMarginLimitUsd": estMarginLimit,
                "estMarginUsedPct": estMarginUsedPct,
            }
        return {
            "running": bool(self._thread and self._thread.is_alive()),
            "broker": "Paper" if self.usePaperBroker else "OANDA",
            "dbPath": self.dbPath,
            "pollIntervalSec": self.pollIntervalSec,
            "historyDays": self.historyDays,
            "risk": risk,
            "hs": hs,
            "strategy": strategyDict,
        }

# ---------- App + config ----------
loadEnvVariables()
usePaper = _parseBoolEnv(os.getenv("USE_PAPER_BROKER"), True)
runOnStart = _parseBoolEnv(os.getenv("RUN_ON_START"), False)
haltOnInconsistency = _parseBoolEnv(os.getenv("HALT_ON_INCONSISTENCY"), True)
logLevel = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, logLevel, logging.INFO))

service = TraderService(
    dbPath=os.getenv("DB_PATH", "triangular_grid.db"),
    usePaperBroker=usePaper,
    pollIntervalSec=float(os.getenv("POLL_INTERVAL_SEC", "3")),
    historyDays=int(os.getenv("HISTORY_DAYS", "730")),
    riskConfig=RiskConfig(
        totalCapitalUsd=float(os.getenv("TOTAL_CAPITAL_USD", "100000")),
        leverageMultiplier=float(os.getenv("LEVERAGE_MULTIPLIER", "1.0")),  # default 1.0x
        takeProfitPct=float(os.getenv("TAKE_PROFIT_PCT", "0.015")),
        numBuyWagers=int(os.getenv("NUM_BUY_WAGERS", "100")),
        marginRateApprox=float(os.getenv("MARGIN_RATE_APPROX", "0.035")),
        marginUtilizationLimit=float(os.getenv("MARGIN_UTILIZATION_LIMIT", "0.80")),
        enforceEquityCap=_parseBoolEnv(os.getenv("ENFORCE_EQUITY_CAP"), True),
    ),
    haltOnInconsistency=haltOnInconsistency,
)

app = FastAPI(title="Triangular Grid Trader API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Models ----------
class UpdateRiskRequest(BaseModel):
    totalCapitalUsd: Optional[float] = None
    leverageMultiplier: Optional[float] = None
    takeProfitPct: Optional[float] = None
    numBuyWagers: Optional[int] = None
    rebuildGrid: Optional[bool] = None

class OptimizeGridRequest(BaseModel):
    minLevels: Optional[int] = None
    maxLevels: Optional[int] = None
    step: Optional[int] = None
    lookbackDays: Optional[int] = None
    marginRateApprox: Optional[float] = None
    roundTripCostUsd: Optional[float] = None
    marginUtilizationLimit: Optional[float] = 0.8
    apply: Optional[bool] = False

# ---------- Lifecycle ----------
@app.on_event("startup")
async def onStartup():
    eventBus.loop = asyncio.get_running_loop()

# ---------- SSE events ----------
@app.get("/events")
async def apiEvents():
    q = await eventBus.subscribe()
    async def gen():
        try:
            while True:
                data = await q.get()
                yield f"data: {data}\n\n"
        finally:
            await eventBus.unsubscribe(q)
    return StreamingResponse(gen(), media_type="text/event-stream")

# ---------- REST endpoints ----------
@app.get("/status")
def api_status():
    s = service.status()
    latest = service.stateStore.loadLatestAccountSnapshot()
    s["account"] = latest
    s["sync"] = {"status": service.syncStatus, "detail": service.syncDetail}
    return s

@app.get("/levels")
def api_levels():
    with service.lock:
        return [
            {
                "levelPrice": l.levelPrice,
                "state": l.levelState.name,
                "entry": l.levelEntrySyntheticPrice,
                "takeProfit": l.levelTakeProfitSyntheticPrice,
                "usdNotionalPerLeg": l.usdNotionalPerLeg,
            }
            for l in service.strategy.gridLevels
        ]

@app.get("/executions")
def api_executions(limit: int = 100):
    return service.stateStore.loadExecutions(limit=limit)

@app.get("/executions/last")
def api_last_execution_ts():
    rows = service.stateStore.loadExecutions(limit=1)
    return {"ts": rows[0]["ts"] if rows else 0}

@app.get("/history")
def api_history(days: int = 180, points: int = 2000):
    rows = service.stateStore.loadHistorySeries(days)
    n = len(rows)
    if n > points:
        step = max(1, n // points)
        rows = rows[::step]
    return {"timestamps": [int(t.timestamp()) for t, _ in rows], "cnhjpy": [v for _, v in rows]}

@app.get("/account/latest")
def api_account_latest():
    return service.stateStore.loadLatestAccountSnapshot() or {}

@app.post("/audit")
def api_audit():
    with service.lock:
        service._reconcileExternalClosures()
        service._auditConsistency()
    return {"status": service.syncStatus, "detail": service.syncDetail}

@app.post("/start")
def api_start():
    service.start()
    return {"ok": True}

@app.post("/stop")
def api_stop():
    service.stop()
    return {"ok": True}

@app.post("/close-all")
def api_close_all():
    service.executionBroker.closeAll(datetime.now(timezone.utc))
    time.sleep(0.8)  # allow fills to settle
    with service.lock:
        service._reconcileExternalClosures()
        netJ, netC = self.executionBroker.netUnits()
        held = sum(1 for l in service.strategy.gridLevels if l.levelState.name == "HELD")
        flatNow = (netJ == 0 and netC == 0 and held == 0)
        if flatNow:
            service._saveAccountSnapshot()
            service._auditConsistency()
            return {"ok": True}
        service._auditConsistency()
        return {
            "ok": False,
            "detail": f"Not flat after close-all: netJ={netJ}, netC={netC}, heldLevels={held}",
            "sync": {"status": service.syncStatus, "detail": service.syncDetail},
        }

@app.post("/risk")
def api_update_risk(req: UpdateRiskRequest):
    changed = False
    if req.totalCapitalUsd is not None:
        service.riskConfig.totalCapitalUsd = float(req.totalCapitalUsd)
        changed = True
    if req.leverageMultiplier is not None:
        service.riskConfig.leverageMultiplier = float(req.leverageMultiplier)
        changed = True
    if req.takeProfitPct is not None:
        service.riskConfig.takeProfitPct = float(req.takeProfitPct)
        changed = True
    if req.numBuyWagers is not None:
        service.riskConfig.numBuyWagers = int(req.numBuyWagers)
        changed = True
    if changed and (req.rebuildGrid or False):
        with service.stateStore._connect() as cx:
            cx.execute("DELETE FROM grid_levels")
            cx.execute("DELETE FROM strategy_state")
        with service.lock:
            service.strategy.gridLevels = []
        service._ensureGridAndHistory()
    return {"ok": True, "risk": service.status()["risk"]}

@app.post("/optimize-grid")
def api_optimize_grid(req: OptimizeGridRequest):
    days = int(req.lookbackDays or service.riskConfig.optimizationLookbackDays)
    rows = service.stateStore.loadHistorySeries(days)
    if len(rows) < 3:
        raise HTTPException(400, "not enough history for optimization")
    try:
        result = optimizeGridDensity(
            rows,
            service.riskConfig,
            minLevels=req.minLevels,
            maxLevels=req.maxLevels,
            step=req.step,
            marginRateApprox=req.marginRateApprox,
            roundTripCostUsd=req.roundTripCostUsd,
            marginUtilizationLimit=float(req.marginUtilizationLimit or 0.8),
        )
    except Exception as e:
        raise HTTPException(400, str(e))
    if req.apply:
        bestN = int(result["best"]["numBuyWagers"])
        service.riskConfig.numBuyWagers = bestN
        with service.stateStore._connect() as cx:
            cx.execute("DELETE FROM grid_levels")
            cx.execute("DELETE FROM strategy_state")
        with service.lock:
            service.strategy.gridLevels = []
        service._ensureGridAndHistory()
    return result

# ---------- Publish SSE on each execution ----------
def installExecutionHook():
    original = service.stateStore.recordExecution
    def hooked(ts, action, levelPrice, unitsUsdJpy, unitsUsdCnh, clientTag):
        original(ts, action, levelPrice, unitsUsdJpy, unitsUsdCnh, clientTag)
        try:
            payload = json.dumps({
                "type": "execution", "action": action,
                "ts": int(ts.timestamp()), "levelPrice": levelPrice
            })
            eventBus.publish(payload)
        except Exception:
            pass
    service.stateStore.recordExecution = hooked

installExecutionHook()

if runOnStart:
    service.start()
