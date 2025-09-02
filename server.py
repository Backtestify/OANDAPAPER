from __future__ import annotations
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from core_trader import (
    RiskConfig, PaperBroker, PaperBrokerConfig, getOandaBrokerFromEnv,
    getOandaPriceFeedFromEnv, PaperPriceFeed, StateStore, TriangularGridStrategy,
    _parseBoolEnv, loadEnvVariables
)
import logging

class TraderService:
    def __init__(self, dbPath: str, usePaperBroker: bool, pollIntervalSec: float, historyDays: int, riskConfig: RiskConfig):
        self.dbPath = dbPath
        self.usePaperBroker = usePaperBroker
        self.pollIntervalSec = pollIntervalSec
        self.historyDays = historyDays
        self.riskConfig = riskConfig
        self.stateStore = StateStore(dbPath)
        self.executionBroker = PaperBroker(PaperBrokerConfig()) if usePaperBroker else getOandaBrokerFromEnv()
        self.priceFeed = PaperPriceFeed() if usePaperBroker else getOandaPriceFeedFromEnv()
        self.strategy = TriangularGridStrategy(riskConfig=riskConfig, executionBroker=self.executionBroker, stateStore=self.stateStore)
        self._thread: Optional[threading.Thread] = None
        self._stopFlag = threading.Event()
        self.lock = threading.RLock()
        self._ensureGridAndHistory()

    def _ensureGridAndHistory(self):
        levels = self.stateStore.loadGridLevels()
        if levels:
            self.strategy.gridLevels = levels
            self.strategy.strategyState = self.stateStore.loadStrategyState()
            return
        if self.usePaperBroker:
            now = datetime.now(timezone.utc)
            usdcnh = 6.5
            usdjpy = 130.0
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
            from core_trader import fetchOandaDailyMidSeries, synthCnhJpy
            pf = getOandaPriceFeedFromEnv()
            usdjpySeries = fetchOandaDailyMidSeries(pf.accountId, pf.apiToken, pf.isPractice, "USD_JPY", self.historyDays)
            usdcnhSeries = fetchOandaDailyMidSeries(pf.accountId, pf.apiToken, pf.isPractice, "USD_CNH", self.historyDays)
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
        from core_trader import synthCnhJpy
        while not self._stopFlag.is_set():
            ts, usdcnh, usdjpy = self.priceFeed.getQuote()
            cnhjpy = synthCnhJpy(usdcnh, usdjpy)
            with self.lock:
                self.stateStore.saveHistoryPoint(ts, usdcnh, usdjpy, cnhjpy)
                self.strategy.onTick(usdcnh, usdjpy, ts)
            time.sleep(self.pollIntervalSec)

    def status(self) -> dict:
        with self.lock:
            held = sum(1 for l in self.strategy.gridLevels if l.levelState.name == "HELD")
            st = self.strategy.strategyState
            risk = {
                "totalCapitalUsd": self.riskConfig.totalCapitalUsd,
                "takeProfitPct": self.riskConfig.takeProfitPct,
                "leverageMultiplier": self.riskConfig.leverageMultiplier,
                "buyWagerUsd": self.riskConfig.buyWagerUsd,
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
                "tradesClosedCount": st.tradesClosedCount,
                "heldLevels": held,
                "previousSyntheticPrice": st.previousSyntheticPrice,
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

loadEnvVariables()
usePaper = _parseBoolEnv(os.getenv("USE_PAPER_BROKER"), True)
runOnStart = _parseBoolEnv(os.getenv("RUN_ON_START"), False)
logLevel = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, logLevel, logging.INFO))

service = TraderService(
    dbPath=os.getenv("DB_PATH", "triangular_grid.db"),
    usePaperBroker=usePaper,
    pollIntervalSec=float(os.getenv("POLL_INTERVAL_SEC", "3")),
    historyDays=int(os.getenv("HISTORY_DAYS", "730")),
    riskConfig=RiskConfig(
        totalCapitalUsd=float(os.getenv("TOTAL_CAPITAL_USD", "100000")),
        leverageMultiplier=float(os.getenv("LEVERAGE_MULTIPLIER", "2.0")),
        takeProfitPct=float(os.getenv("TAKE_PROFIT_PCT", "0.015")),
    ),
)

app = FastAPI(title="Triangular Grid Trader API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class UpdateRiskRequest(BaseModel):
    totalCapitalUsd: Optional[float] = None
    leverageMultiplier: Optional[float] = None
    takeProfitPct: Optional[float] = None
    rebuildGrid: Optional[bool] = None

@app.get("/status")
def api_status():
    return service.status()

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

@app.get("/history")
def api_history(days: int = 180, points: int = 2000):
    rows = service.stateStore.loadHistorySeries(days)
    n = len(rows)
    if n > points:
        step = max(1, n // points)
        rows = rows[::step]
    return {"timestamps": [int(t.timestamp()) for t, _ in rows], "cnhjpy": [v for _, v in rows]}

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
    try:
        service.executionBroker.closeAll(datetime.now(timezone.utc))
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, str(e))

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
    if changed and (req.rebuildGrid or False):
        with service.stateStore._connect() as cx:
            cx.execute("DELETE FROM grid_levels")
            cx.execute("DELETE FROM strategy_state")
        with service.lock:
            service.strategy.gridLevels = []
        service._ensureGridAndHistory()
    return {"ok": True, "risk": service.status()["risk"]}

if runOnStart:
    service.start()