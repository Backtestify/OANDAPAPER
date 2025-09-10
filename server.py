# === server.py ===
from __future__ import annotations

import os, json, time, threading, queue
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, Response, Request, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

from core_trader import (
    RiskConfig,
    StateStore,
    TriangularGridStrategy,
    OandaBroker,
    PaperBroker,
    PaperBrokerConfig,
    OandaPriceFeed,
    PaperPriceFeed,
    synthCnhJpy,
    fetchOandaDailyMidSeries,
)

# -----------------------------
# Environment / defaults
# -----------------------------
if load_dotenv is not None:
    load_dotenv()

backendPort = int(os.getenv("BACKEND_PORT", "8000"))
dbPath = os.getenv("DB_PATH", "triangular_grid.db")

usePaperBroker = os.getenv("BROKER", "OANDA").upper() == "PAPER"
oandaAccountId = os.getenv("OANDA_ACCOUNT_ID", "")
oandaApiToken = os.getenv("OANDA_API_TOKEN", "")
oandaPractice = os.getenv("OANDA_PRACTICE", "true").strip().lower() in {"1", "true", "t", "yes", "y"}

totalCapitalUsdDefault = float(os.getenv("TOTAL_CAPITAL_USD", "100000"))
leverageMultiplierDefault = float(os.getenv("RISK_LEVERAGE_MULTIPLIER", "1.0"))  # safe default 1x
takeProfitPctDefault = float(os.getenv("TAKE_PROFIT_PCT", "0.015"))
numBuyWagersDefault = int(os.getenv("NUM_BUY_WAGERS", "100"))
maxActiveCapitalFractionDefault = float(os.getenv("MAX_ACTIVE_CAPITAL_FRACTION", str(1/3)))
lookbackDaysDefault = int(os.getenv("LOOKBACK_DAYS", "730"))
minTickDefault = float(os.getenv("MIN_TICK", "0.00001"))
marginRateApproxDefault = float(os.getenv("MARGIN_RATE_APPROX", "0.05"))
marginUtilizationLimitDefault = float(os.getenv("MARGIN_UTILIZATION_LIMIT", "0.80"))
enforceEquityCapDefault = os.getenv("ENFORCE_EQUITY_CAP", "true").strip().lower() in {"1","true","t","yes","y"}

historyDaysBootstrap = int(os.getenv("HISTORY_DAYS", str(lookbackDaysDefault)))
workerPollSec = float(os.getenv("WORKER_POLL_SEC", "2.0"))

# -----------------------------
# Globals (app state)
# -----------------------------
app = FastAPI(title="Triangular Grid Trader API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("UI_ORIGIN", "*")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

stateStore = StateStore(dbPath=dbPath)

if usePaperBroker:
    executionBroker = PaperBroker(paperBrokerConfig=PaperBrokerConfig())
    priceFeed = PaperPriceFeed()
    brokerName = "Paper"
else:
    if not oandaAccountId or not oandaApiToken:
        raise RuntimeError("OANDA_ACCOUNT_ID and OANDA_API_TOKEN must be set for live/practice broker.")
    executionBroker = OandaBroker(accountId=oandaAccountId, apiToken=oandaApiToken, isPractice=oandaPractice)
    priceFeed = OandaPriceFeed(accountId=oandaAccountId, apiToken=oandaApiToken, isPractice=oandaPractice)
    brokerName = "OANDA"

riskConfig = RiskConfig(
    totalCapitalUsd=totalCapitalUsdDefault,
    maxActiveCapitalFraction=maxActiveCapitalFractionDefault,
    numBuyWagers=numBuyWagersDefault,
    takeProfitPct=takeProfitPctDefault,
    lookbackDays=lookbackDaysDefault,
    minTick=minTickDefault,
    leverageMultiplier=leverageMultiplierDefault,
    marginRateApprox=marginRateApproxDefault,
    marginUtilizationLimit=marginUtilizationLimitDefault,
    enforceEquityCap=enforceEquityCapDefault,
    centerGridOnCurrent=True,  # Option A default
)

strategy = TriangularGridStrategy(
    riskConfig=riskConfig,
    executionBroker=executionBroker,
    stateStore=stateStore,
)

# Background worker controls
workerRunningFlag = threading.Event()
workerThread: Optional[threading.Thread] = None
workerLock = threading.Lock()  # guards onTick + mutable strategy fields

# SSE event bus
eventQueue: "queue.Queue[dict]" = queue.Queue(maxsize=1024)

def _emitEvent(evt: dict):
    try:
        eventQueue.put_nowait(evt)
    except queue.Full:
        # Drop oldest to keep bus moving
        try:
            _ = eventQueue.get_nowait()
        except Exception:
            pass
        try:
            eventQueue.put_nowait(evt)
        except Exception:
            pass

# -----------------------------
# Bootstrap grid (history)
# -----------------------------
def _buildSyntheticHistory(days: int) -> List[Tuple[datetime, float]]:
    # Use cached DB history if present
    series = stateStore.loadHistorySeries(days)
    if len(series) >= 2:
        return series

    # Otherwise fetch from broker (OANDA)
    if usePaperBroker:
        # Paper: synth fake flat history around current
        now = datetime.now(timezone.utc)
        base = 19.5
        out: List[Tuple[datetime, float]] = []
        for i in range(days):
            t = now - timedelta(days=days - i)
            # light random walk around base
            base += 0.01 if i % 10 == 0 else 0.0
            out.append((t, base))
        return out

    usdcnh = fetchOandaDailyMidSeries(oandaAccountId, oandaApiToken, oandaPractice, "USD_CNH", days)
    usdjpy = fetchOandaDailyMidSeries(oandaAccountId, oandaApiToken, oandaPractice, "USD_JPY", days)

    # align on timestamps (day)
    def toDayKey(t: datetime) -> str:
        return t.astimezone(timezone.utc).strftime("%Y-%m-%d")

    mapCnh = {toDayKey(t): v for t, v in usdcnh}
    mapJpy = {toDayKey(t): v for t, v in usdjpy}
    keys = sorted(set(mapCnh.keys()) & set(mapJpy.keys()))
    out: List[Tuple[datetime, float]] = []
    for k in keys:
        t = datetime.fromisoformat(k + "T00:00:00+00:00")
        cnh = mapCnh[k]
        jpy = mapJpy[k]
        try:
            cnhjpy = synthCnhJpy(cnh, jpy)
        except Exception:
            continue
        out.append((t, cnhjpy))

    # persist to DB
    for t, cnhjpy in out:
        # recreate midpoints inserted
        # we don't have the exact usdcnh/usdjpy for each row here; store derived only
        stateStore.saveHistoryPoint(t, usdcnh=0.0, usdjpy=0.0, cnhjpy=cnhjpy)

    return out

def _bootstrapGrid():
    history = _buildSyntheticHistory(riskConfig.lookbackDays)
    if len(history) < 2:
        raise RuntimeError("Not enough history to bootstrap grid.")
    strategy.bootstrapGrid(history)

# initialize on import
try:
    _bootstrapGrid()
except Exception as e:
    # Defer bootstrap errors to first /start if network hiccups
    print(f"[WARN] bootstrap failed: {e}")

# -----------------------------
# Worker loop
# -----------------------------
def _computeHeldLevels() -> int:
    return sum(1 for lvl in strategy.gridLevels if lvl.levelState.name == "HELD")

def _estimateMargin() -> Tuple[float, float, float]:
    held = _computeHeldLevels()
    perLevel = riskConfig.buyWagerUsd * riskConfig.leverageMultiplier * riskConfig.marginRateApprox
    used = held * perLevel
    limit = riskConfig.totalCapitalUsd * riskConfig.marginUtilizationLimit
    pct = 0.0 if limit <= 0 else min(used / limit, 9.99)
    return used, limit, pct

def _flatIfBrokerFlat():
    """If broker positions are (near) flat, snap local levels/state to flat."""
    try:
        brokerJ, brokerC = executionBroker.netUnits()
    except Exception:
        return

    tol = 5  # small tolerance on units
    if abs(brokerJ) <= tol and abs(brokerC) <= tol:
        # Snap all to IDLE
        changed = False
        for lvl in strategy.gridLevels:
            if lvl.levelState.name == "HELD" or lvl.levelEntrySyntheticPrice is not None or lvl.levelTakeProfitSyntheticPrice is not None:
                lvl.levelState = type(lvl.levelState).IDLE
                lvl.levelEntrySyntheticPrice = None
                lvl.levelTakeProfitSyntheticPrice = None
                changed = True
        if changed:
            stateStore.saveGridLevels(strategy.gridLevels)
        # Reset capital usage
        st = strategy.strategyState
        st.activeCapitalUsdUnlevered = 0.0
        stateStore.saveStrategyState(st)

def workerLoop():
    lastOpened = 0
    lastClosed = 0
    _emitEvent({"type": "worker", "status": "started", "ts": time.time()})
    while workerRunningFlag.is_set():
        try:
            ts, usdcnh, usdjpy = priceFeed.getQuote()
            synthetic = synthCnhJpy(usdcnh, usdjpy)
            stateStore.saveHistoryPoint(ts, usdcnh=usdcnh, usdjpy=usdjpy, cnhjpy=synthetic)

            with workerLock:
                beforeOpen = strategy.strategyState.tradesOpenedCount
                beforeClose = strategy.strategyState.tradesClosedCount
                strategy.onTick(usdcnhPrice=usdcnh, usdjpyPrice=usdjpy, timestamp=ts)
                afterOpen = strategy.strategyState.tradesOpenedCount
                afterClose = strategy.strategyState.tradesClosedCount

            # Emit events if anything changed
            if afterOpen != beforeOpen or afterClose != beforeClose:
                _emitEvent({
                    "type": "execution",
                    "openedDelta": afterOpen - beforeOpen,
                    "closedDelta": afterClose - beforeClose,
                    "ts": time.time(),
                })

            # Opportunistically snap to flat if broker is flat
            _flatIfBrokerFlat()

        except Exception as e:
            _emitEvent({"type": "error", "detail": str(e), "ts": time.time()})
        time.sleep(workerPollSec)
    _emitEvent({"type": "worker", "status": "stopped", "ts": time.time()})

# -----------------------------
# Serialization helpers
# -----------------------------
def _levelToDict(lvl) -> dict:
    return {
        "levelPrice": float(lvl.levelPrice),
        "state": lvl.levelState.name,
        "entry": None if lvl.levelEntrySyntheticPrice is None else float(lvl.levelEntrySyntheticPrice),
        "takeProfit": None if lvl.levelTakeProfitSyntheticPrice is None else float(lvl.levelTakeProfitSyntheticPrice),
        "usdNotionalPerLeg": float(lvl.usdNotionalPerLeg),
    }

def _statusJson() -> Dict[str, Any]:
    used, limit, pct = _estimateMargin()
    held = _computeHeldLevels()
    st = strategy.strategyState
    hs = {
        "high": strategy.historicLossHigh,
        "low": strategy.historicLossLow,
        "buyIncrement": strategy.buyIncrement,
    }

    # Basic sync hint (non-fatal)
    sync = {"status": "OK", "detail": ""}
    try:
        bJ, bC = executionBroker.netUnits()
        # expected local
        localJ = 0
        localC = 0
        for lvl in strategy.gridLevels:
            if lvl.levelState.name == "HELD":
                localJ += int(round(abs(lvl.usdNotionalPerLeg)))
                localC -= int(round(abs(lvl.usdNotionalPerLeg)))
        tol = 5
        jOk = abs((bJ or 0) - localJ) <= tol
        cOkExact = abs((bC or 0) - localC) <= tol
        cOkAbs = abs(abs(bC or 0) - abs(localC)) <= tol
        if not (jOk and (cOkExact or cOkAbs)):
            sync = {
                "status": "WARN",
                "detail": f"broker J={bJ} C={bC} vs local J={localJ} C={localC} (sign-tolerant on CNH)",
            }
    except Exception as e:
        sync = {"status": "WARN", "detail": f"netUnits error: {e}"}

    return {
        "running": workerRunningFlag.is_set(),
        "broker": "Paper" if usePaperBroker else "OANDA",
        "risk": {
            "totalCapitalUsd": riskConfig.totalCapitalUsd,
            "buyWagerUsd": riskConfig.buyWagerUsd,
            "leverageMultiplier": riskConfig.leverageMultiplier,
            "takeProfitPct": riskConfig.takeProfitPct,
            "numBuyWagers": riskConfig.numBuyWagers,
            "marginRateApprox": riskConfig.marginRateApprox,
            "marginUtilizationLimit": riskConfig.marginUtilizationLimit,
            "perLevelMarginUsd": riskConfig.buyWagerUsd * riskConfig.leverageMultiplier * riskConfig.marginRateApprox,
        },
        "strategy": {
            "heldLevels": held,
            "tradesOpenedCount": st.tradesOpenedCount,
            "tradesClosedCount": st.tradesClosedCount,
            "activeCapitalUsdUnlevered": st.activeCapitalUsdUnlevered,
            "estMarginUsedUsd": used,
            "estMarginLimitUsd": limit,
            "estMarginUsedPct": pct,
            "previousSyntheticPrice": st.previousSyntheticPrice,
        },
        "hs": hs,
        "sync": sync,
    }

# -----------------------------
# Routes
# -----------------------------
@app.get("/status")
def get_status():
    return JSONResponse(_statusJson())

@app.get("/levels")
def get_levels():
    return JSONResponse([_levelToDict(l) for l in strategy.gridLevels])

@app.get("/executions")
def get_executions(limit: int = 100):
    return JSONResponse(stateStore.loadExecutions(limit=limit))

@app.post("/start")
def post_start():
    global workerThread
    if workerRunningFlag.is_set():
        return {"ok": True, "detail": "already running"}
    # Ensure grid exists (bootstrap if needed)
    try:
        if not strategy.gridLevels:
            _bootstrapGrid()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"bootstrap failed: {e}")

    workerRunningFlag.set()
    workerThread = threading.Thread(target=workerLoop, daemon=True)
    workerThread.start()
    _emitEvent({"type": "control", "action": "start", "ts": time.time()})
    return {"ok": True}

@app.post("/stop")
def post_stop():
    if not workerRunningFlag.is_set():
        return {"ok": True, "detail": "already stopped"}
    workerRunningFlag.clear()
    _emitEvent({"type": "control", "action": "stop", "ts": time.time()})
    return {"ok": True}

@app.post("/close-all")
def post_close_all():
    try:
        executionBroker.closeAll(datetime.now(timezone.utc))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"broker closeAll failed: {e}")

    # Give broker a moment, then verify and snap local if flat
    time.sleep(0.3)
    _flatIfBrokerFlat()

    # Recheck
    try:
        bJ, bC = executionBroker.netUnits()
    except Exception as e:
        return {"ok": False, "detail": f"netUnits failed: {e}", "sync": {"status": "ERROR", "detail": str(e)}}

    tol = 5
    if abs(bJ) <= tol and abs(bC) <= tol:
        _emitEvent({"type": "execution", "action": "closeAll", "ts": time.time()})
        return {"ok": True}
    else:
        return {
            "ok": False,
            "detail": f"broker still not flat J={bJ} C={bC}",
            "sync": {"status": "WARN", "detail": "manual intervention may be required"},
        }

@app.post("/risk")
def post_risk(body: Dict[str, Any]):
    # Mutate risk and optionally rebuild grid (centered on current)
    rebuild = bool(body.get("rebuildGrid", False))
    with workerLock:
        if "totalCapitalUsd" in body and float(body["totalCapitalUsd"]) > 0:
            riskConfig.totalCapitalUsd = float(body["totalCapitalUsd"])
        if "leverageMultiplier" in body and float(body["leverageMultiplier"]) > 0:
            riskConfig.leverageMultiplier = float(body["leverageMultiplier"])
        if "takeProfitPct" in body and float(body["takeProfitPct"]) > 0:
            riskConfig.takeProfitPct = float(body["takeProfitPct"])
        if "numBuyWagers" in body and int(body["numBuyWagers"]) > 0:
            riskConfig.numBuyWagers = int(body["numBuyWagers"])
        if "marginRateApprox" in body and float(body["marginRateApprox"]) > 0:
            riskConfig.marginRateApprox = float(body["marginRateApprox"])
        if "marginUtilizationLimit" in body and 0 < float(body["marginUtilizationLimit"]) <= 1.0:
            riskConfig.marginUtilizationLimit = float(body["marginUtilizationLimit"])
        if "centerGridOnCurrent" in body:
            riskConfig.centerGridOnCurrent = bool(body["centerGridOnCurrent"])

        if rebuild:
            history = _buildSyntheticHistory(riskConfig.lookbackDays)
            if len(history) < 2:
                raise HTTPException(status_code=400, detail="not enough history to rebuild grid")
            strategy.bootstrapGrid(history)

    _emitEvent({"type": "risk", "action": "update", "rebuild": rebuild, "ts": time.time()})
    return {"ok": True}

@app.post("/optimize-grid")
def post_optimize_grid(body: Dict[str, Any]):
    from core_trader import optimizeGridDensity  # import here to keep top tidy
    applyBest = bool(body.get("apply", False))
    minLevels = int(body["minLevels"]) if "minLevels" in body else None
    maxLevels = int(body["maxLevels"]) if "maxLevels" in body else None
    step = int(body["step"]) if "step" in body else None
    lookbackDays = int(body["lookbackDays"]) if "lookbackDays" in body else riskConfig.optimizationLookbackDays
    marginRateApprox = float(body["marginRateApprox"]) if "marginRateApprox" in body else None
    roundTripCostUsd = float(body["roundTripCostUsd"]) if "roundTripCostUsd" in body else None
    marginUtilLimit = float(body["marginUtilizationLimit"]) if "marginUtilizationLimit" in body else 0.8

    history = _buildSyntheticHistory(lookbackDays)
    if len(history) < 3:
        raise HTTPException(status_code=400, detail="not enough history for optimization")

    try:
        result = optimizeGridDensity(
            historySeries=history,
            riskConfig=riskConfig,
            minLevels=minLevels,
            maxLevels=maxLevels,
            step=step,
            marginRateApprox=marginRateApprox,
            roundTripCostUsd=roundTripCostUsd,
            marginUtilizationLimit=marginUtilLimit,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"optimize error: {e}")

    if applyBest:
        best = result.get("best", {})
        nbw = int(best.get("numBuyWagers", riskConfig.numBuyWagers))
        with workerLock:
            riskConfig.numBuyWagers = nbw
            history2 = _buildSyntheticHistory(riskConfig.lookbackDays)
            strategy.bootstrapGrid(history2)
        _emitEvent({"type": "risk", "action": "applyBest", "numBuyWagers": nbw, "ts": time.time()})
    return JSONResponse(result)

@app.post("/audit")
def post_audit():
    try:
        bJ, bC = executionBroker.netUnits()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"broker netUnits failed: {e}")

    # Local expected based on held levels
    localJ = 0
    localC = 0
    for lvl in strategy.gridLevels:
        if lvl.levelState.name == "HELD":
            notional = int(round(abs(lvl.usdNotionalPerLeg)))
            localJ += notional
            localC -= notional

    tol = 5
    jOk = abs(bJ - localJ) <= tol
    cOkExact = abs(bC - localC) <= tol
    cOkAbs = abs(abs(bC) - abs(localC)) <= tol  # tolerate sign mismatch for CNH leg
    if jOk and (cOkExact or cOkAbs):
        return {"status": "OK", "detail": f"broker J={bJ} C={bC} vs local J={localJ} C={localC} (CNH sign-tolerant)"}
    else:
        return {"status": "WARN", "detail": f"broker J={bJ} C={bC} vs local J={localJ} C={localC} — investigate"}

# -----------------------------
# SSE /events
# -----------------------------
async def _eventStream():
    # Server-Sent Events stream with keepalives
    keepAliveEvery = 15.0
    lastBeat = time.time()
    yield "retry: 2000\n\n"
    while True:
        # Non-blocking poll; if empty, send keepalive every keepAliveEvery seconds
        try:
            evt = eventQueue.get(timeout=0.5)
            payload = json.dumps(evt)
            yield f"data: {payload}\n\n"
        except queue.Empty:
            pass
        now = time.time()
        if now - lastBeat >= keepAliveEvery:
            lastBeat = now
            yield "data: {}\n\n"

@app.get("/events")
async def get_events():
    return StreamingResponse(_eventStream(), media_type="text/event-stream")
