# === core_trader.py ===
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional, Tuple
from collections import deque
from datetime import datetime, timedelta, timezone
import uuid, os, sqlite3, requests

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


class LevelState(Enum):
    IDLE = auto()
    HELD = auto()


@dataclass
class RiskConfig:
    totalCapitalUsd: float
    maxActiveCapitalFraction: float = 1 / 3
    numBuyWagers: int = 100
    takeProfitPct: float = 0.015
    lookbackDays: int = 730
    minTick: float = 1e-5
    leverageMultiplier: float = 1.0
    gridLevelsMin: int = 50
    gridLevelsMax: int = 400
    gridLevelsStep: int = 25
    optimizationLookbackDays: int = 365
    roundTripCostUsd: float = 1.0
    marginRateApprox: float = 0.035
    optimizeMetric: str = "evPerDay"
    marginUtilizationLimit: float = 0.80
    enforceEquityCap: bool = True

    @property
    def buyWagerUsd(self) -> float:
        return self.totalCapitalUsd * (self.maxActiveCapitalFraction / self.numBuyWagers)


@dataclass
class PaperBrokerConfig:
    spreadUsdcnh: float = 0.0
    spreadUsdjpy: float = 0.0


class Broker:
    def executeTwoLegMarket(self, unitsUsdJpy: int, unitsUsdCnh: int, clientTag: str, timestamp: datetime) -> bool:
        raise NotImplementedError

    def closeAll(self, timestamp: datetime):
        raise NotImplementedError

    def netUnits(self) -> Tuple[int, int]:
        raise NotImplementedError

    def getAccountSummary(self) -> dict:
        raise NotImplementedError


@dataclass
class PaperBroker(Broker):
    paperBrokerConfig: PaperBrokerConfig
    openUsdJpyUnits: int = 0
    openUsdCnhUnits: int = 0

    def executeTwoLegMarket(self, unitsUsdJpy: int, unitsUsdCnh: int, clientTag: str, timestamp: datetime) -> bool:
        self.openUsdJpyUnits += unitsUsdJpy
        self.openUsdCnhUnits += unitsUsdCnh
        return True

    def closeAll(self, timestamp: datetime):
        self.openUsdJpyUnits = 0
        self.openUsdCnhUnits = 0

    def netUnits(self) -> Tuple[int, int]:
        return self.openUsdJpyUnits, self.openUsdCnhUnits

    def getAccountSummary(self) -> dict:
        return {"balance": 0.0, "NAV": 0.0, "unrealizedPL": 0.0, "currency": "USD"}


@dataclass
class OandaBroker(Broker):
    accountId: str
    apiToken: str
    isPractice: bool = True
    requestTimeoutSec: int = 10

    def _apiHost(self) -> str:
        return "https://api-fxpractice.oanda.com" if self.isPractice else "https://api-fxtrade.oanda.com"

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.apiToken}", "Content-Type": "application/json"}

    def _newClientExtensions(self, clientTag: str) -> dict:
        return {"id": f"{clientTag}-{uuid.uuid4().hex[:12]}", "tag": clientTag, "comment": "triangular-grid"}

    def _postOrder(
        self,
        instrument: str,
        units: int,
        orderType: str = "MARKET",
        price: Optional[float] = None,
        tif: str = "FOK",
        clientTag: str = "tg",
    ) -> dict:
        url = f"{self._apiHost()}/v3/accounts/{self.accountId}/orders"
        order: dict = {
            "instrument": instrument,
            "units": str(units),
            "type": orderType,
            "timeInForce": tif,
            "clientExtensions": self._newClientExtensions(clientTag),
        }
        if orderType in ("LIMIT", "MARKET_IF_TOUCHED", "STOP") and price is not None:
            order["price"] = f"{price:.6f}"
        r = requests.post(url, headers=self._headers(), json={"order": order}, timeout=self.requestTimeoutSec)
        if r.status_code >= 300:
            raise RuntimeError(f"OANDA order error {r.status_code}: {r.text}")
        return r.json()

    def _closePosition(self, instrument: str) -> None:
        url = f"{self._apiHost()}/v3/accounts/{self.accountId}/positions/{instrument}/close"
        r = requests.put(
            url,
            headers=self._headers(),
            json={"longUnits": "ALL", "shortUnits": "ALL"},
            timeout=self.requestTimeoutSec,
        )
        if r.status_code >= 300:
            raise RuntimeError(f"OANDA close error {r.status_code}: {r.text}")

    def _getPosition(self, instrument: str) -> int:
        url = f"{self._apiHost()}/v3/accounts/{self.accountId}/positions/{instrument}"
        r = requests.get(url, headers=self._headers(), timeout=self.requestTimeoutSec)
        if r.status_code == 404:
            return 0
        if r.status_code >= 300:
            raise RuntimeError(f"OANDA positions error {r.status_code}: {r.text}")
        data = r.json().get("position", {})
        longUnits = int(float(data.get("long", {}).get("units", "0")))
        shortUnits = int(float(data.get("short", {}).get("units", "0")))
        return longUnits - shortUnits

    def getAccountSummary(self) -> dict:
        url = f"{self._apiHost()}/v3/accounts/{self.accountId}/summary"
        r = requests.get(url, headers=self._headers(), timeout=self.requestTimeoutSec)
        if r.status_code >= 300:
            raise RuntimeError(f"OANDA summary error {r.status_code}: {r.text}")
        acc = r.json().get("account", {})
        def fnum(k: str, default: float = 0.0) -> float:
            try:
                return float(acc.get(k, default))
            except Exception:
                return default
        return {
            "balance": fnum("balance"),
            "NAV": fnum("NAV", fnum("balance")),
            "unrealizedPL": fnum("unrealizedPL"),
            "currency": acc.get("currency", "USD"),
            "marginAvailable": fnum("marginAvailable") if "marginAvailable" in acc else None,
            "marginUsed": fnum("marginUsed") if "marginUsed" in acc else None,
        }

    def executeTwoLegMarket(self, unitsUsdJpy: int, unitsUsdCnh: int, clientTag: str, timestamp: datetime) -> bool:
        try:
            jpyFill = self._postOrder("USD_JPY", unitsUsdJpy, orderType="MARKET", clientTag=f"{clientTag}-jpy")
            try:
                _ = self._postOrder("USD_CNH", unitsUsdCnh, orderType="MARKET", clientTag=f"{clientTag}-cnh")
                return True
            except Exception:
                undoUnits = -int(jpyFill.get("orderFillTransaction", {}).get("units", unitsUsdJpy))
                try:
                    self._postOrder("USD_JPY", undoUnits, orderType="MARKET", clientTag=f"{clientTag}-undo-jpy")
                finally:
                    pass
                return False
        except Exception:
            return False

    def closeAll(self, timestamp: datetime):
        for inst in ("USD_JPY", "USD_CNH"):
            try:
                self._closePosition(inst)
            except Exception:
                pass
        for inst in ("USD_JPY", "USD_CNH"):
            try:
                net = self._getPosition(inst)
                if net != 0:
                    self._postOrder(inst, -net, orderType="MARKET", clientTag=f"tg-force-close-{inst.lower()}")
            except Exception:
                pass

    def netUnits(self) -> Tuple[int, int]:
        return self._getPosition("USD_JPY"), self._getPosition("USD_CNH")


@dataclass
class GridLevel:
    levelPrice: float
    levelState: LevelState = LevelState.IDLE
    levelEntrySyntheticPrice: Optional[float] = None
    levelTakeProfitSyntheticPrice: Optional[float] = None
    usdNotionalPerLeg: float = 0.0


@dataclass
class StrategyState:
    previousSyntheticPrice: Optional[float] = None
    activeCapitalUsdUnlevered: float = 0.0
    tradesOpenedCount: int = 0
    tradesClosedCount: int = 0


class PriceFeed:
    def getQuote(self) -> Tuple[datetime, float, float]:
        raise NotImplementedError


@dataclass
class OandaPriceFeed(PriceFeed):
    accountId: str
    apiToken: str
    isPractice: bool = True
    requestTimeoutSec: int = 10

    def _apiHost(self) -> str:
        return "https://api-fxpractice.oanda.com" if self.isPractice else "https://api-fxtrade.oanda.com"

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self.apiToken}"}

    def getQuote(self) -> Tuple[datetime, float, float]:
        url = f"{self._apiHost()}/v3/accounts/{self.accountId}/pricing"
        params = {"instruments": "USD_JPY,USD_CNH"}
        r = requests.get(url, headers=self._headers(), params=params, timeout=self.requestTimeoutSec)
        if r.status_code >= 300:
            raise RuntimeError(f"pricing error {r.status_code}: {r.text}")
        data = r.json()
        usdjpy = None
        usdcnh = None
        for inst in data.get("prices", []):
            instrument = inst.get("instrument")
            bids = inst.get("bids", [])
            asks = inst.get("asks", [])
            if not bids or not asks:
                continue
            mid = (float(bids[0]["price"]) + float(asks[0]["price"])) / 2.0
            if instrument == "USD_JPY":
                usdjpy = mid
            elif instrument == "USD_CNH":
                usdcnh = mid
        if usdjpy is None or usdcnh is None:
            raise RuntimeError("missing instrument in pricing")
        ts = datetime.now(timezone.utc)
        return ts, usdcnh, usdjpy


@dataclass
class PaperPriceFeed(PriceFeed):
    usdcnhStart: float = 6.5
    usdjpyStart: float = 130.0

    def getQuote(self) -> Tuple[datetime, float, float]:
        import random
        self.usdcnhStart = max(self.usdcnhStart + random.gauss(0, 0.002), 5.5)
        self.usdjpyStart = max(self.usdjpyStart + random.gauss(0, 0.1), 90)
        return datetime.now(timezone.utc), self.usdcnhStart, self.usdjpyStart


class StateStore:
    def __init__(self, dbPath: str):
        self.dbPath = dbPath
        self._initSchema()

    def _connect(self):
        return sqlite3.connect(self.dbPath, check_same_thread=False)

    def _initSchema(self):
        with self._connect() as cx:
            cx.execute("PRAGMA journal_mode=WAL;")
            cx.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
            cx.execute("CREATE TABLE IF NOT EXISTS history (ts INTEGER PRIMARY KEY, usdcnh REAL, usdjpy REAL, cnhjpy REAL)")
            cx.execute(
                "CREATE TABLE IF NOT EXISTS grid_levels (level_price REAL PRIMARY KEY, state TEXT, entry_px REAL, tp_px REAL, usd_notional_per_leg REAL)"
            )
            cx.execute(
                "CREATE TABLE IF NOT EXISTS strategy_state (id INTEGER PRIMARY KEY CHECK (id=1), prev_px REAL, active_cap_unlev REAL, opened INT, closed INT)"
            )
            cx.execute(
                "CREATE TABLE IF NOT EXISTS executions (id TEXT PRIMARY KEY, ts INTEGER, action TEXT, level_price REAL, units_usdjpy INT, units_usdcnh INT, client_tag TEXT)"
            )
            cx.execute(
                "CREATE TABLE IF NOT EXISTS account_snapshots (ts INTEGER PRIMARY KEY, balance REAL, nav REAL, unrealized REAL, currency TEXT)"
            )

    def saveMeta(self, key: str, value: str):
        with self._connect() as cx:
            cx.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def loadMeta(self, key: str) -> Optional[str]:
        with self._connect() as cx:
            row = cx.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row[0] if row else None

    def saveHistoryPoint(self, ts: datetime, usdcnh: float, usdjpy: float, cnhjpy: float):
        with self._connect() as cx:
            cx.execute(
                "INSERT OR REPLACE INTO history(ts,usdcnh,usdjpy,cnhjpy) VALUES(?,?,?,?)",
                (int(ts.timestamp()), usdcnh, usdjpy, cnhjpy),
            )

    def loadHistorySeries(self, days: int) -> List[Tuple[datetime, float]]:
        startTs = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
        with self._connect() as cx:
            rows = cx.execute("SELECT ts,cnhjpy FROM history WHERE ts>=? ORDER BY ts ASC", (startTs,)).fetchall()
            return [(datetime.fromtimestamp(r[0], tz=timezone.utc), float(r[1])) for r in rows]

    def saveGridLevels(self, levels: List[GridLevel]):
        with self._connect() as cx:
            for lvl in levels:
                cx.execute(
                    "INSERT OR REPLACE INTO grid_levels(level_price,state,entry_px,tp_px,usd_notional_per_leg) VALUES(?,?,?,?,?)",
                    (
                        lvl.levelPrice,
                        lvl.levelState.name,
                        None if lvl.levelEntrySyntheticPrice is None else float(lvl.levelEntrySyntheticPrice),
                        None if lvl.levelTakeProfitSyntheticPrice is None else float(lvl.levelTakeProfitSyntheticPrice),
                        lvl.usdNotionalPerLeg,
                    ),
                )

    def loadGridLevels(self) -> List[GridLevel]:
        with self._connect() as cx:
            rows = cx.execute(
                "SELECT level_price,state,entry_px,tp_px,usd_notional_per_leg FROM grid_levels ORDER BY level_price ASC"
            ).fetchall()
            out: List[GridLevel] = []
            for level_price, state, entry_px, tp_px, notional in rows:
                out.append(
                    GridLevel(
                        levelPrice=float(level_price),
                        levelState=LevelState[state],
                        levelEntrySyntheticPrice=None if entry_px is None else float(entry_px),
                        levelTakeProfitSyntheticPrice=None if tp_px is None else float(tp_px),
                        usdNotionalPerLeg=float(notional),
                    )
                )
            return out

    def saveStrategyState(self, st: StrategyState):
        with self._connect() as cx:
            cx.execute(
                "INSERT OR REPLACE INTO strategy_state(id,prev_px,active_cap_unlev,opened,closed) VALUES(1,?,?,?,?)",
                (
                    None if st.previousSyntheticPrice is None else float(st.previousSyntheticPrice),
                    st.activeCapitalUsdUnlevered,
                    st.tradesOpenedCount,
                    st.tradesClosedCount,
                ),
            )

    def loadStrategyState(self) -> StrategyState:
        with self._connect() as cx:
            row = cx.execute("SELECT prev_px,active_cap_unlev,opened,closed FROM strategy_state WHERE id=1").fetchone()
            if not row:
                return StrategyState()
            prev_px, active, opened, closed = row
            return StrategyState(
                previousSyntheticPrice=None if prev_px is None else float(prev_px),
                activeCapitalUsdUnlevered=float(active),
                tradesOpenedCount=int(opened),
                tradesClosedCount=int(closed),
            )

    def recordExecution(
        self, ts: datetime, action: str, levelPrice: float, unitsUsdJpy: int, unitsUsdCnh: int, clientTag: str
    ):
        with self._connect() as cx:
            cx.execute(
                "INSERT OR REPLACE INTO executions(id,ts,action,level_price,units_usdjpy,units_usdcnh,client_tag) VALUES(?,?,?,?,?,?,?)",
                (uuid.uuid4().hex, int(ts.timestamp()), action, levelPrice, unitsUsdJpy, unitsUsdCnh, clientTag),
            )

    def loadExecutions(self, limit: int = 100) -> List[dict]:
        with self._connect() as cx:
            rows = cx.execute(
                "SELECT ts,action,level_price,units_usdjpy,units_usdcnh,client_tag FROM executions ORDER BY ts DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [
                {
                    "ts": int(ts),
                    "action": action,
                    "levelPrice": float(level_price),
                    "unitsUsdJpy": int(u1),
                    "unitsUsdCnh": int(u2),
                    "clientTag": tag,
                }
                for ts, action, level_price, u1, u2, tag in rows
            ]

    def countExecutionsByAction(self, action: str) -> int:
        with self._connect() as cx:
            row = cx.execute("SELECT COUNT(1) FROM executions WHERE action=?", (action,)).fetchone()
            return int(row[0]) if row else 0

    def saveAccountSnapshot(self, ts: datetime, balance: float, nav: float, unrealized: float, currency: str = "USD"):
        with self._connect() as cx:
            cx.execute(
                "INSERT OR REPLACE INTO account_snapshots(ts,balance,nav,unrealized,currency) VALUES(?,?,?,?,?)",
                (int(ts.timestamp()), balance, nav, unrealized, currency),
            )

    def loadLatestAccountSnapshot(self) -> Optional[dict]:
        with self._connect() as cx:
            row = cx.execute(
                "SELECT ts,balance,nav,unrealized,currency FROM account_snapshots ORDER BY ts DESC LIMIT 1"
            ).fetchone()
            if not row:
                return None
            ts, balance, nav, unrealized, currency = row
            return {
                "ts": int(ts),
                "balance": float(balance),
                "nav": float(nav),
                "unrealized": float(unrealized),
                "currency": currency,
            }


def synthCnhJpy(usdcnhPrice: float, usdjpyPrice: float) -> float:
    if usdcnhPrice <= 0:
        raise ValueError("usdcnhPrice must be > 0")
    return usdjpyPrice / usdcnhPrice


def worstDrawdownWithinWindow(
    priceSeries: List[Tuple[datetime, float]], maxDays: int = 730
) -> Tuple[float, float, datetime, datetime]:
    if not priceSeries or len(priceSeries) < 2:
        raise ValueError("need ≥2 points")
    times = [t for t, _ in priceSeries]
    values = [v for _, v in priceSeries]
    dq = deque()
    bestDrop = -1.0
    bestPeakIdx = 0
    bestTroughIdx = 0
    startIdx = 0
    for i, (timeValue, priceValue) in enumerate(priceSeries):
        while startIdx <= i and (timeValue - priceSeries[startIdx][0]).days > maxDays:
            if dq and dq[0] == startIdx:
                dq.popleft()
            startIdx += 1
        while dq and values[dq[-1]] <= priceValue:
            dq.pop()
        dq.append(i)
        peakIdx = dq[0]
        if peakIdx < i:
            dropValue = values[peakIdx] - priceValue
            if dropValue > bestDrop:
                bestDrop = dropValue
                bestPeakIdx = peakIdx
                bestTroughIdx = i
    return values[bestPeakIdx], values[bestTroughIdx], times[bestPeakIdx], times[bestTroughIdx]


def optimizeGridDensity(
    historySeries: List[Tuple[datetime, float]],
    riskConfig: RiskConfig,
    minLevels: Optional[int] = None,
    maxLevels: Optional[int] = None,
    step: Optional[int] = None,
    marginRateApprox: Optional[float] = None,
    roundTripCostUsd: Optional[float] = None,
    marginUtilizationLimit: float = 0.8,
) -> dict:
    if len(historySeries) < 3:
        raise ValueError("not enough history")
    hsh, hsl, _, _ = worstDrawdownWithinWindow(historySeries, riskConfig.lookbackDays)
    hsAbs = max(hsh - hsl, 0.0)
    if hsAbs <= riskConfig.minTick:
        raise ValueError("HS too small")
    minL = int(minLevels or riskConfig.gridLevelsMin)
    maxL = int(maxLevels or riskConfig.gridLevelsMax)
    stp = int(step or riskConfig.gridLevelsStep)
    mRate = float(marginRateApprox if marginRateApprox is not None else riskConfig.marginRateApprox)
    rtCost = float(roundTripCostUsd if roundTripCostUsd is not None else riskConfig.roundTripCostUsd)
    totalDays = max((historySeries[-1][0] - historySeries[0][0]).total_seconds() / 86400.0, 1e-9)
    best = None
    candidates = []
    for nLevels in range(minL, maxL + 1, stp):
        bwUsd = riskConfig.totalCapitalUsd * (riskConfig.maxActiveCapitalFraction / float(nLevels))
        bi = max(hsAbs / float(nLevels), riskConfig.minTick)
        held = [False] * (nLevels + 1)
        tpPx = [0.0] * (nLevels + 1)
        prevPx = None
        maxConc = 0
        areaConc = 0.0
        lastT = historySeries[0][0]
        trades = 0
        profitUsd = 0.0
        for t, px in historySeries:
            for i in range(nLevels + 1):
                if held[i] and px >= tpPx[i]:
                    held[i] = False
                    trades += 1
                    profitUsd += bwUsd * riskConfig.leverageMultiplier * riskConfig.takeProfitPct - rtCost
            if prevPx is not None:
                for i in range(nLevels + 1):
                    lvl = hsl + i * bi
                    if (prevPx - lvl) * (px - lvl) <= 0.0 and not held[i]:
                        held[i] = True
                        tpPx[i] = px * (1.0 + riskConfig.takeProfitPct)
            conc = sum(1 for h in held if h)
            maxConc = max(maxConc, conc)
            dt = (t - lastT).total_seconds() / 86400.0
            if dt > 0:
                areaConc += conc * dt
                lastT = t
            prevPx = px
        evPerDay = profitUsd / totalDays
        avgConc = areaConc / totalDays
        marginPerLevel = bwUsd * riskConfig.leverageMultiplier * mRate
        peakMargin = maxConc * marginPerLevel
        if peakMargin <= riskConfig.totalCapitalUsd * marginUtilizationLimit:
            rec = {
                "numBuyWagers": nLevels,
                "buyIncrement": bi,
                "evPerDayUsd": evPerDay,
                "trades": trades,
                "tradesPerDay": trades / totalDays,
                "avgConcurrentLevels": avgConc,
                "maxConcurrentLevels": maxConc,
                "bwUsd": bwUsd,
                "peakMarginUsd": peakMargin,
                "marginRateApprox": mRate,
                "roundTripCostUsd": rtCost,
            }
            candidates.append(rec)
            if best is None or rec["evPerDayUsd"] > best["evPerDayUsd"]:
                best = rec
    if best is None:
        raise ValueError("no feasible grid under margin constraints")
    return {
        "best": best,
        "candidates": sorted(candidates, key=lambda x: -x["evPerDayUsd"])[:10],
        "hsHigh": hsh,
        "hsLow": hsl,
        "hsAbs": hsAbs,
    }


@dataclass
class TriangularGridStrategy:
    riskConfig: RiskConfig
    executionBroker: Broker
    stateStore: StateStore
    gridLevels: List[GridLevel] = field(default_factory=list)
    historicLossHigh: Optional[float] = None
    historicLossLow: Optional[float] = None
    historicLossHighTime: Optional[datetime] = None
    historicLossLowTime: Optional[datetime] = None
    buyIncrement: Optional[float] = None
    strategyState: StrategyState = field(default_factory=StrategyState)

    def bootstrapGrid(self, syntheticCnhJpyHistory: List[Tuple[datetime, float]]):
        hsh, hsl, timeHigh, timeLow = worstDrawdownWithinWindow(syntheticCnhJpyHistory, self.riskConfig.lookbackDays)
        self.historicLossHigh, self.historicLossLow = hsh, hsl
        self.historicLossHighTime, self.historicLossLowTime = timeHigh, timeLow
        hsAbs = max(hsh - hsl, 0.0)
        self.buyIncrement = max(hsAbs / float(self.riskConfig.numBuyWagers), self.riskConfig.minTick)
        self.gridLevels = []
        for levelIndex in range(0, self.riskConfig.numBuyWagers + 1):
            levelPrice = hsl + levelIndex * self.buyIncrement
            usdNotionalPerLeg = (self.riskConfig.buyWagerUsd * self.riskConfig.leverageMultiplier) / 2
            self.gridLevels.append(GridLevel(levelPrice=levelPrice, usdNotionalPerLeg=usdNotionalPerLeg))
        self.stateStore.saveGridLevels(self.gridLevels)

    def _capAvailableUsdUnlevered(self) -> float:
        cap = self.riskConfig.totalCapitalUsd * self.riskConfig.maxActiveCapitalFraction
        return max(cap - self.strategyState.activeCapitalUsdUnlevered, 0.0)

    def _heldLevelsCount(self) -> int:
        return sum(1 for l in self.gridLevels if l.levelState == LevelState.HELD)

    def _perLevelMarginUsd(self) -> float:
        return self.riskConfig.buyWagerUsd * self.riskConfig.leverageMultiplier * self.riskConfig.marginRateApprox

    def _canOpenLevel(self) -> bool:
        if self.riskConfig.enforceEquityCap:
            if self._capAvailableUsdUnlevered() + 1e-9 < self.riskConfig.buyWagerUsd:
                return False
        used = self._heldLevelsCount() * self._perLevelMarginUsd()
        limit = self.riskConfig.totalCapitalUsd * self.riskConfig.marginUtilizationLimit
        return used + self._perLevelMarginUsd() <= limit + 1e-9

    def _crossed(self, previousPrice: Optional[float], currentPrice: float, levelPrice: float) -> bool:
        if previousPrice is None:
            return False
        return (previousPrice - levelPrice) * (currentPrice - levelPrice) <= 0.0

    def _tryOpenLevel(self, level: GridLevel, syntheticPrice: float, usdcnhPrice: float, usdjpyPrice: float, timestamp: datetime):
        if level.levelState != LevelState.IDLE:
            return
        if not self._canOpenLevel():
            return
        legUnitsUsdJpy = int(round(+level.usdNotionalPerLeg))
        legUnitsUsdCnh = int(round(-level.usdNotionalPerLeg))
        clientTag = f"lvl-{int(level.levelPrice*1e6)}"
        didExecute = self.executionBroker.executeTwoLegMarket(legUnitsUsdJpy, legUnitsUsdCnh, clientTag, timestamp)
        if not didExecute:
            return
        level.levelState = LevelState.HELD
        level.levelEntrySyntheticPrice = syntheticPrice
        level.levelTakeProfitSyntheticPrice = syntheticPrice * (1.0 + self.riskConfig.takeProfitPct)
        self.strategyState.activeCapitalUsdUnlevered += self.riskConfig.buyWagerUsd
        self.strategyState.tradesOpenedCount += 1
        self.stateStore.recordExecution(timestamp, "OPEN", level.levelPrice, legUnitsUsdJpy, legUnitsUsdCnh, clientTag)
        self.stateStore.saveGridLevels([level])
        self.stateStore.saveStrategyState(self.strategyState)

    def _tryCloseLevel(self, level: GridLevel, syntheticPrice: float, usdcnhPrice: float, usdjpyPrice: float, timestamp: datetime):
        if level.levelState != LevelState.HELD or level.levelTakeProfitSyntheticPrice is None:
            return
        if syntheticPrice >= level.levelTakeProfitSyntheticPrice:
            legUnitsUsdJpy = int(round(-level.usdNotionalPerLeg))
            legUnitsUsdCnh = int(round(+level.usdNotionalPerLeg))
            clientTag = f"lvl-exit-{int(level.levelPrice*1e6)}"
            didExecute = self.executionBroker.executeTwoLegMarket(legUnitsUsdJpy, legUnitsUsdCnh, clientTag, timestamp)
            if not didExecute:
                return
            level.levelState = LevelState.IDLE
            level.levelEntrySyntheticPrice = None
            level.levelTakeProfitSyntheticPrice = None
            self.strategyState.activeCapitalUsdUnlevered = max(
                self.strategyState.activeCapitalUsdUnlevered - self.riskConfig.buyWagerUsd, 0.0
            )
            self.strategyState.tradesClosedCount += 1
            self.stateStore.recordExecution(timestamp, "CLOSE", level.levelPrice, legUnitsUsdJpy, legUnitsUsdCnh, clientTag)
            self.stateStore.saveGridLevels([level])
            self.stateStore.saveStrategyState(self.strategyState)

    def onTick(self, usdcnhPrice: float, usdjpyPrice: float, timestamp: datetime):
        syntheticPrice = synthCnhJpy(usdcnhPrice, usdjpyPrice)
        for level in self.gridLevels:
            self._tryCloseLevel(level, syntheticPrice, usdcnhPrice, usdjpyPrice, timestamp)
        for level in self.gridLevels:
            if self._crossed(self.strategyState.previousSyntheticPrice, syntheticPrice, level.levelPrice):
                self._tryOpenLevel(level, syntheticPrice, usdcnhPrice, usdjpyPrice, timestamp)
        self.strategyState.previousSyntheticPrice = syntheticPrice
        self.stateStore.saveStrategyState(self.strategyState)


def _parseBoolEnv(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y"}


def loadEnvVariables(dotenvPath: Optional[str] = None) -> None:
    if load_dotenv is not None:
        load_dotenv(dotenvPath) if dotenvPath else load_dotenv()


def getOandaBrokerFromEnv() -> OandaBroker:
    accountId = os.getenv("OANDA_ACCOUNT_ID")
    apiToken = os.getenv("OANDA_API_TOKEN")
    isPractice = _parseBoolEnv(os.getenv("OANDA_PRACTICE"), True)
    if not accountId or not apiToken:
        raise ValueError("Missing OANDA_ACCOUNT_ID or OANDA_API_TOKEN in environment")
    return OandaBroker(accountId=accountId, apiToken=apiToken, isPractice=isPractice)


def getOandaPriceFeedFromEnv() -> OandaPriceFeed:
    accountId = os.getenv("OANDA_ACCOUNT_ID")
    apiToken = os.getenv("OANDA_API_TOKEN")
    isPractice = _parseBoolEnv(os.getenv("OANDA_PRACTICE"), True)
    if not accountId or not apiToken:
        raise ValueError("Missing OANDA_ACCOUNT_ID or OANDA_API_TOKEN in environment")
    return OandaPriceFeed(accountId=accountId, apiToken=apiToken, isPractice=isPractice)


def fetchOandaDailyMidSeries(
    accountId: str, apiToken: str, isPractice: bool, instrument: str, days: int
) -> List[Tuple[datetime, float]]:
    host = "https://api-fxpractice.oanda.com" if isPractice else "https://api-fxtrade.oanda.com"
    url = f"{host}/v3/instruments/{instrument}/candles"
    params = {"granularity": "D", "price": "M", "count": str(max(2, days + 2))}
    r = requests.get(url, headers={"Authorization": f"Bearer {apiToken}"}, params=params, timeout=10)
    if r.status_code >= 300:
        raise RuntimeError(f"candles error {r.status_code}: {r.text}")
    out: List[Tuple[datetime, float]] = []
    for c in r.json().get("candles", []):
        if not c.get("complete", False):
            continue
        t = datetime.fromisoformat(c["time"].replace("Z", "+00:00"))
        m = c.get("mid", {})
        if "c" in m:
            out.append((t, float(m["c"])))
    out.sort(key=lambda x: x[0])
    start = datetime.now(timezone.utc) - timedelta(days=days)
    return [(t, v) for t, v in out if t >= start]
