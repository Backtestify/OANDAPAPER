# === FILE: core_trader.py ===
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional, Tuple
from collections import deque
from datetime import datetime, timedelta, timezone
import uuid
import requests
import os
import logging
import sqlite3

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
    maxActiveCapitalFraction: float = 1/3
    numBuyWagers: int = 100
    takeProfitPct: float = 0.015
    lookbackDays: int = 730
    minTick: float = 1e-5
    leverageMultiplier: float = 1.0
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
    def _postOrder(self, instrument: str, units: int, orderType: str = "MARKET", price: Optional[float] = None, tif: str = "FOK", clientTag: str = "tg") -> dict:
        url = f"{self._apiHost()}/v3/accounts/{self.accountId}/orders"
        order: dict = {"instrument": instrument, "units": str(units), "type": orderType, "timeInForce": tif, "clientExtensions": self._newClientExtensions(clientTag)}
        if orderType in ("LIMIT", "MARKET_IF_TOUCHED", "STOP") and price is not None:
            order["price"] = f"{price:.6f}"
        r = requests.post(url, headers=self._headers(), json={"order": order}, timeout=self.requestTimeoutSec)
        if r.status_code >= 300:
            raise RuntimeError(f"OANDA order error {r.status_code}: {r.text}")
        return r.json()
    def _closePosition(self, instrument: str) -> None:
        url = f"{self._apiHost()}/v3/accounts/{self.accountId}/positions/{instrument}/close"
        r = requests.put(url, headers=self._headers(), json={"longUnits": "ALL", "shortUnits": "ALL"}, timeout=self.requestTimeoutSec)
        if r.status_code >= 300:
            raise RuntimeError(f"OANDA close error {r.status_code}: {r.text}")
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
        self._closePosition("USD_JPY")
        self._closePosition("USD_CNH")

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
            cx.execute("CREATE TABLE IF NOT EXISTS grid_levels (level_price REAL PRIMARY KEY, state TEXT, entry_px REAL, tp_px REAL, usd_notional_per_leg REAL)")
            cx.execute("CREATE TABLE IF NOT EXISTS strategy_state (id INTEGER PRIMARY KEY CHECK (id=1), prev_px REAL, active_cap_unlev REAL, opened INT, closed INT)")
            cx.execute("CREATE TABLE IF NOT EXISTS executions (id TEXT PRIMARY KEY, ts INTEGER, action TEXT, level_price REAL, units_usdjpy INT, units_usdcnh INT, client_tag TEXT)")
    def saveMeta(self, key: str, value: str):
        with self._connect() as cx:
            cx.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    def loadMeta(self, key: str) -> Optional[str]:
        with self._connect() as cx:
            row = cx.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row[0] if row else None
    def saveHistoryPoint(self, ts: datetime, usdcnh: float, usdjpy: float, cnhjpy: float):
        with self._connect() as cx:
            cx.execute("INSERT OR REPLACE INTO history(ts,usdcnh,usdjpy,cnhjpy) VALUES(?,?,?,?)", (int(ts.timestamp()), usdcnh, usdjpy, cnhjpy))
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
                    (lvl.levelPrice, lvl.levelState.name, lvl.levelEntrySyntheticPrice if lvl.levelEntrySyntheticPrice is not None else None, lvl.levelTakeProfitSyntheticPrice if lvl.levelTakeProfitSyntheticPrice is not None else None, lvl.usdNotionalPerLeg),
                )
    def loadGridLevels(self) -> List[GridLevel]:
        with self._connect() as cx:
            rows = cx.execute("SELECT level_price,state,entry_px,tp_px,usd_notional_per_leg FROM grid_levels ORDER BY level_price ASC").fetchall()
            out: List[GridLevel] = []
            for level_price, state, entry_px, tp_px, notional in rows:
                out.append(GridLevel(levelPrice=float(level_price), levelState=LevelState[state], levelEntrySyntheticPrice=None if entry_px is None else float(entry_px), levelTakeProfitSyntheticPrice=None if tp_px is None else float(tp_px), usdNotionalPerLeg=float(notional)))
            return out
    def saveStrategyState(self, st: StrategyState):
        with self._connect() as cx:
            cx.execute("INSERT OR REPLACE INTO strategy_state(id,prev_px,active_cap_unlev,opened,closed) VALUES(1,?,?,?,?)", (st.previousSyntheticPrice if st.previousSyntheticPrice is not None else None, st.activeCapitalUsdUnlevered, st.tradesOpenedCount, st.tradesClosedCount))
    def loadStrategyState(self) -> StrategyState:
        with self._connect() as cx:
            row = cx.execute("SELECT prev_px,active_cap_unlev,opened,closed FROM strategy_state WHERE id=1").fetchone()
            if not row:
                return StrategyState()
            prev_px, active, opened, closed = row
            return StrategyState(previousSyntheticPrice=None if prev_px is None else float(prev_px), activeCapitalUsdUnlevered=float(active), tradesOpenedCount=int(opened), tradesClosedCount=int(closed))
    def recordExecution(self, ts: datetime, action: str, levelPrice: float, unitsUsdJpy: int, unitsUsdCnh: int, clientTag: str):
        with self._connect() as cx:
            cx.execute("INSERT OR REPLACE INTO executions(id,ts,action,level_price,units_usdjpy,units_usdcnh,client_tag) VALUES(?,?,?,?,?,?,?)", (uuid.uuid4().hex, int(ts.timestamp()), action, levelPrice, unitsUsdJpy, unitsUsdCnh, clientTag))
    def loadExecutions(self, limit: int = 100) -> List[dict]:
        with self._connect() as cx:
            rows = cx.execute("SELECT ts,action,level_price,units_usdjpy,units_usdcnh,client_tag FROM executions ORDER BY ts DESC LIMIT ?", (limit,)).fetchall()
            out = []
            for ts, action, level_price, u1, u2, tag in rows:
                out.append({"ts": int(ts), "action": action, "levelPrice": float(level_price), "unitsUsdJpy": int(u1), "unitsUsdCnh": int(u2), "clientTag": tag})
            return out

# HS / BI

def synthCnhJpy(usdcnhPrice: float, usdjpyPrice: float) -> float:
    if usdcnhPrice <= 0:
        raise ValueError("usdcnhPrice must be > 0")
    return usdjpyPrice / usdcnhPrice

def worstDrawdownWithinWindow(priceSeries: List[Tuple[datetime, float]], maxDays: int = 730) -> Tuple[float, float, datetime, datetime]:
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

# Strategy
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
        historicLossAbs = max(hsh - hsl, 0.0)
        self.buyIncrement = max(historicLossAbs / 100.0, self.riskConfig.minTick)
        self.gridLevels = []
        for levelIndex in range(0, 101):
            levelPrice = hsl + levelIndex * self.buyIncrement
            usdNotionalPerLeg = (self.riskConfig.buyWagerUsd * self.riskConfig.leverageMultiplier) / 2
            self.gridLevels.append(GridLevel(levelPrice=levelPrice, usdNotionalPerLeg=usdNotionalPerLeg))
        self.stateStore.saveGridLevels(self.gridLevels)
    def _capAvailableUsdUnlevered(self) -> float:
        return max(self.riskConfig.totalCapitalUsd * self.riskConfig.maxActiveCapitalFraction - self.strategyState.activeCapitalUsdUnlevered, 0.0)
    def _crossed(self, previousPrice: Optional[float], currentPrice: float, levelPrice: float) -> bool:
        if previousPrice is None:
            return False
        return (previousPrice - levelPrice) * (currentPrice - levelPrice) <= 0.0
    def _tryOpenLevel(self, level: GridLevel, syntheticPrice: float, usdcnhPrice: float, usdjpyPrice: float, timestamp: datetime):
        if level.levelState != LevelState.IDLE:
            return
        if self._capAvailableUsdUnlevered() + 1e-9 < self.riskConfig.buyWagerUsd:
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
            self.strategyState.activeCapitalUsdUnlevered = max(self.strategyState.activeCapitalUsdUnlevered - self.riskConfig.buyWagerUsd, 0.0)
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

# env helpers

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
