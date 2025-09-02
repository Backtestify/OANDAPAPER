from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional, Tuple
from collections import deque
from datetime import datetime, timedelta
import uuid
import requests
import os

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
        payload = {"order": order}
        response = requests.post(url, headers=self._headers(), json=payload, timeout=self.requestTimeoutSec)
        if response.status_code >= 300:
            raise RuntimeError(f"OANDA order error {response.status_code}: {response.text}")
        return response.json()
    def _closePosition(self, instrument: str) -> None:
        url = f"{self._apiHost()}/v3/accounts/{self.accountId}/positions/{instrument}/close"
        response = requests.put(url, headers=self._headers(), json={"longUnits": "ALL", "shortUnits": "ALL"}, timeout=self.requestTimeoutSec)
        if response.status_code >= 300:
            raise RuntimeError(f"OANDA close error {response.status_code}: {response.text}")
    def executeTwoLegMarket(self, unitsUsdJpy: int, unitsUsdCnh: int, clientTag: str, timestamp: datetime) -> bool:
        try:
            jpyFill = self._postOrder("USD_JPY", unitsUsdJpy, orderType="MARKET", clientTag=f"{clientTag}-jpy")
            try:
                cnhFill = self._postOrder("USD_CNH", unitsUsdCnh, orderType="MARKET", clientTag=f"{clientTag}-cnh")
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

@dataclass
class TriangularGridStrategy:
    riskConfig: RiskConfig
    executionBroker: Broker
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

    def onTick(self, usdcnhPrice: float, usdjpyPrice: float, timestamp: datetime):
        syntheticPrice = synthCnhJpy(usdcnhPrice, usdjpyPrice)
        for level in self.gridLevels:
            self._tryCloseLevel(level, syntheticPrice, usdcnhPrice, usdjpyPrice, timestamp)
        for level in self.gridLevels:
            if self._crossed(self.strategyState.previousSyntheticPrice, syntheticPrice, level.levelPrice):
                self._tryOpenLevel(level, syntheticPrice, usdcnhPrice, usdjpyPrice, timestamp)
        self.strategyState.previousSyntheticPrice = syntheticPrice

# Env

def _parseBoolEnv(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y"}

def loadEnvVariables(dotenvPath: Optional[str] = None) -> None:
    if load_dotenv is not None:
        if dotenvPath:
            load_dotenv(dotenvPath)
        else:
            load_dotenv()

def getOandaBrokerFromEnv() -> OandaBroker:
    accountId = os.getenv("OANDA_ACCOUNT_ID")
    apiToken = os.getenv("OANDA_API_TOKEN")
    isPractice = _parseBoolEnv(os.getenv("OANDA_PRACTICE"), True)
    if not accountId or not apiToken:
        raise ValueError("Missing OANDA_ACCOUNT_ID or OANDA_API_TOKEN in environment")
    return OandaBroker(accountId=accountId, apiToken=apiToken, isPractice=isPractice)

# Utilities

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

if __name__ == "__main__":
    import random
    loadEnvVariables()
    usePaper = _parseBoolEnv(os.getenv("USE_PAPER_BROKER"), True)
    startDate = datetime(2022, 1, 1)
    totalDays = 365 * 3
    timeSeries, usdcnhSeries, usdjpySeries = [], [], []
    usdcnhStart, usdjpyStart = 6.5, 130.0
    for _ in range(totalDays):
        startDate += timedelta(days=1)
        usdcnhStart += random.gauss(0, 0.01)
        usdjpyStart += random.gauss(0, 0.5)
        usdcnhStart = max(usdcnhStart, 5.5)
        usdjpyStart = max(usdjpyStart, 90)
        timeSeries.append(startDate)
        usdcnhSeries.append(usdcnhStart)
        usdjpySeries.append(usdjpyStart)
    cnhjpyHistory = list(zip(timeSeries, [synthCnhJpy(a, b) for a, b in zip(usdcnhSeries, usdjpySeries)]))
    riskConfig = RiskConfig(totalCapitalUsd=float(os.getenv("TOTAL_CAPITAL_USD", "100000")), leverageMultiplier=float(os.getenv("LEVERAGE_MULTIPLIER", "2.0")))
    executionBroker: Broker = PaperBroker(PaperBrokerConfig()) if usePaper else getOandaBrokerFromEnv()
    strategy = TriangularGridStrategy(riskConfig=riskConfig, executionBroker=executionBroker)
    strategy.bootstrapGrid(cnhjpyHistory)
    for i in range(totalDays - 180, totalDays):
        strategy.onTick(usdcnhSeries[i], usdjpySeries[i], timeSeries[i])
