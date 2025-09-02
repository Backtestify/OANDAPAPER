from __future__ import annotations
import os
import requests
import gradio as gr
import pandas as pd

API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")

def _get(path: str, params: dict | None = None):
    try:
        r = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=1.5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def _post(path: str, jsonData: dict | None = None):
    try:
        r = requests.post(f"{API_BASE_URL}{path}", json=jsonData, timeout=1.5)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def uiFetchStatus():
    s = _get("/status")
    if isinstance(s, dict) and "error" in s:
        return f"API error: {s['error']}"
    running = "🟢 running" if s["running"] else "🔴 stopped"
    risk = s["risk"]
    strat = s["strategy"]
    return f"{running} | broker={s['broker']} | BW=${risk['buyWagerUsd']:.2f} | leverage={risk['leverageMultiplier']}x | TP={risk['takeProfitPct']*100:.2f}% | held={strat['heldLevels']} | opened={strat['tradesOpenedCount']} | closed={strat['tradesClosedCount']}"

def uiFetchLevels():
    res = _get("/levels")
    if isinstance(res, dict) and "error" in res:
        return []
    return [[d["levelPrice"], d["state"], d["entry"], d["takeProfit"], d["usdNotionalPerLeg"]] for d in res]

def uiFetchExecutions(limit: int):
    res = _get("/executions", {"limit": limit})
    if isinstance(res, dict) and "error" in res:
        return []
    return [[e["ts"], e["action"], e["levelPrice"], e["unitsUsdJpy"], e["unitsUsdCnh"], e["clientTag"]] for e in res]

def uiFetchHistoryDf(days: int):
    res = _get("/history", {"days": days, "points": 1200})
    if isinstance(res, dict) and "error" in res:
        return pd.DataFrame({"epoch": [], "price": []})
    return pd.DataFrame({"epoch": res["timestamps"], "price": res["cnhjpy"]})

def uiRefreshAll(limit: int, days: int):
    status = uiFetchStatus()
    levels = uiFetchLevels()
    execs = uiFetchExecutions(limit)
    histDf = uiFetchHistoryDf(days)
    return status, levels, execs, histDf

def uiStart():
    res = _post("/start")
    if isinstance(res, dict) and "error" in res:
        return f"Start error: {res['error']}"
    return "Start signal sent. Use Refresh to update status."

def uiStop():
    _post("/stop")
    return uiFetchStatus()

def uiCloseAll():
    _post("/close-all")
    return uiFetchStatus()

def uiUpdateRisk(tc: float, lev: float, tpPct: float, rebuild: bool):
    _post("/risk", {"totalCapitalUsd": tc, "leverageMultiplier": lev, "takeProfitPct": tpPct, "rebuildGrid": rebuild})
    return uiFetchStatus()

with gr.Blocks(title="Triangular Grid Trader") as demo:
    gr.Markdown("# Triangular Grid Trader Dashboard")
    statusBox = gr.Textbox(label="Status", interactive=False)
    with gr.Row():
        startBtn = gr.Button("Start")
        stopBtn = gr.Button("Stop")
        closeBtn = gr.Button("Close All Positions")
        refreshBtn = gr.Button("Refresh")
    with gr.Accordion("Risk Config", open=False):
        tc = gr.Number(label="Total Capital USD", value=float(os.getenv("TOTAL_CAPITAL_USD", "100000")))
        lev = gr.Number(label="Leverage Multiplier", value=float(os.getenv("LEVERAGE_MULTIPLIER", "2.0")))
        tp = gr.Number(label="Take Profit Pct (e.g. 0.015)", value=float(os.getenv("TAKE_PROFIT_PCT", "0.015")))
        rebuild = gr.Checkbox(label="Rebuild Grid After Update", value=False)
        applyBtn = gr.Button("Apply Risk")
    with gr.Tab("Levels"):
        levelsDf = gr.Dataframe(headers=["levelPrice","state","entry","takeProfit","usdNotionalPerLeg"], row_count=5)
    with gr.Tab("Executions"):
        limitBox = gr.Number(label="Limit", value=100)
        execDf = gr.Dataframe(headers=["ts","action","levelPrice","unitsUsdJpy","unitsUsdCnh","clientTag"], row_count=5)
    with gr.Tab("History"):
        daysBox = gr.Number(label="Days", value=180)
        plot = gr.LinePlot(x="epoch", y="price", title="CNHJPY synthetic", x_title="epoch", y_title="price")

    startBtn.click(uiStart, outputs=statusBox)
    stopBtn.click(uiStop, outputs=statusBox)
    closeBtn.click(uiCloseAll, outputs=statusBox)
    refreshBtn.click(uiRefreshAll, inputs=[limitBox, daysBox], outputs=[statusBox, levelsDf, execDf, plot])
    applyBtn.click(uiUpdateRisk, inputs=[tc, lev, tp, rebuild], outputs=statusBox)

if __name__ == "__main__":
    demo.queue().launch(
        server_name=os.getenv("GRADIO_HOST", "127.0.0.1"),
        server_port=int(os.getenv("GRADIO_PORT", "7860")),
    )
