# === gradioUI.py ===
from __future__ import annotations
import os, json, requests, gradio as gr, pandas as pd
import matplotlib.pyplot as plt

API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")

def _get(path: str, params: dict | None = None):
    try:
        r = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=2.0)
        try:
            data = r.json()
        except Exception:
            data = {"error": f"HTTP {r.status_code}", "body": r.text[:400]}
        if r.status_code >= 400 and "error" not in data:
            data["error"] = f"HTTP {r.status_code}"
        return data
    except Exception as e:
        return {"error": str(e)}

def _post(path: str, jsonData: dict | None = None):
    try:
        r = requests.post(f"{API_BASE_URL}{path}", json=jsonData, timeout=20.0)
        try:
            data = r.json()
        except Exception:
            data = {"error": f"HTTP {r.status_code}", "body": r.text[:400]}
        if r.status_code >= 400 and "error" not in data:
            data["error"] = f"HTTP {r.status_code}"
        return data
    except Exception as e:
        return {"error": str(e)}

def _fmtUsd(x: float) -> str:
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return "$0.00"

def uiMakeStatusText(st: dict) -> str:
    running = "🟢 running" if st.get("running") else "🔴 stopped"
    risk = st.get("risk", {}) or {}
    strat = st.get("strategy", {}) or {}

    # Pull server-provided margin fields if available, else approximate
    held = strat.get("heldLevels", 0)
    mr = risk.get("marginRateApprox", 0.035)
    perLevelMargin = risk.get("perLevelMarginUsd", risk.get("buyWagerUsd", 0.0) * risk.get("leverageMultiplier", 1.0) * mr)
    estUsed = strat.get("estMarginUsedUsd", held * perLevelMargin)
    estLimit = strat.get("estMarginLimitUsd", risk.get("totalCapitalUsd", 0.0) * risk.get("marginUtilizationLimit", 0.8))
    usedPct = strat.get("estMarginUsedPct", (estUsed / estLimit) if estLimit else 0.0)

    return (
        f"{running} | broker={st.get('broker')} | "
        f"BW={_fmtUsd(risk.get('buyWagerUsd', 0))} | "
        f"leverage={risk.get('leverageMultiplier', 1)}x | "
        f"TP={risk.get('takeProfitPct', 0)*100:.2f}% | "
        f"levels={risk.get('numBuyWagers', 0)} | "
        f"held={held} | "
        f"opened={strat.get('tradesOpenedCount', 0)} | "
        f"closedBot={strat.get('tradesClosedCount', 0)} | "
        f"closedExt={strat.get('tradesClosedExternalCount', 0)} | "
        f"margin used={_fmtUsd(estUsed)}/{_fmtUsd(estLimit)} ({usedPct*100:.1f}%)"
    )

def uiMakeSyncHtml(st: dict) -> str:
    sync = st.get("sync", {}) or {}
    status = str(sync.get("status", "OK")).upper()
    detail = sync.get("detail", "")
    color = "#16a34a" if status == "OK" else ("#ca8a04" if status == "RECOVERING" else "#dc2626")
    return f'<div style="padding:6px 10px;border-radius:10px;background:{color};color:#fff;display:inline-block;">sync: {status}</div>' + \
           (f"<div style='margin-top:6px;font-size:12px'>{detail}</div>" if detail else "")

def uiMakeAccountText(st: dict) -> str:
    acc = st.get("account") or {}
    if not acc:
        return "Account snapshot: (none)"
    bal = acc.get("balance", 0.0)
    nav = acc.get("nav", acc.get("NAV", bal))
    unrl = acc.get("unrealized", acc.get("unrealizedPL", 0.0))
    cur = acc.get("currency", "USD")
    ts = acc.get("ts")
    tsStr = f" @ {ts}" if ts else ""
    return f"Balance: {bal:.2f} {cur} | NAV: {nav:.2f} {cur} | Unrealized P/L: {unrl:.2f} {cur}{tsStr}"

def uiFetchStatusBundle():
    st = _get("/status")
    if isinstance(st, dict) and "error" in st:
        return f"API error: {st['error']}", "<div>sync: N/A</div>", "Account snapshot: (error)", []
    statusText = uiMakeStatusText(st)
    syncHtml = uiMakeSyncHtml(st)
    acctText = uiMakeAccountText(st)
    return statusText, syncHtml, acctText, st

def uiFetchLevels():
    res = _get("/levels")
    if isinstance(res, dict) and "error" in res:
        return []
    rows = []
    for d in res:
        rows.append([
            d["levelPrice"],
            d["state"],
            d["entry"] if d["entry"] is not None else "",
            d["takeProfit"] if d["takeProfit"] is not None else "",
            d["usdNotionalPerLeg"],
        ])
    return rows

def uiFetchExecutions(limit: int):
    res = _get("/executions", {"limit": int(limit)})
    if isinstance(res, dict) and "error" in res:
        return []
    return [[e["ts"], e["action"], e["levelPrice"], e["unitsUsdJpy"], e["unitsUsdCnh"], e["clientTag"]] for e in res]

def uiFetchHistoryDf(days: int) -> pd.DataFrame:
    res = _get("/history", {"days": int(days), "points": 1200})
    if isinstance(res, dict) and "error" in res:
        return pd.DataFrame({"epoch": [], "price": []})
    return pd.DataFrame({"epoch": res["timestamps"], "price": res["cnhjpy"]})

def uiMakeHistoryPlot(histDf: pd.DataFrame):
    fig, ax = plt.subplots()
    ax.set_title("Synthetic CNHJPY")
    ax.set_xlabel("epoch (s)")
    ax.set_ylabel("price")
    if not histDf.empty:
        ax.plot(histDf["epoch"], histDf["price"])
    return fig

def uiRefreshAll(limit: int, days: int):
    statusText, syncHtml, acctText, _ = uiFetchStatusBundle()
    levels = uiFetchLevels()
    execs = uiFetchExecutions(limit)
    histDf = uiFetchHistoryDf(days)
    plot = uiMakeHistoryPlot(histDf)
    return statusText, syncHtml, acctText, levels, execs, plot

def uiStart():
    res = _post("/start")
    if isinstance(res, dict) and "error" in res:
        return f"Start error: {res['error']}"
    return "Start signal sent. Use Refresh to update status."

def uiStop():
    _post("/stop")
    statusText, _, _, _ = uiFetchStatusBundle()
    return statusText

def uiCloseAll():
    res = _post("/close-all")
    if isinstance(res, dict) and res.get("ok"):
        return "Close-all sent. Account should now be flat. Use Refresh to update status."
    if isinstance(res, dict) and "detail" in res:
        return f"Close-all: {res['detail']}"
    if isinstance(res, dict) and "error" in res:
        return f"Close-all error: {res['error']}"
    return "Close-all: unknown response"

def uiAudit():
    res = _post("/audit")
    if isinstance(res, dict) and "error" in res:
        return f"Audit error: {res['error']}"
    return f"audit: {res}"

def uiUpdateRisk(totalCapitalUsd: float, leverageMultiplier: float, takeProfitPct: float, rebuildGrid: bool):
    _post("/risk", {
        "totalCapitalUsd": totalCapitalUsd,
        "leverageMultiplier": leverageMultiplier,
        "takeProfitPct": takeProfitPct,
        "rebuildGrid": rebuildGrid
    })
    statusText, _, _, _ = uiFetchStatusBundle()
    return statusText

def uiOptimize(minLevels, maxLevels, stepLevels, lookbackDays, marginRateApprox, roundTripCostUsd, marginUtilizationLimit, applyNow):
    payload = {
        "minLevels": int(minLevels),
        "maxLevels": int(maxLevels),
        "step": int(stepLevels),
        "lookbackDays": int(lookbackDays),
        "marginRateApprox": float(marginRateApprox),
        "roundTripCostUsd": float(roundTripCostUsd),
        "marginUtilizationLimit": float(marginUtilizationLimit),
        "apply": bool(applyNow),
    }
    res = _post("/optimize-grid", payload)
    if isinstance(res, dict) and "error" in res:
        return f"Optimize error: {res['error']}", []
    if isinstance(res, dict) and "detail" in res:
        return f"Optimize error: {res['detail']}", []
    best = res.get("best", {})
    if not best:
        return "No result", []
    txt = (
        f"Best numBuyWagers={best.get('numBuyWagers')} | "
        f"BI={best.get('buyIncrement'):.6f} | "
        f"EV/day=${best.get('evPerDayUsd'):.2f} | "
        f"trades/day={best.get('tradesPerDay'):.2f} | "
        f"avgConc={best.get('avgConcurrentLevels'):.1f} | "
        f"maxConc={best.get('maxConcurrentLevels')} | "
        f"BW=${best.get('bwUsd'):.2f} | peakMargin=${best.get('peakMarginUsd'):.2f}"
    )
    rows = res.get("candidates", [])
    out = []
    for r in rows:
        out.append([
            r["numBuyWagers"], r["buyIncrement"], r["evPerDayUsd"], r["trades"],
            r["tradesPerDay"], r["avgConcurrentLevels"], r["maxConcurrentLevels"],
            r["bwUsd"], r["peakMarginUsd"]
        ])
    return txt, out

with gr.Blocks(title="Triangular Grid Trader") as demo:
    gr.Markdown("# Triangular Grid Trader Dashboard")

    with gr.Row():
        statusBox = gr.Textbox(label="Status", interactive=False)
        syncHtml = gr.HTML("<div>sync: N/A</div>")
    accountBox = gr.Textbox(label="Account", interactive=False)

    with gr.Row():
        startBtn = gr.Button("Start")
        stopBtn = gr.Button("Stop")
        closeBtn = gr.Button("Close All Positions")
        auditBtn = gr.Button("Audit Consistency")
        refreshBtn = gr.Button("Refresh", elem_id="tgRefreshBtn")

    autoPushToggle = gr.Checkbox(label="Auto-refresh on trade (push)", value=True)
    eventBridge = gr.HTML(f"""
<script>
(function() {{
  const apiBase = {json.dumps(API_BASE_URL)};
  let enabled = true;
  window.tgSetAutoPush = (val) => {{ enabled = !!val; }};
  try {{
    const es = new EventSource(apiBase + "/events");
    es.onmessage = function(e) {{
      if (!enabled) return;
      const btn = document.getElementById("tgRefreshBtn");
      if (btn) btn.click();
    }};
  }} catch (err) {{ console.warn("SSE connect failed:", err); }}
}})();
</script>
""")
    autoPushToggle.change(fn=None, inputs=autoPushToggle, outputs=None,
                          js="(val)=>{ if (window.tgSetAutoPush) window.tgSetAutoPush(val); }")

    with gr.Accordion("Risk Config", open=False):
        tc = gr.Number(label="Total Capital USD", value=float(os.getenv("TOTAL_CAPITAL_USD", "100000")))
        # Default leverage to 1.0x to avoid accidental leverage use
        lev = gr.Number(label="Leverage Multiplier", value=float(os.getenv("LEVERAGE_MULTIPLIER", "1.0")))
        tp = gr.Number(label="Take Profit Pct (e.g. 0.015)", value=float(os.getenv("TAKE_PROFIT_PCT", "0.015")))
        rebuild = gr.Checkbox(label="Rebuild Grid After Update", value=False)
        applyRiskBtn = gr.Button("Apply Risk")

    with gr.Accordion("Optimize Grid Density", open=False):
        minLevels = gr.Number(label="Min Levels", value=60)
        maxLevels = gr.Number(label="Max Levels", value=360)
        stepLevels = gr.Number(label="Step", value=30)
        lookbackDays = gr.Number(label="Lookback Days", value=365)
        marginRateApprox = gr.Number(label="Margin Rate Approx (blended)", value=0.035)
        roundTripCostUsd = gr.Number(label="Round-Trip Cost (USD)", value=1.0)
        marginUtilizationLimit = gr.Number(label="Max Margin Utilization (0-1)", value=0.8)
        with gr.Row():
            optimizeDryBtn = gr.Button("Optimize (Dry Run)")
            optimizeApplyBtn = gr.Button("Optimize & Apply")
        optSummary = gr.Textbox(label="Optimization Result", interactive=False)
        optTable = gr.Dataframe(
            headers=[
                "numBuyWagers","buyIncrement","evPerDayUsd","trades","tradesPerDay",
                "avgConcurrentLevels","maxConcurrentLevels","bwUsd","peakMarginUsd"
            ],
            row_count=5
        )

    with gr.Row():
        limitBox = gr.Number(label="Executions Limit", value=200)
        daysBox = gr.Number(label="History Days (plot)", value=180)

    with gr.Row():
        levelsDf = gr.Dataframe(headers=["levelPrice","state","entry","takeProfit","usdNotionalPerLeg"], label="Grid Levels", interactive=False)
        execDf = gr.Dataframe(headers=["ts","action","levelPrice","unitsUsdJpy","unitsUsdCnh","clientTag"], label="Executions", interactive=False)
    plot = gr.Plot(label="Synthetic CNHJPY")

    # Wiring
    startBtn.click(uiStart, inputs=None, outputs=statusBox)
    stopBtn.click(uiStop, inputs=None, outputs=statusBox)
    closeBtn.click(uiCloseAll, inputs=None, outputs=statusBox)
    auditBtn.click(uiAudit, inputs=None, outputs=statusBox)
    refreshBtn.click(uiRefreshAll, inputs=[limitBox, daysBox], outputs=[statusBox, syncHtml, accountBox, levelsDf, execDf, plot])
    applyRiskBtn.click(uiUpdateRisk, inputs=[tc, lev, tp, rebuild], outputs=statusBox)
    optimizeDryBtn.click(
        uiOptimize,
        inputs=[minLevels, maxLevels, stepLevels, lookbackDays, marginRateApprox, roundTripCostUsd, marginUtilizationLimit, gr.State(False)],
        outputs=[optSummary, optTable]
    )
    optimizeApplyBtn.click(
        uiOptimize,
        inputs=[minLevels, maxLevels, stepLevels, lookbackDays, marginRateApprox, roundTripCostUsd, marginUtilizationLimit, gr.State(True)],
        outputs=[optSummary, optTable]
    )

if __name__ == "__main__":
    demo.launch()
