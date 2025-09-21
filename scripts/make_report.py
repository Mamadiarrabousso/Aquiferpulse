# scripts/make_report.py
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from fpdf import FPDF

ROOT  = Path(__file__).resolve().parents[1]
TABLE = ROOT / "data" / "processed" / "asi_table.csv"
OUT   = ROOT / "data" / "processed" / "weekly_brief.pdf"

WATCH, ALERT = -0.5, -1.0

def classify(asi):
    if pd.isna(asi): return "no-data"
    if asi <= ALERT: return "alert"
    if asi <= WATCH: return "watch"
    return "normal"

def main():
    if not TABLE.exists():
        raise SystemExit(f"Missing table: {TABLE}")

    df = pd.read_csv(TABLE)
    df["date"] = pd.to_datetime(df["date"]).dt.to_period("M").astype(str)
    for c in ("twsa_z","sm_z","rain_z","rain_def_z","asi"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    nonnull = df.dropna(subset=["asi"])
    if nonnull.empty:
        raise SystemExit("No ASI values in table yet.")
    latest = nonnull["date"].max()
    mon = nonnull[nonnull["date"] == latest].copy()
    if "class" not in mon.columns:
        mon["class"] = mon["asi"].apply(classify)

    counts = mon["class"].value_counts().to_dict()
    for k in ("alert","watch","normal","no-data"):
        counts.setdefault(k, 0)

    top = mon.sort_values("asi").head(10)[["basin_id","asi","class"]]

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # Use core font and ASCII-only text
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, "AquiferPulse - Senegal weekly brief", ln=1)

    pdf.set_font("Helvetica", "", 11)
    gen = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    pdf.cell(0, 8, f"Month: {latest}    Generated: {gen}", ln=1)
    pdf.ln(2)

    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(0, 8, "Summary", ln=1)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 7,
             f"Alerts: {counts['alert']}   Watch: {counts['watch']}   "
             f"Normal: {counts['normal']}   No-data: {counts['no-data']}",
             ln=1)
    pdf.cell(0, 7, "Classes: alert <= -1.0  |  watch <= -0.5  |  otherwise normal", ln=1)
    pdf.ln(2)

    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(0, 8, "Top 10 basins to watch", ln=1)
    pdf.set_font("Helvetica", "", 11)
    for i, (bid, asi, cls) in enumerate(
        top[["basin_id","asi","class"]].itertuples(index=False, name=None), 1
    ):
        pdf.cell(0, 7, f"{i}. {int(bid)}  -  ASI {asi:.3f}  ({cls})", ln=1)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pdf.output(str(OUT))
    print("Wrote", OUT)

if __name__ == "__main__":
    main()
