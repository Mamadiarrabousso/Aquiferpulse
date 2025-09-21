# scripts/make_brief.py
import pandas as pd
from pathlib import Path

table = pd.read_csv("data/processed/asi_table.csv")
table = table.dropna(subset=["asi"])
latest = table["date"].max()
top = (table[table["date"]==latest]
       .sort_values("asi")
       .head(10)[["basin_id","date","asi","twsa_z","sm_z","rain_def_z"]])

out = Path("data/processed/brief_top10_" + latest.replace("-","") + ".csv")
top.to_csv(out, index=False)
print("Wrote", out)
