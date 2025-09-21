# scripts/compute_asi.py  — robust inputs + renormalized ASI + latest-with-coverage
from pathlib import Path
import pandas as pd, numpy as np, json

ROOT = Path(__file__).resolve().parents[1]
INTERIM   = ROOT / "data" / "interim"
PROCESSED = ROOT / "data" / "processed"; PROCESSED.mkdir(parents=True, exist_ok=True)
STATIC    = ROOT / "data" / "static"

BASINS_GJ = STATIC / "basins.geojson"
TABLE_OUT = PROCESSED / "asi_table.csv"
LATEST_GJ = PROCESSED / "asi_latest.geojson"

# weights (used, but re-normalized if a component is missing)
W_TWSA, W_SM, W_RAIN = 0.4, 0.4, 0.2
ALERT_T, WATCH_T = -1.0, -0.5

def _read_csv(path, needed):
    if not path.exists():
        print(f"[WARN] missing: {path}")
        return pd.DataFrame(columns=needed)
    df = pd.read_csv(path)
    return df

def _to_month(s):
    d = pd.to_datetime(s, errors="coerce")
    return d.dt.to_period("M").dt.to_timestamp()

def _z_by_basin(df, col):
    def z(s):
        sd = s.std(ddof=0)
        return (s - s.mean())/sd if (sd is not None and sd > 0) else pd.Series([np.nan]*len(s), index=s.index)
    return df.groupby("basin_id")[col].transform(z)

def _classify(asi):
    if pd.isna(asi): return "no-data"
    if asi <= ALERT_T: return "alert"
    if asi <= WATCH_T: return "watch"
    return "normal"

def main():
    # ---- Load inputs (grace, era5, imerg) ----
    grace = _read_csv(INTERIM / "grace.csv", ["basin_id","date","twsa"])
    era5  = _read_csv(INTERIM / "era5.csv",   ["basin_id","date","sm"])
    imerg = _read_csv(INTERIM / "imerg.csv",  ["basin_id","date"])  # rain or rain_def inside

    # keep only necessary cols and harmonize
    frames = []

    if not grace.empty and {"basin_id","date","twsa"} <= set(grace.columns):
        g = grace.loc[:, ["basin_id","date","twsa"]].copy()
        g["basin_id"] = g["basin_id"].astype(str); g["date"] = _to_month(g["date"])
        frames.append(g)

    if not era5.empty and {"basin_id","date","sm"} <= set(era5.columns):
        e = era5.loc[:, ["basin_id","date","sm"]].copy()
        e["basin_id"] = e["basin_id"].astype(str); e["date"] = _to_month(e["date"])
        frames.append(e)

    rain_col = None
    if not imerg.empty:
        if "rain" in imerg.columns:
            rain_col = "rain"
        elif "rain_def" in imerg.columns:
            rain_col = "rain_def"
        if rain_col:
            r = imerg.loc[:, ["basin_id","date", rain_col]].copy()
            r["basin_id"] = r["basin_id"].astype(str); r["date"] = _to_month(r["date"])
            frames.append(r)

    if not frames:
        raise SystemExit("No usable CSVs found in data/interim")

    # ---- Merge wide table ----
    df = frames[0]
    for d in frames[1:]:
        df = pd.merge(df, d, on=["basin_id","date"], how="outer")

    # ---- Z-scores by basin ----
    for c in ["twsa","sm"]:
        if c in df.columns:
            df[f"{c}_z"] = _z_by_basin(df, c)
        else:
            df[f"{c}_z"] = np.nan

    if rain_col == "rain":
        df["rain_z"] = _z_by_basin(df, "rain")
        df["rain_def_z"] = -df["rain_z"]
    elif rain_col == "rain_def":
        df["rain_def_z"] = _z_by_basin(df, "rain_def")
        df["rain_z"] = -df["rain_def_z"]
    else:
        df["rain_z"] = np.nan
        df["rain_def_z"] = np.nan

    # ---- ASI with weight re-normalization (ignore missing components) ----
    comp = pd.DataFrame({
        "twsa_z": df["twsa_z"],
        "sm_z":   df["sm_z"],
        "rain_z": df["rain_z"]
    })
    w = np.array([W_TWSA, W_SM, W_RAIN])
    mask = comp.notna().to_numpy()
    num  = (comp.fillna(0.0).to_numpy() * w).sum(axis=1)
    den  = (mask * w).sum(axis=1)
    asi  = np.divide(num, den, out=np.full_like(num, np.nan, dtype=float), where=den>0)
    df["asi"] = asi
    df["class"] = df["asi"].apply(_classify)

    # ---- Output table ----
    df = df.sort_values(["basin_id","date"]).reset_index(drop=True)
    out_cols = ["basin_id","date","twsa","sm",
                "rain","rain_def","twsa_z","sm_z","rain_z","rain_def_z","asi","class"]
    for c in out_cols:
        if c not in df.columns: df[c] = np.nan
    df_out = df[out_cols].copy()
    df_out["date"] = df_out["date"].dt.strftime("%Y-%m-%d")
    df_out.to_csv(TABLE_OUT, index=False)
    print(f"[OK] wrote {TABLE_OUT}")

    # ---- Choose latest month WITH coverage ----
    df_out["date_dt"] = pd.to_datetime(df_out["date"], errors="coerce")
    cov = (df_out.assign(asi_ok=df_out["asi"].notna())
                 .groupby("date_dt")["asi_ok"].sum().sort_index())
    latest_dt = cov[cov > 0].index.max() if (cov > 0).any() else df_out["date_dt"].max()
    latest = df_out[df_out["date_dt"] == latest_dt].copy()
    latest_str = latest_dt.strftime("%Y-%m-%d") if pd.notna(latest_dt) else None

    # ---- Build GeoJSON ----
    gj = json.loads(BASINS_GJ.read_text(encoding="utf-8"))
    by_id = {str(r["basin_id"]): r for r in latest.to_dict(orient="records")}
    for f in gj.get("features", []):
        p = f.setdefault("properties", {})
        bid = str(p.get("basin_id"))
        row = by_id.get(bid)
        if row:
            p.update({
                "date": row["date"],
                "twsa_z": _r(row["twsa_z"]), "sm_z": _r(row["sm_z"]),
                "rain_z": _r(row["rain_z"]), "rain_def_z": _r(row["rain_def_z"]),
                "asi": _r(row["asi"]), "class": row["class"],
                "name": p.get("name") or str(bid)
            })
        else:
            p.update({"date": latest_str, "twsa_z": None, "sm_z": None,
                      "rain_z": None, "rain_def_z": None, "asi": None, "class": "no-data"})
    LATEST_GJ.write_text(json.dumps(gj, ensure_ascii=False), encoding="utf-8")
    print(f"[OK] wrote {LATEST_GJ}")

def _r(x, nd=3):
    try: return None if pd.isna(x) else round(float(x), nd)
    except: return None

if __name__ == "__main__":
    main()
