import json, pathlib

p = pathlib.Path("data/static/basins.geojson")
gj = json.loads(p.read_text(encoding="utf-8"))

for f in gj.get("features", []):
    pr = f.setdefault("properties", {})
    pr["name"] = pr.get("name") or pr.get("SUB_NAME") or pr.get("HYBAS_ID") or str(pr.get("basin_id"))

p.write_text(json.dumps(gj, ensure_ascii=False), encoding="utf-8")
print("Updated names in", p)
