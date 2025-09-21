 AquiferPulse — Senegal groundwater early warning

AquiferPulse is a small monthly map for Senegal. It highlights basins where groundwater conditions look unusual based on an Aquifer Storage Index (ASI). Each basin is classified as:
- alert (ASI ≤ -1.0)
- watch (ASI ≤ -0.5)
- normal (otherwise)

Dashboard: https://mamadiarrabousso.github.io/Aquiferpulse/  
API: https://aquiferpulse.onrender.com

What the dashboard shows
- A Leaflet map of Senegal basins colored by class
- A month picker to move through time
- A “Top 10 to watch” list (most negative ASI)
- Basin popups with ASI and component anomalies
- A small status line with counts per class

 How it works
1. Data table  
   `data/processed/asi_table.csv` has one row per basin per month:
