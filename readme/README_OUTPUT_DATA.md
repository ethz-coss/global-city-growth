# Boundaries & population of cities: USA (1850–2020) and World (1975–2025)   

This repository provides:

1) **City boundaries** and **population** for the United States (1850-2020) and the world (1975-2025). Data are organized around the idea of **cluster_growth**: a stable city boundary computed for a time window $(y_1, y_2)$, within which growth is measured consistently. Among the numerous city definition, this definition has two main advantages: (i) it dynamically adjusts as urban areas expand; and (ii) it is consistent across space and time, facilitating comparative analyses.

2) Data and projections of **the share of world population living in cities of 1 million people or more** between 1975 and 2100. 

See the [paper](https://arxiv.org/abs/2510.12417).

## Contents
```
.
├── usa/
│   ├── usa_census_place_stable_geom.gpkg
│   ├── usa_census_place_stable_population.csv
│   ├── usa_cluster_growth_geom.gpkg
│   └── usa_cluster_growth_population.csv
└── world/
    ├── world_cluster_growth_geom.gpkg
    ├── world_cluster_growth_population_country.csv
    └── world_tot_pop_share_cities_above_1m_with_projections.csv
  ```



## What is a "cluster growth"?

![Cluster growth computation](figure_cca.png)

Cluster growth are stable city boundaries computed with the City Clustering Algorithm (CCA). The CCA computes stable city boundaries for a time interval $(y_1, y_2)$. This algorithm proceeds in five steps: (A) It takes two grids as input, one for each year, containing estimates of population or built-up area. (B) It classifies grid cells as either urban or non-urban using a threshold on these population/built-up area estimates. (C) It groups contiguous urban grid cells to form clusters. (D) It matches clusters across years when they overlap spatially, forming a bipartite graph. (E) It defines a city's boundary as the spatial union of all clusters within a single connected component of the graph. See the paper for full details.

An important choice in the algorithm above are the population and built-up area thresholds to classify a cell as urban or non-urban. We provide the following thresholds in the datasets below:
- **USA:** This threshold is the number of people in one cell of the grid. Whenever the number of people of in a grid cell is greater than the threshold we classify the cell as urban. Thresholds: **50**, **100**, **200**.  
- **World:** This threshold is the GHSL SMOD degree‑of‑urbanization (see [here](https://human-settlement.emergency.copernicus.eu/ghs_smod2023.php)). Whenever the degree-of-urbanization of a grid cell is greater than the threshold we classify the cell as urban. Thresholds: **21 (Suburban+), 22 (Semi‑dense+), 23 (Dense urban+).** Note the thresholds are **exclusive**: e.g., 21 ⇒ cells with value **> 21** are classified as urban. 

See our paper for more information: https://arxiv.org/abs/2510.12417. 

## Sources

**USA (1850–2020)**  
- IPUMS Full Count (1850–1940): https://usa.ipums.org/usa/full_count.shtml  
- NHGIS Place points & population time series (1990–2020):  
  - Time series: https://www.nhgis.org/time-series-tables  
  - Place identifiers: https://www.nhgis.org/place-points#identifiers

**Global (1975–2025)**  
- GHSL SMOD 2023 (degree of urbanization): https://human-settlement.emergency.copernicus.eu/ghs_smod2023.php  
- GHSL POP (population rasters): https://human-settlement.emergency.copernicus.eu/ghs_pop.php  
- CShapes (country boundaries): https://icr.ethz.ch/data/cshapes/


## File and column documentation

### `usa/usa_census_place_stable_geom.gpkg`
**What:** Point locations for **stable census places** (subset of NHGIS places) with cleaned/harmonized population histories.

- `census_place_id` — NHGISPLACE code (see NHGIS identifiers link above).
- `geom` - CRS **EPSG:5070** (Conus Albers).

**What are stable census places?**  
We unify historical (1850–1940) “historical census places” and modern (1990–2020) NHGIS places, resolve disappearing places and implausible spikes using migration‑informed linkages (via IPUMS linking). Result: a harmonized, cleaner place‑level panel for long‑run analysis (full pipeline in the [paper](https://arxiv.org/abs/2510.12417)).

---

### `usa/usa_census_place_stable_population.csv`
**What:** Population time series for **stable census places** (1850–2020; missing years: **1890, 1950, 1960, 1970, 1980**).

- `census_place_id` — NHGISPLACE code.  
- `year` — observation year.  
- `population` — total persons.

**Provenance:**  
Historical (1850–1940) counts reconstructed from IPUMS Full Count + Census Place Project mapping; modern (1990–2020) from NHGIS time series. 

---

### `usa/usa_cluster_growth_geom.gpkg`
**What:** Stable **cluster_growth** geometries for USA.

  - `cluster_id` — deterministic ID = concat of `y1`, `y2`, `urban_threshold` (people‑per‑pixel), and an **integer index** obtained by sorting cluster centroids west→east, then north→south.  
  - `y1` — start year.  
  - `y2` — end year.  
  - `urban_threshold` — people‑per‑pixel threshold to be classified as urban (**50**, **100**, **200**).
  - `geom` - CRS **EPSG:5070**.

**Method:** See *What is a "cluster growth"* above and the paper.

---

### `usa/usa_cluster_growth_population.csv`
**What:** Population of each USA **cluster_growth** at $y_1$ and $y_2$ (also includes rows where $y_1$ = $y_2$ for single‑year snapshots).

- `cluster_id` — matches geometry file.  
- `y1` — start year.  
- `y2` — end year.  
- `urban_threshold` — matches geometry file.  
- `population_y1` — persons within the **stable** boundary measured at $y_1$.  
- `population_y2` — persons within the **stable** boundary measured at $y_2$.

**Computation:** Sum the populations of all **census places** that fall inside the stable cluster boundary at the corresponding year. Growth can be computed as $\text{population\_y2} / \text{population\_y1}$ (or log‑difference).

---

### `world/world_cluster_growth_geom.gpkg`
**What:** Stable **cluster_growth** geometries for the world derived from **GHSL SMOD**.

  - `cluster_id` — deterministic ID = concat of `y1`, `y2`, `urban_threshold`, and an **integer index** (centroid sort west→east, then north→south).  
  - `y1` — start year.  
  - `y2` — end year.  
  - `urban_threshold` — SMOD degree‑of‑urbanization threshold (**exclusive**; 21, 22, 23 as above).
  - `geom` - CRS **EPSG:54009** (Mollweide).

**Method:** See *What is a "cluster growth"* above and the paper. The degree of urbanization threshold comes from GHSL SMOD (see link above).

---

### `world/world_cluster_growth_population_country.csv`
**What:** Population of each global **cluster_growth** at $y_1$ and $y_2$.

- `cluster_id` — matches geometry file.  
- `y1` — start year.  
- `y2` — end year.  
- `urban_threshold` — matches geometry file.  
- `population_y1` — persons from **GHSL POP** cells intersecting the stable boundary at $y_1$.  
- `population_y2` — persons from **GHSL POP** cells intersecting the stable boundary at $y_2$.  
- `country` - World Bank code of the country in which the cluster is located. Country boundaries are 2019 boundaries according to the Cshapes database. 

---

### `world/world_tot_pop_share_cities_above_1m_with_projections.csv`
**What:** Data and projections of the share of world population living in cities of 1 million people or more between 1975 and 2100. 
These are the data and projections used in the main analysis of our [paper](https://arxiv.org/abs/2510.12417), estimated with the methodology described there. 

- `country` - World Bank code of the country in which the cluster is located. Country boundaries are 2019 boundaries according to the Cshapes database.  
- `year` – Calendar year (1975–2100).
- `total_population_share_cities_above_one_million` – Share of the total population living in cities with more than 1,000,000 residents in that year (fraction between 0 and 1). This is projected as explained in the paper. 
- `population` – Total national population for that country and year (number of persons).
- `is_projection` – Indicator for whether the row is a projection (1) or an observed/estimated historical value (0).

---

## Coordinate Reference Systems (CRS)

- **USA:** EPSG:5070 (NAD83 / Conus Albers). Units: meters.  
- **World:** EPSG:54009 (Mollweide). Units: meters.  
- Files are **GPKG** carrying coordinates in these CRSs.

---

## Quick start (GeoPandas)

```python
import pandas as pd, geopandas as gpd

# USA clusters (geometry)
g = gpd.read_file("usa/usa_cluster_growth_geom.gpkg")

if g.crs is None or (g.crs.to_epsg() or 0) != 5070:
    g = g.set_crs(5070, allow_override=True)

# Join population
p = pd.read_csv("usa/usa_cluster_growth_population.csv")
gpop = g.merge(p, on=["cluster_id","y1","y2","urban_threshold"], how="left")

# Example: compute growth ratio
gpop["growth_ratio"] = gpop["population_y2"] / gpop["population_y1"]

```

