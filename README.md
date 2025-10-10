# Global City Growth

_A harmonized view of how cities grew in the United States (1850–2020) and worldwide (1975–2025), with reproducible code and shareable data._

[IMAGE GOES HERE]

## Summary

Over the past 50 years, urban populations have become increasingly concentrated in large cities. In 1975, the average country had 18\% of its urban residents living in cities of at least one million people. By 2025, that average had risen to 39\%. Are we headed toward a world where most urbanites live in large cities, or will this concentration trend level off? Using new data built from satellite grids from GHSL and historical censuses from IPUMS, we show that concentration in large cities slows as urban systems mature, and project a global slowdown in the rise of the large cities in the near future. Read the paper here: [PAPER LINK HERE]

## What’s in this project

Harmonized **boundaries** and **populations**, comparable across **time** and **countries** for:
  - **World:** 1975–2025  
  - **United States:** 1850–2020  

You can find the data here: [DATA LINK HERE]

**Reproducible pipeline**
  - End‑to‑end DAG (100+ tasks) over ≈**300 GB** of raster, vector, CSV, and Parquet.
  - Outputs **200+ tables** to **PostgreSQL** (primary store) and **DuckDB** (analytics).
  - Containerized with **Docker**; orchestrated with **Dagster**.
  - Reference run (Apple M2, 32 GB RAM): **~5 hours** for the full build.

## Important links

- Paper (arXiv): add link

- Raw data (Zenodo): add link

- Output data (Zenodo): add link

- Database dump (Zenodo): add link

## Quick start (replication)

### 0) Prerequisites
- **Docker:** https://www.docker.com/  
- **Git:** https://git-scm.com/  
- **IPUMS API key:** https://developer.ipums.org/docs/v2/get-started/

### 1) Get the code
```bash
git clone https://github.com/andreamusso96/global-city-growth.git
```
### 2) Enter the folder
```
cd global-city-growth
```

### 3) Configure environment
```
cp .env.example .env
```
Open `.env` and set the IPUMS_API_KEY=your_key_here. This is necessary to download IPUMS data (get your key [here](https://developer.ipums.org/docs/v2/get-started/))


### 4) Launch docker
```
docker compose up --build
```
First build usually takes ~3 minutes. 
This will create two docker container global-city-growth-database and global-city-growth-orchestrator. 
The first hosts a postgres database, the second hosts the code and is used to orchestrate the pipeline using Dagster. 

### 5) Orchestrate with Dagster

- Open Dagster UI: http://localhost:3000/
- Open the `Jobs` window in the top navigation bar 
- Run `0_all_job` to execute the full pipeline. The other jobs run subsets of the pipeline in case something breaks or you want to iterate
- You can monitor runs and asset materializations in the Dagster UI. [Quick guide](https://docs.dagster.io/guides/operate/webserver)

### 6) Outputs

- DuckDB and PostgreSQL are populated as configured in `.env`
- Tables and Figures are outputted in the same directory in the figures/ folder
- Inspect outputs with your favorite SQL tool or DuckDB CLI.
- Remember inputs are ≈300 GB; ensure ample free disk space for intermediates and outputs.

## How this repo is organized

The documentation for the pipeline is hosted here [DOCUMENTATION LINK HERE]

- readme/ - The readmes of the various repositories associated with the project
- src/ - The code
    - src/orchestration/ - The code for the dagster pipeline
    - src/warehouse/ - The code for the dbt parts of the pipeline
        - src/warehouse/macros - Dbt macros
        - src/warehouse/models - Dbt models

## Citation
[CITATION HERE]