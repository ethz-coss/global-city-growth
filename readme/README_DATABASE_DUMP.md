# Global City Growth — Database Dump

This repository contains a single SQL dump for the Global City Growth Project. This project studies city growth with a harmonized definition of city across space and time across:
- **World:** 1975–2025
- **United States:** 1850–2020  

## Contents
- `db_dump.sql` — full schema **and** data

## Quick start

**PostgreSQL**
```bash
createdb city_growth
psql -d city_growth -f city_growth_dump.sql
```

## Documentation & Paper

- [Documentation](https://andreamusso96.github.io/global-city-growth-pipeline-doc/) expalaining the tables and columns in the database dump
- The [paper](LINK TO PAPER)
- [Github repository](https://github.com/andreamusso96/global-city-growth)