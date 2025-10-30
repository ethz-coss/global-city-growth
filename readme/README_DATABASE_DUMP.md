# Large cities lose their growth advantage as urban systems mature -- Database dump

This repository contains a single SQL dump for the paper "Large cities lose their growth advantage as urban systems mature". 
This paper studies city growth with a harmonized definition of city across space and time in:
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

- [Documentation](https://andreamusso96.github.io/global-city-growth-pipeline-doc/) explaining the tables and columns in the database dump
- The [paper](https://arxiv.org/abs/2510.12417)
- [GitHub repository](https://github.com/andreamusso96/global-city-growth)