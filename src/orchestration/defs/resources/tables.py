from dataclasses import dataclass
from pathlib import Path

# Paths are grouped by data source, and then by USA vs World.

@dataclass(frozen=True)
class IpumsFullCount:
    def ipums_full_count_table_raw(self, year: int) -> str:
        return f"ipums_full_count_raw_{year}" # TODO: change name to ipums_full_count_table_{year}_raw
    
    def ipums_full_count_table_clean(self, year: int) -> str:
        return f"ipums_full_count_clean_{year}"
    
    def crosswalk_hist_id_to_hist_census_place_table_raw(self, year: int) -> str:
        return f"crosswalk_hist_id_to_hist_census_place_raw_{year}" # TODO: change name to crosswalk_hist_id_to_hist_census_place_table_{year}_raw
    
    def crosswalk_hist_id_to_hist_census_place_table_clean(self, year: int) -> str:
        return f"crosswalk_hist_id_to_hist_census_place_clean_{year}"
    
    def ipums_full_count_census_with_census_place_id(self, year: int) -> str:
        return f"ipums_full_count_census_with_census_place_id_{year}"
    
    def ipums_full_count_census_with_census_place_id_all_years(self) -> str:
        return f"ipums_full_count_census_with_census_place_id_all_years"
    
    def ipums_full_count_individual_migration(self) -> str:
        return f"ipums_full_count_individual_migration"
    
    def census_place_population(self) -> str:
        return f"census_place_population"
    
    def census_place_migration(self) -> str:
        return f"census_place_migration"
    

@dataclass(frozen=True)
class USASources:
    def usa_hist_census_place_population(self) -> str:
        return "usa_hist_census_place_population"
    
    def usa_hist_census_place_migration(self) -> str:
        return "usa_hist_census_place_migration"
    
    def usa_nhgis_census_place_population_raw_1990_2020(self) -> str:
        return "usa_nhgis_census_place_population_1990_2020_raw"
    
    def usa_nhgis_census_place_geom_all_years_raw(self) -> str:
        return "usa_nhgis_census_place_geom_all_years_raw"
    
    def usa_hist_census_place_geom_raw(self) -> str:
        return "usa_hist_census_place_geom_raw"
    
    def usa_states_geom_raw(self) -> str:
        return "usa_states_geom_raw"
    

@dataclass(frozen=True)
class USATransformations:
    def usa_nhgis_census_place_equivalence_network(self) -> str:
        return "usa_nhgis_census_place_equivalence_network"
    
    def usa_crosswalk_nhgis_census_place_to_connected_component(self) -> str:
        return "usa_crosswalk_nhgis_census_place_to_connected_component"
    
    def usa_raster_census_place(self) -> str:
        return "usa_raster_census_place"

    def usa_raster_census_place_convolved_year(self, year: int) -> str:
        return f"usa_raster_census_place_convolved_{year}"
    
    def usa_raster_census_place_convolved_all_years(self) -> str:
        return f"usa_raster_census_place_convolved_all_years"
    
    def usa_cluster_base_geom(self) -> str:
        return "usa_cluster_base_geom"
    
    def usa_cluster_base_matching(self) -> str:
        return "usa_cluster_base_matching"
    
    def usa_crosswalk_component_id_to_cluster_id(self) -> str:
        return "usa_crosswalk_component_id_to_cluster_id"
    
@dataclass(frozen=True)
class USAFinal:
    def usa_size_vs_growth(self) -> str:
        return "usa_size_vs_growth"
    
    def usa_rank_vs_size(self) -> str:
        return "usa_rank_vs_size"
    

@dataclass(frozen=True)
class USA:
    ipums_full_count: IpumsFullCount = IpumsFullCount()
    sources: USASources = USASources()
    transformations: USATransformations = USATransformations()
    final: USAFinal = USAFinal()


class WorldSources:
    def world_country_borders_raw(self) -> str:
        return "world_country_borders_raw"
    
    def world_urbanization_raw(self) -> str:
        return "world_urbanization_raw"
    
    def world_raster_ghsl_pop(self, year: int) -> str:
        return f"world_raster_ghsl_pop_{year}"

    def world_raster_ghsl_smod(self, year: int) -> str:
        return f"world_raster_ghsl_smod_{year}"
    
    def world_raster_ghsl_pop_all_years(self) -> str:
        return "world_raster_ghsl_pop_all_years"
    
    def world_raster_ghsl_smod_all_years(self) -> str:
        return "world_raster_ghsl_smod_all_years"
    
    def world_crosswalk_cshapes_code_to_iso_code(self) -> str:
        return "world_crosswalk_cshapes_code_to_iso_code"


@dataclass(frozen=True)
class World:
    sources: WorldSources = WorldSources()


@dataclass(frozen=True)
class TableNames:
    usa: USA = USA()
    world: World = World()