

import os
import ipumspy
import zipfile
import shutil
from pathlib import Path
from ipumspy import AggregateDataExtract, IpumsApiClient


def download_nhgis_1900_2010_place_geom_year(folder_path: Path, ipums_api_key: str, year: int):
    extract = AggregateDataExtract(
        collection="nhgis",
        description=f"NHGIS Place Points {year}",
        shapefiles=[f"us_place_point_{year}_tlgnis"]
    )
    ipums = IpumsApiClient(ipums_api_key)
    ipums.submit_extract(extract)
    ipums.wait_for_extract(extract)
    ipums.download_extract(extract, download_dir=folder_path
    )

    extract_zip_file_name = f"nhgis{extract.extract_id:04d}_shape.zip"
    extract_zip_file_path = folder_path / extract_zip_file_name
    with zipfile.ZipFile(extract_zip_file_path, "r") as zip_ref:
        zip_ref.extractall(folder_path)

    extract_folder_path = folder_path / f"nhgis{extract.extract_id:04d}_shape"
    shapefile_zip_file_path = extract_folder_path / f"nhgis{extract.extract_id:04d}_shapefile_tlgnis_us_place_point_{year}.zip"
    new_shapefile_folder_path = folder_path / f"nhgis_shapefile_tlgnis_us_place_point_{year}"
    
    with zipfile.ZipFile(shapefile_zip_file_path, "r") as zip_ref:
        zip_ref.extractall(new_shapefile_folder_path)

    shutil.rmtree(extract_folder_path)
    os.remove(extract_zip_file_path)


from typing import Dict, Any
def _load_data_catalog(file_path: Path) -> Dict[str, Any]:
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def _get_inputs_raw_data_zenodo(data_catalog: Dict[str, Any]) -> dict:
    zenodo_inputs = data_catalog["raw_data_zenodo"]
    dest_dir_relative_path = zenodo_inputs["download_dir"]
    download_url = zenodo_inputs["url"]
    return download_url, dest_dir_relative_path

def _get_inputs_ipums_usa_full_count(data_catalog: Dict[str, Any]) -> dict:
    ipums_inputs = data_catalog["ipums_usa_full_count"]
    dest_dir_relative_path = ipums_inputs["download_dir"]

    extract_definitions = ipums_inputs["extract_definitions"]
    years = extract_definitions["years"]
    samples = extract_definitions["samples"]
    variables = extract_definitions["variables"]
    data_format = extract_definitions["data_format"]
    return years, samples, variables, data_format, dest_dir_relative_path

def _get_inputs_nhgis_place_population_1990_2020(data_catalog: Dict[str, Any]) -> dict:
    nhgis_inputs = data_catalog["nhgis_place_population_1990_2020"]
    dest_dir_relative_path = nhgis_inputs["download_dir"]
    extract_definitions = nhgis_inputs["extract_definitions"]
    table_name = extract_definitions["table_name"]
    years = extract_definitions["years"]
    geog_level = extract_definitions["geog_level"]
    return table_name, years, geog_level, dest_dir_relative_path


def _get_inputs_nhgis_place_geom_1900_2010(data_catalog: Dict[str, Any]) -> dict:
    nhgis_inputs = data_catalog["nhgis_place_geom_1900_2010"]
    dest_dir_relative_path = nhgis_inputs["download_dir"]
    extract_definitions = nhgis_inputs["extract_definitions"]
    years = extract_definitions["years"]
    shapefile_name_template = extract_definitions["shapefile_name_template"]
    return years, shapefile_name_template, dest_dir_relative_path

if __name__ == "__main__":
    import yaml
    with open("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data_catalog.yml", "r") as f:
        data_catalog = yaml.safe_load(f)
    print(data_catalog)




    """





    from pathlib import Path
    IPUMS_API_KEY = '59cba10d8a5da536fc06b59dcdeaab900afb48928d79a11ad7e5a1b5'
    RAW_DATA_ROOT_PATH = Path("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data_temp")

    folder_path = RAW_DATA_ROOT_PATH / "nhgis" / "geo"
    for year in [1990, 1920]:
        download_nhgis_1900_2010_place_geom_year(folder_path=folder_path, ipums_api_key=IPUMS_API_KEY, year=year)
    
    IPUMS_API_KEY = '59cba10d8a5da536fc06b59dcdeaab900afb48928d79a11ad7e5a1b5'
    RAW_DATA_ROOT_PATH = Path("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data_temp")
    from ipumspy import AggregateDataExtract, IpumsApiClient
    extract = AggregateDataExtract(
        collection="nhgis",
        description="NHGIS Place Points 1900",
        shapefiles=["us_place_point_1900_tlgnis"]
    )

    ipums = IpumsApiClient(IPUMS_API_KEY)
    ipums.submit_extract(extract)
    print(f"Extract submitted with id {extract.extract_id}")
    ipums.wait_for_extract(extract)
    folder_path = RAW_DATA_ROOT_PATH / "nhgis" / "geo"
    ipums.download_extract(extract, download_dir=folder_path)

    extract_folder_name = f"nhgis{extract.extract_id:04d}_shape.zip"
    extract_folder_path = folder_path / extract_folder_name
    with zipfile.ZipFile(extract_folder_path, "r") as zip_ref:
        zip_ref.extractall(folder_path)"""



    """
    file_name = f"nhgis{extract.extract_id:04d}_ts_geog2010_place.csv"
    file_path =  extract_folder_path_unzipped / file_name

    new_file_name = f"nhgis_place_population_1990_2020.csv"
    new_file_path = folder_path / new_file_name
    shutil.move(file_path, new_file_path)
    # Clean up
    shutil.rmtree(extract_folder_path_unzipped)
    os.remove(extract_folder_path)"""
