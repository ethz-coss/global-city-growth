from ipumspy import IpumsApiClient, MicrodataExtract, AggregateDataExtract, TimeSeriesTable
import requests
import zipfile
import gzip
import shutil
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import yaml


def _unzip(zip_path: Path, dest_dir: Path, delete_zip: bool = True) -> Path:
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(dest_dir)
    if delete_zip:
        os.remove(zip_path)
    return dest_dir

def _gunzip(src_gz: Path, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(src_gz, "rb") as fin, open(dest, "wb") as fout:
        shutil.copyfileobj(fin, fout)
    return dest

def _move(src: Path, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dest))
    return dest

def _clean_path(p: Path) -> None:
    if p.is_dir():
        shutil.rmtree(p)
    elif p.exists():
        os.remove(p)
        

def download_raw_data_zenodo(dest_dir: Path, download_url: str, access_token: str) -> None:
    # TODO: Remove access token when Zenodo is published
    target_filename = "raw_data.zip"
    zip_path = dest_dir / target_filename
    
    resp = requests.get(download_url, headers={"Authorization": f"Bearer {access_token}"})

    if resp.status_code != 200:
        raise ValueError(f"Failed to download raw data from Zenodo: {resp.status_code} {resp.text}")

    dest_dir.parent.mkdir(parents=True, exist_ok=True)
    with open(zip_path, "wb") as f:
        f.write(resp.content)

    _unzip(zip_path=zip_path, dest_dir=dest_dir, delete_zip=True)


def _submit_wait_download(ipums: IpumsApiClient, extract, download_dir: Path) -> int:
    ipums.submit_extract(extract)
    ipums.wait_for_extract(extract)
    ipums.download_extract(extract, download_dir=download_dir)
    return extract.extract_id

def download_ipums_full_count_year(dest_dir: Path, ipums_api_key: str, year: int, sample_id: str, variables: List[str], data_format: str) -> None:
    ipums = IpumsApiClient(ipums_api_key)
    extract = MicrodataExtract(
        collection="usa",
        description=f"IPUMS USA Full Count {year}",
        samples=[sample_id],
        variables=variables,
        data_format=data_format,
    )

    extract_id = _submit_wait_download(ipums=ipums, extract=extract, download_dir=dest_dir)
    extract_id_formatted = f"{extract_id:05d}"

    gz_name = f"usa_{extract_id_formatted}.{data_format}.gz"
    gz_path = dest_dir / gz_name

    final_name = f"ipums_full_count_{year}.{data_format}"
    final_path = dest_dir / final_name

    _gunzip(gz_path, final_path)
    os.remove(gz_path)

    # Remove companion XML
    xml_path = dest_dir / f"usa_{extract_id_formatted}.xml"
    if xml_path.exists():
        os.remove(xml_path)


def download_nhgis_1990_2020_place_population(dest_dir: Path, ipums_api_key: str, table_name: str, years: List[str], geog_level: str) -> Path:
    extract = AggregateDataExtract(
        collection="nhgis",
        description="NHGIS time series: place population 1990–2020",
        time_series_tables=[
            TimeSeriesTable(table_name, geog_levels=[geog_level], years=years)
        ],
        tst_layout="time_by_row_layout",
        dataFormat="csv_header",
    )

    ipums = IpumsApiClient(ipums_api_key)
    extract_id = _submit_wait_download(ipums, extract, dest_dir)

    zip_name = f"nhgis{extract_id:04d}_csv.zip"
    zip_path = dest_dir / zip_name

    _unzip(zip_path, dest_dir, delete_zip=True)

    unzipped_dir = dest_dir / f"nhgis{extract_id:04d}_csv"
    source_csv = unzipped_dir / f"nhgis{extract_id:04d}_ts_geog2010_place.csv"
    final_csv = dest_dir / "nhgis_place_population_1990_2020.csv"

    _move(source_csv, final_csv)
    _clean_path(unzipped_dir)

def download_nhgis_1900_2010_place_geom_year(dest_dir: Path, ipums_api_key: str, year: int, shapefile_name: str) -> Path:
    extract = AggregateDataExtract(
        collection="nhgis",
        description=f"NHGIS Place Points {year}",
        shapefiles=[shapefile_name],
    )

    ipums = IpumsApiClient(ipums_api_key)
    extract_id = _submit_wait_download(ipums, extract, dest_dir)

    outer_zip_name = f"nhgis{extract_id:04d}_shape.zip"
    outer_zip_path = dest_dir / outer_zip_name

    _unzip(outer_zip_path, dest_dir, delete_zip=True)

    extracted_dir = dest_dir / f"nhgis{extract_id:04d}_shape"
    inner_zip_name = f"nhgis{extract_id:04d}_shapefile_tlgnis_us_place_point_{year}.zip"
    inner_zip_path = extracted_dir / inner_zip_name

    final_dir = dest_dir / f"nhgis_shapefile_tlgnis_us_place_point_{year}"
    _unzip(inner_zip_path, final_dir, delete_zip=False)  # keep the inner zip if you like; delete if not needed

    _clean_path(extracted_dir)


def _load_data_catalog(file_path: Path) -> Dict[str, Any]:
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def _get_inputs_raw_data_zenodo(data_catalog: Dict[str, Any], root_data_dir: Path) -> dict:
    zenodo_inputs = data_catalog["raw_data_zenodo"]
    dest_dir_relative_path = zenodo_inputs["download_dir"]
    dest_dir = root_data_dir / dest_dir_relative_path
    download_url = zenodo_inputs["url"]
    return download_url, dest_dir

def _get_inputs_ipums_usa_full_count(data_catalog: Dict[str, Any], root_data_dir: Path) -> dict:
    ipums_inputs = data_catalog["ipums_usa_full_count"]
    dest_dir_relative_path = ipums_inputs["download_dir"]
    dest_dir = root_data_dir / dest_dir_relative_path
    extract_definitions = ipums_inputs["extract_definitions"]
    years = extract_definitions["years"]
    samples = extract_definitions["samples"]
    variables = extract_definitions["variables"]
    data_format = extract_definitions["data_format"]
    return years, samples, variables, data_format, dest_dir

def _get_inputs_nhgis_place_population_1990_2020(data_catalog: Dict[str, Any], root_data_dir: Path) -> dict:
    nhgis_inputs = data_catalog["nhgis_place_population_1990_2020"]
    dest_dir_relative_path = nhgis_inputs["download_dir"]
    dest_dir = root_data_dir / dest_dir_relative_path
    extract_definitions = nhgis_inputs["extract_definitions"]
    table_name = extract_definitions["table_name"]
    years = extract_definitions["years"]
    geog_level = extract_definitions["geog_level"]
    return table_name, years, geog_level, dest_dir


def _get_inputs_nhgis_place_geom_1900_2010(data_catalog: Dict[str, Any], root_data_dir: Path) -> dict:
    nhgis_inputs = data_catalog["nhgis_place_geom_1900_2010"]
    dest_dir_relative_path = nhgis_inputs["download_dir"]
    dest_dir = root_data_dir / dest_dir_relative_path
    extract_definitions = nhgis_inputs["extract_definitions"]
    years = extract_definitions["years"]
    shapefile_name_template = extract_definitions["shapefile_name_template"]
    return years, shapefile_name_template, dest_dir

if __name__ == "__main__":
    IPUMS_API_KEY = '59cba10d8a5da536fc06b59dcdeaab900afb48928d79a11ad7e5a1b5'
    ROOT_DATA_DIR = Path("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data_temp")
    ZENODO_ACCESS_TOKEN = 'faOt2P8QYRHkl1i68SnG961MZO6tN4kXtnSWqgimYaAQTnetiG2uBjSxv9fV'
    download_url = 'https://zenodo.org/api/records/17240844/draft/files/raw_data.zip/content'
    data_catalog_path = Path("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data_catalog.yml")

    data_catalog = _load_data_catalog(data_catalog_path)
    dest_dir, download_url = _get_inputs_raw_data_zenodo(data_catalog, ROOT_DATA_DIR)
    # download_raw_data_zenodo(dest_dir=download_dir, download_url=download_url, access_token=ZENODO_ACCESS_TOKEN)
    """
    years, samples, variables, data_format, dest_dir = _get_inputs_ipums_usa_full_count(data_catalog, ROOT_DATA_DIR)
    year_sample = zip(years, samples)
    for year, sample in year_sample:
        print(f"Downloading IPUMS USA Full Count {year} {sample}")
        download_ipums_full_count_year(dest_dir=dest_dir, ipums_api_key=IPUMS_API_KEY, year=year, sample_id=sample, variables=variables, data_format=data_format)"""

    table_name, years, geog_level, dest_dir = _get_inputs_nhgis_place_population_1990_2020(data_catalog, ROOT_DATA_DIR)
    print(f"Downloading NHGIS Place Population 1990-2020")
    download_nhgis_1990_2020_place_population(dest_dir=dest_dir, ipums_api_key=IPUMS_API_KEY, table_name=table_name, years=years, geog_level=geog_level)

    years, shapefile_name_template, dest_dir = _get_inputs_nhgis_place_geom_1900_2010(data_catalog, ROOT_DATA_DIR)
    shapefile_names = [shapefile_name_template.format(year=year) for year in years]
    year_shapefile_name = zip(years, shapefile_names)
    for year, shapefile_name in year_shapefile_name:
        print(f"Downloading NHGIS Place Geometry {year} {shapefile_name}")
        download_nhgis_1900_2010_place_geom_year(dest_dir=dest_dir, ipums_api_key=IPUMS_API_KEY, year=year, shapefile_name=shapefile_name)