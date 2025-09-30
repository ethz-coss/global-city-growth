from pathlib import Path
import requests
import zipfile
from typing import List
from ipumspy import MicrodataExtract, IpumsApiClient, AggregateDataExtract, TimeSeriesTable
import gzip
import shutil
import os

def download_raw_data_zenodo(folder_path: Path, zenodo_download_url: str):
    url  = f"{zenodo_download_url}?download=1"
    response = requests.get(url)
    print(f"Downloaded raw data from {url} to {folder_path}")
    with open(folder_path, "wb") as f:
        f.write(response.content)
    
    print(f"Extracting raw data from {folder_path}")
    with zipfile.ZipFile(folder_path, "r") as zip_ref:
        zip_ref.extractall(folder_path)

    folder_path.rmdir()


def download_ipums_full_count_year(folder_path: Path, ipums_api_key: str, year: int, sample_id: str, variables: List[str], data_format: str):
    ipums = IpumsApiClient(ipums_api_key)
    extract = MicrodataExtract(
        collection="usa",
        description=f"IPUMS USA Full Count {year}",
        samples=[sample_id],
        variables=variables,
        data_format=data_format,
    )
    ipums.submit_extract(extract)
    print(f"Extract submitted with id {extract.extract_id}")
    ipums.wait_for_extract(extract)
    print(f"Extract finished with id {extract.extract_id}")
    ipums.download_extract(extract, download_dir=folder_path)

    formatted_number = f"{extract.extract_id:05d}"
    file_name = f"usa_{formatted_number}.{data_format}.gz"
    file_path = folder_path / file_name
   
    new_file_name = f"ipums_full_count_{year}.{data_format}"
    new_file_path = folder_path / new_file_name

    with gzip.open(file_path, "rb") as fin, open(new_file_path, "wb") as fout:
        shutil.copyfileobj(fin, fout)

    os.remove(file_path)
    xml_file_name = f"usa_{formatted_number}.xml"
    xml_file_path = folder_path / xml_file_name
    os.remove(xml_file_path)

def download_nhgis_1990_2020_place_population(folder_path: Path, ipums_api_key: str):
    extract = AggregateDataExtract(
        collection="nhgis",
        description="An NHGIS example extract: time series tables",
        time_series_tables=[
            TimeSeriesTable("CL8", geog_levels=["place"], years=["1990", "2000", "2010", "2020"])
        ],
        tst_layout="time_by_row_layout",
        dataFormat="csv_header"
    )

    ipums = IpumsApiClient(ipums_api_key)
    ipums.submit_extract(extract)
    print(f"Extract submitted with id {extract.extract_id}")
    ipums.wait_for_extract(extract)
    ipums.download_extract(extract, download_dir=folder_path)

    extract_folder_name = f"nhgis{extract.extract_id:04d}_csv.zip"
    extract_folder_path = folder_path / extract_folder_name
    with zipfile.ZipFile(extract_folder_path, "r") as zip_ref:
        zip_ref.extractall(folder_path)

    extract_folder_path_unzipped = folder_path / extract_folder_name.replace(".zip", "")
    file_name = f"nhgis{extract.extract_id:04d}_ts_geog2010_place.csv"
    file_path =  extract_folder_path_unzipped / file_name

    new_file_name = f"nhgis_place_population_1990_2020.csv"
    new_file_path = folder_path / new_file_name
    shutil.move(file_path, new_file_path)
    # Clean up
    shutil.rmtree(extract_folder_path_unzipped)
    os.remove(extract_folder_path)


        

if __name__ == "__main__":
    IPUMS_API_KEY = '59cba10d8a5da536fc06b59dcdeaab900afb48928d79a11ad7e5a1b5'
    RAW_DATA_ROOT_PATH = Path("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data_temp")
    ZENODO_RECORD_ID = "7619080"

    # download_ipums_full_count_year(folder_path=RAW_DATA_ROOT_PATH / "ipums_usa_full_count", ipums_api_key=IPUMS_API_KEY, year=1850, sample_id="us1850c", variables=["YEAR", "HISTID", "HIK"], data_format="csv")

    # download_nhgis_1990_2020_place_population(folder_path=RAW_DATA_ROOT_PATH / "nhgis" / "pop", ipums_api_key=IPUMS_API_KEY)
    extract_folder_path_unzipped = RAW_DATA_ROOT_PATH / "nhgis" / "pop" / "nhgis0026_csv"
    extract_folder_path = RAW_DATA_ROOT_PATH / "nhgis" / "pop" / "nhgis0026_csv.zip"
    shutil.rmtree(extract_folder_path_unzipped)
    os.remove(extract_folder_path)

    """
    from ipumspy import AggregateDataExtract, TimeSeriesTable   
    extract = AggregateDataExtract(
    collection="nhgis",
    description="An NHGIS example extract: time series tables",
    time_series_tables=[
        TimeSeriesTable("CL8", geog_levels=["place"], years=["1990", "2000", "2010", "2020"])
    ],
    tst_layout="time_by_row_layout",
    dataFormat="csv_header"
    )
    ipums = IpumsApiClient(IPUMS_API_KEY)
    ipums.submit_extract(extract)
    print(f"Extract submitted with id {extract.extract_id}")
    ipums.wait_for_extract(extract)
    ipums.download_extract(extract, download_dir=RAW_DATA_ROOT_PATH / "nhgis" / "pop")

    folder_path = RAW_DATA_ROOT_PATH / "nhgis" / "pop"
    extract_id = 25
    extract_folder_name = f"nhgis{extract_id:04d}_csv.zip"
    extract_folder_path = folder_path / extract_folder_name
    with zipfile.ZipFile(extract_folder_path, "r") as zip_ref:
        zip_ref.extractall(folder_path)

    extract_folder_path_unzipped = folder_path / extract_folder_name.replace(".zip", "")
    file_name = f"nhgis{extract_id:04d}_ts_geog2010_place.csv"
    file_path =  extract_folder_path_unzipped / file_name

    new_file_name = f"nhgis_place_population_1990_2020.csv"
    new_file_path = folder_path / new_file_name

    shutil.move(file_path, new_file_path)
    os.remove(extract_folder_path_unzipped)"""



    """

    from ipumspy import MicrodataExtract, IpumsApiClient
    ipums = IpumsApiClient(IPUMS_API_KEY)
    print(f"Ipums API client initialized")
    extract = MicrodataExtract(
        collection="usa",
        description="Sample USA extract 2",
        samples=["us1850c"],
        variables=["YEAR", "HISTID", "HIK"],
        data_structure={"rectangular": {"on": "P"}},
    )

    ipums.submit_extract(extract)
    print(f"Extract submitted with id {extract.extract_id}")
    ipums.wait_for_extract(extract)
    download_dir = RAW_DATA_ROOT_PATH / "ipums_usa_full_count"
    download_dir.mkdir(parents=True, exist_ok=True)
    ipums.download_extract(extract, download_dir=download_dir)"""