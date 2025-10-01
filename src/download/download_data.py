from pathlib import Path
from re import A
import requests
import zipfile
from typing import List
from ipumspy import MicrodataExtract, IpumsApiClient, AggregateDataExtract, TimeSeriesTable
import gzip
import shutil
import os


def download_raw_data_zenodo(folder_path: Path, access_token: str):
    r = requests.get('https://zenodo.org/api/deposit/depositions/17240844/files',
                 headers={'Authorization': f'Bearer {access_token}'})

    print(r.status_code)
    print(r.json())
    files = r.json()
    file_url = None
    for f in files:
        if f['filename'] == 'raw_data.zip':
            file_url = f['links']['download']
            break

    print(file_url)
    if file_url is None:
        raise ValueError("File not found")

    r = requests.get(file_url, headers={'Authorization': f'Bearer {access_token}'})
    file_path = folder_path / "raw_data.zip"
    with open(file_path, "wb") as f:
        f.write(r.content)
    
    print(f"Extracting raw data from {folder_path}")
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(folder_path)

    os.remove(file_path)


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


def download_nhgis_1900_2010_place_geom(folder_path: Path, ipums_api_key: str):
    pass


def upload_raw_data_to_zenodo():
    zenodo_bucket_url = 'https://zenodo.org/api/files/eaa853fb-d5cb-4c92-81f7-1b382a24e04f'
    filename = 'raw_data.zip'
    file_path = f'/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/readme/{filename}'
    access_token = 'faOt2P8QYRHkl1i68SnG961MZO6tN4kXtnSWqgimYaAQTnetiG2uBjSxv9fV'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    with open(file_path, 'rb') as f:
        r = requests.put(
            f'{zenodo_bucket_url}/{filename}',
            data=f,
            headers=headers
        )
    print(r.status_code)
    print(r.json())


def upload_readme_to_zenodo():
    zenodo_bucket_url = 'https://zenodo.org/api/files/eaa853fb-d5cb-4c92-81f7-1b382a24e04f'
    filename = 'README_RAW_DATA.md'
    file_path = f'/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/readme/{filename}'
    access_token = 'faOt2P8QYRHkl1i68SnG961MZO6tN4kXtnSWqgimYaAQTnetiG2uBjSxv9fV'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    with open(file_path, 'rb') as f:
        r = requests.put(
            f'{zenodo_bucket_url}/{filename}',
            data=f,
            headers=headers
        )
    print(r.status_code)
    print(r.json())
    

def upload_data_to_zenodo():
    ZENODO_BUCKET_URL = 'https://zenodo.org/api/files/eaa853fb-d5cb-4c92-81f7-1b382a24e04f'
    import time
    data_path = '/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data'
    access_token = 'faOt2P8QYRHkl1i68SnG961MZO6tN4kXtnSWqgimYaAQTnetiG2uBjSxv9fV'
    deposition_id = '17233451'

    import requests
    headers = {'Authorization': f'Bearer {access_token}'}

    # Test if the access token is working
    r = requests.get('https://zenodo.org/api/deposit/depositions',
                  headers=headers)
    print(r.status_code)
    print(r.json())

    time.sleep(1)

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'}
    # Test if deposit is working
    r = requests.post(f'https://zenodo.org/api/deposit/depositions',
                   json={},
                   headers=headers)
    print(r.status_code)
    print(r.json())

    time.sleep(1)
    # Get the bucket url

    bucket_url = r.json()['links']['bucket']
    print(bucket_url)

    filename = 'raw_data.zip'
    file_path = f'{data_path}/{filename}'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    with open(file_path, 'rb') as f:
        r = requests.put(
            f'{bucket_url}/{filename}',
            data=f,
            headers=headers
        )
    print(r.status_code)
    print(r.json())
        

if __name__ == "__main__":
    IPUMS_API_KEY = '59cba10d8a5da536fc06b59dcdeaab900afb48928d79a11ad7e5a1b5'
    RAW_DATA_ROOT_PATH = Path("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data_temp")

    # download_ipums_full_count_year(folder_path=RAW_DATA_ROOT_PATH / "ipums_usa_full_count", ipums_api_key=IPUMS_API_KEY, year=1850, sample_id="us1850c", variables=["YEAR", "HISTID", "HIK"], data_format="csv")

    # download_nhgis_1990_2020_place_population(folder_path=RAW_DATA_ROOT_PATH / "nhgis" / "pop", ipums_api_key=IPUMS_API_KEY)
   
    # upload_readme_to_zenodo()
    import requests
    ACCESS_TOKEN = 'faOt2P8QYRHkl1i68SnG961MZO6tN4kXtnSWqgimYaAQTnetiG2uBjSxv9fV'

    r = requests.get('https://zenodo.org/api/deposit/depositions/17240844/files',
                 headers={'Authorization': f'Bearer {ACCESS_TOKEN}'})

    print(r.status_code)
    print(r.json())
    files = r.json()
    file_url = None
    for f in files:
        if f['filename'] == 'raw_data.zip':
            file_url = f['links']['download']
            break

    print(file_url)
    if file_url is None:
        raise ValueError("File not found")
        
    # download_raw_data_zenodo(folder_path=RAW_DATA_ROOT_PATH, zenodo_download_url="https://zenodo.org/record/17240844/files/raw_data.zip")

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