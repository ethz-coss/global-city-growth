import requests
from os import fspath
from pathlib import Path
from typing import List, Dict, Any
import time
from enum import Enum
from pySmartDL import SmartDL

class IpumsAPIExtractType(Enum):
    IPUM_USA_FULL_COUNT = "ipums_usa_full_count"
    NHGIS_TIME_SERIES = "nhgis_time_series"
    NHGIS_SHAPEFILE = "nhgis_shapefile"


class IpumsAPIClientResource:
    api_key: str

    def __init__(self, api_key: str):
        self.api_key = api_key

    def _get_header(self) -> Dict[str, str]:
        return {'Authorization': self.api_key, 'Content-Type': 'application/json'}

    def _get_base_url_extract_request(self, collection: str) -> str:
        return f"https://api.ipums.org/extracts?collection={collection}&version=2"

    def _get_base_url_check_request_status(self, extract_id: int, extract_type: ExtractType) -> str:
        if extract_type == ExtractType.IPUM_USA_FULL_COUNT:
            collection = "usa"
        elif extract_type == ExtractType.NHGIS_TIME_SERIES or extract_type == ExtractType.NHGIS_SHAPEFILE:
            collection = "nhgis"
        return f"https://api.ipums.org/extracts/{extract_id}?collection={collection}&version=2"

    def _get_download_url(self, extract_type: ExtractType, response_json: Dict[str, Any]) -> str:
        if extract_type == ExtractType.IPUM_USA_FULL_COUNT:
            return response_json['downloadLinks']['data']['url']
        elif extract_type == ExtractType.NHGIS_TIME_SERIES:
            return response_json['downloadLinks']['tableData']['url']
        elif extract_type == ExtractType.NHGIS_SHAPEFILE:
            return response_json['downloadLinks']['gisData']['url']

    def _get_download_file_name(self, extract_id: int, extract_type: ExtractType) -> str:
        if extract_type == ExtractType.IPUM_USA_FULL_COUNT:
            return f"usa_{extract_id:05d}.csv.gz"
        elif extract_type == ExtractType.NHGIS_TIME_SERIES:
            return f"nhgis{extract_id:04d}_csv.zip"
        elif extract_type == ExtractType.NHGIS_SHAPEFILE:
            return f"nhgis{extract_id:04d}_shape.zip"

    def submit_ipums_usa_extract_request(self, description: str, sample: str, variables: List[str], data_format: str) -> int:
        base_url = self._get_base_url_extract_request(collection="usa")
        data_raw = {
            "description": description,
            "dataStructure": {
                "rectangular": {
                    "on": "P"
                }
            },
            "dataFormat": data_format,
            "samples": {sample: {}},
            "variables": {variable: {} for variable in variables}
        }
        response = requests.post(base_url, headers=self._get_header(), json=data_raw)
        print(response.status_code)
        print(response.json())
        extract_id = response.json()['number']
        return extract_id

    def submit_nhgis_time_series_extract_request(self, description: str, time_series_table: str, geog_level: str, years: List[str], data_format: str) -> int:
        base_url = self._get_base_url_extract_request(collection="nhgis")
        d = {
            "timeSeriesTables": {
                time_series_table: {
                    "geogLevels": [geog_level],
                    "years": years
                }
            },
            "timeSeriesTableLayout": "time_by_row_layout",
            "dataFormat": data_format,
            "description": description
        }
        response = requests.post(base_url, headers=self._get_header(), json=d)
        print(response.status_code)
        print(response.json())
        extract_id = response.json()['number']
        return extract_id

    def submit_nhgis_shapefile_extract_request(self, description: str, shapefile_name: str) -> int:
        base_url = self._get_base_url_extract_request(collection="nhgis")
        d = {
            "shapefiles": [shapefile_name],
            "description": description
        }
        response = requests.post(base_url, headers=self._get_header(), json=d)
        print(response.status_code)
        print(response.json())
        extract_id = response.json()['number']
        return extract_id

    def wait_for_extract_request(self, extract_id: int, extract_type: ExtractType, loop_delay: int = 1) -> None:
        base_url = self._get_base_url_check_request_status(extract_id=extract_id, extract_type=extract_type)
        status = None
        while status != "completed":
            if status == "cancelled" or status == "failed":
                raise ValueError(f"Extract {extract_id} failed")
            
            time.sleep(loop_delay)
            response = requests.get(base_url, headers=self._get_header())
            print(response.status_code)
            print(response.json())
            status = response.json()['status']

        download_url = self._get_download_url(extract_type=extract_type, response_json=response.json())
        return download_url

    def download_extract(self,  extract_id: int, extract_type: ExtractType, download_url: str, download_dir: Path) -> None:
        file_name = self._get_download_file_name(extract_id=extract_id, extract_type=extract_type)
        request_args = {"headers": self._get_header()}
        obj = SmartDL(urls=download_url, dest=fspath(download_dir / file_name), request_args=request_args)
        obj.start(blocking=True)

        
        

if __name__ == "__main__":
    IPUMS_API_KEY = "59cba10d8a5da536fc06b59dcdeaab900afb48928d79a11ad7e5a1b5"
    ipums = IpumsAPIClientResource(api_key=IPUMS_API_KEY)

    # IPUMS USA Full Count 1850
    """
    extract_id = ipums.submit_ipums_usa_extract_request(description="IPUMS USA Full Count 1850", sample="us1850c", variables=["YEAR", "HISTID", "HIK"], data_format="csv")
    download_url = ipums.wait_for_extract_request(extract_id=extract_id, extract_type=ExtractType.IPUM_USA_FULL_COUNT)
    download_dir = Path("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data_download_temp")
    ipums.download_extract(extract_id=extract_id, extract_type=ExtractType.IPUM_USA_FULL_COUNT, download_url=download_url, download_dir=download_dir)"""


    # IPUMS NHGIS Time Series 1990
    """
    extract_id = ipums.submit_nhgis_time_series_extract_request(description="IPUMS NHGIS Time Series 1990", time_series_table="CL8", geog_level="place", years=["1990", "2000", "2010", "2020"], data_format="csv_header")
    download_url = ipums.wait_for_extract_request(extract_id=extract_id, extract_type=ExtractType.NHGIS_TIME_SERIES)
    download_dir = Path("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data_download_temp")
    ipums.download_extract(extract_id=extract_id, extract_type=ExtractType.NHGIS_TIME_SERIES, download_url=download_url, download_dir=download_dir)"""


    # IPUMS NHGIS Shapefile 1900
    extract_id = ipums.submit_nhgis_shapefile_extract_request(description="IPUMS NHGIS Shapefile 1900", shapefile_name="us_place_point_1900_tlgnis")
    download_url = ipums.wait_for_extract_request(extract_id=extract_id, extract_type=ExtractType.NHGIS_SHAPEFILE)
    download_dir = Path("/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data_download_temp")
    ipums.download_extract(extract_id=extract_id, extract_type=ExtractType.NHGIS_SHAPEFILE, download_url=download_url, download_dir=download_dir)




