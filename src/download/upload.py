import requests
from pathlib import Path


def upload_file_to_zenodo(file_path: Path, filename: str, access_token: str):
    zenodo_bucket_url = 'https://zenodo.org/api/files/eaa853fb-d5cb-4c92-81f7-1b382a24e04f'
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



def upload_raw_data_to_zenodo(access_token: str):
    filename = 'raw_data.zip'
    file_path = f'/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/data/zenodo/{filename}'
    upload_file_to_zenodo(file_path, filename, access_token)

def upload_readme_to_zenodo(access_token: str):
    filename = 'README_RAW_DATA.md'
    file_path = f'/Users/andrea/Desktop/PhD/Data/Pipeline/global-city-growth/readme/{filename}'
    upload_file_to_zenodo(file_path, filename, access_token)


    

if __name__ == "__main__":
    ZENODO_ACCESS_TOKEN = 'faOt2P8QYRHkl1i68SnG961MZO6tN4kXtnSWqgimYaAQTnetiG2uBjSxv9fV'
    upload_readme_to_zenodo(ZENODO_ACCESS_TOKEN)
    # upload_readme_to_zenodo(ZENODO_ACCESS_TOKEN)