import json
import os
from importlib.resources import files

import requests
import tomllib
from tqdm import tqdm

from ..path import (
    _add_one_day,
    _date_start_to_month_dirpath,
    data_report_filename,
    data_report_name,
)

API_LOGIN_TOML_FILEPATH = r"Q:\repos\configs\private\data-inrix-api-login.toml"
REGIONS_JSON_PKGPATH = ("inrix.api", "configs/regions.json")


def load_toml(toml_filepath: str):
    with open(toml_filepath, "rb") as f:
        return tomllib.load(f)


def _print_json(j):
    print(json.dumps(j, indent=2))
    return


def get_token(email: str, password: str) -> str:
    payload = {
        "email": email,
        "password": password,
    }
    r = requests.post(
        "https://roadway-analytics-api.inrix.com/v1/auth", json=payload
    )
    _print_json(r.json())
    token_dict = r.json()["result"]["accessToken"]
    # print("API token expiry:", token_dict["expiry"])
    return token_dict["token"]


def _authorization_headers(token: str):
    return {"Authorization": f"Bearer {token}"}


def _load_region(
    name: str, regions_json_filepath: str = "configs/regions.json"
):
    """
    name: set in config["data"]["region"], references the keys in regions.json
    """
    return json.loads(
        files(REGIONS_JSON_PKGPATH[0])
        .joinpath(REGIONS_JSON_PKGPATH[1])
        .read_text()
    )[name]


def request_data(
    token: str,
    map_version: str,
    region: str,
    granularity: str,
    date_start: str,
    date_end: str,
    email: str,
) -> str:
    """
    Returns
    -------
    report_id: str
    """
    payload = {
        "mapVersion": map_version,
        "xdSegIds": None,
        "regions": [_load_region(region)],
        "granularity": granularity,  # minutes
        "unit": "IMPERIAL",
        "fields": [
            "LOCAL_DATE_TIME",
            "XDSEGID",
            "UTC_DATE_TIME",
            "SPEED",
            "NAS_SPEED",
            "REF_SPEED",
            "TRAVEL_TIME",
            "CVALUE",
            "SCORE",
            "CLOSURE",
        ],
        "dateRanges": [
            {
                "start": date_start,
                "end": date_end,
                "daysOfWeek": None,
            }
        ],
        "timeRanges": [],
        "timezone": "PST8PDT",
        "useRealSpeed": False,
        "emailAddresses": [email],
        "corridors": None,  # only in docs example, not in main text
        "includeClosures": True,  # only in docs example, not in main text
        "reportName": data_report_name(
            region, date_start, _add_one_day(date_end), granularity
        ),  # only in docs example, not in main text
        "reportType": "DATA_DOWNLOAD",  # only in docs example, not in main text
    }
    r = requests.post(
        "https://roadway-analytics-api.inrix.com/v1/data-downloader",
        headers=_authorization_headers(token),
        json=payload,
    )
    _print_json(r.json())
    return r.json()["reportId"]


def report_status(token: str, report_id: str):
    r = requests.get(
        f"https://roadway-analytics-api.inrix.com/v1/report/status/{report_id}",
        headers=_authorization_headers(token),
    )
    _print_json(r.json())
    return r.json()


def report_completed(token: str, report_id: str) -> bool:
    status = report_status(token, report_id)
    return status["state"] == "COMPLETED"


def _download_file(filepath: str, url: str):
    """Download file from a URL.

    Copied straight from requests docs.

    Parameters
    ----------
    filepath : str
        local filepath
    url : str
        URL of file to be downloaded
    """
    chunk_size = 1024
    r = requests.get(url, stream=True)
    with open(filepath, "wb") as f, tqdm(
        desc=filepath,
        total=int(r.headers.get("Content-Length")),
        unit="iB",  # since chunk_size = 1024
        unit_scale=True,
        unit_divisor=chunk_size,
    ) as pbar:
        for chunk in r.iter_content(chunk_size=chunk_size):
            f.write(chunk)
            pbar.update(chunk_size)
    return


def download_data(
    token: str,
    report_id: str,
    map_version: str,
    region: str,
    date_start: str,
    date_end: str,
    granularity: str,
    from_part: int = 1,
) -> None:
    """
    Parameters
    ----------
    token : str
    report_id : str
    map_version : str
    region : str
    date_start : str
    date_end : str
    granularity : str
    from_part : int, optional
        skip downloads before this part number (1-indexed), by default 1
    """
    month_dirpath = _date_start_to_month_dirpath(map_version, date_start)
    os.makedirs(month_dirpath, exist_ok=True)
    r = requests.get(
        f"https://roadway-analytics-api.inrix.com/v1/data-downloader/{report_id}",
        headers=_authorization_headers(token),
    )
    _print_json(r.json())
    urls = r.json()["urls"]
    for part, url in enumerate(urls[(from_part - 1) :], start=from_part):
        print(f"Downloading part {part} of {len(urls)}...")
        filepath = os.path.join(
            month_dirpath,
            data_report_filename(
                region, date_start, _add_one_day(date_end), granularity, part
            ),
        )
        _download_file(filepath, url)
    return
