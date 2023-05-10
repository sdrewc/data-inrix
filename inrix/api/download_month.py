import argparse
import time
from calendar import monthrange
from datetime import date

from inrix.api import (
    API_LOGIN_TOML_FILEPATH,
    download_data,
    get_token,
    load_toml,
    report_completed,
    request_data,
)


def _calculate_date_range(year, month) -> tuple[str, str]:
    date_start = date(year, month, 1)
    date_end = date(year, month, monthrange(year, month)[1])
    date_format = "%Y-%m-%d"
    return (date_start.strftime(date_format), date_end.strftime(date_format))


def download_month(api_login_toml_filepath, data_report_toml_filepath):
    login_config = load_toml(api_login_toml_filepath)
    email = login_config["email"]
    password = login_config["password"]

    data_report_config = load_toml(data_report_toml_filepath)
    map_version = data_report_config["map_version"]
    region = data_report_config["region"]
    date_start, date_end = _calculate_date_range(
        data_report_config["year"], data_report_config["month"]
    )
    granularity = data_report_config["granularity"]

    token = get_token(email, password)
    report_id = request_data(
        token, map_version, region, granularity, date_start, date_end, email
    )
    while not report_completed(token, report_id):
        time.sleep(60)
    download_data(
        token,
        report_id,
        map_version,
        region,
        date_start,
        date_end,
        granularity,
    )
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_report_toml_filepath")
    args = parser.parse_args()
    download_month(API_LOGIN_TOML_FILEPATH, args.data_report_toml_filepath)
