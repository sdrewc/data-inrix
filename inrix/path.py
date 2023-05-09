import os
from datetime import datetime, timedelta

inrix_dirpath = "Q:\Data\Observed\Streets\INRIX"


def map_version_dirpath(map_version: str) -> str:
    return os.path.join(inrix_dirpath, f"v{map_version}")


def month_dirpath(map_version: str, year: str, month: str) -> str:
    """
    map_version: e.g. "2301"
    year: YYYY, e.g. "2023"
    month: MM, e.g. "09"
    """
    if len(year) != 4:
        raise ValueError("year should be in YYYY format, e.g. '2001'")
    if len(month) != 2:
        raise ValueError("month should be in MM format, e.g. '01'")
    return os.path.join(map_version_dirpath(map_version), year, month)


def _add_one_day(actual_date_end: str) -> str:
    """
    Helper to handle data report (file)names having an end date that is
    one day after the actual end date
    """
    actual_end_datetime = datetime.strptime(actual_date_end, "%Y-%m-%d")
    filename_end_datetime = actual_end_datetime + timedelta(days=1)
    return filename_end_datetime.strftime("%Y-%m-%d")


def data_report_name(
    region: str, date_start: str, date_end: str, granularity: int
) -> str:
    """
    date_end: this should refer to the day after the actual end date,
              e.g. use `2023-04-01` for an actual end date of `2023-03-31`
    """
    return f"{region}_{date_start}_to_{date_end}_{granularity}_min"


def data_report_filename(
    region: str, date_start: str, date_end: str, granularity: int, part: int
) -> str:
    """
    date_end: this should refer to the next day after the actual end date,
              e.g. use `2023-04-01` for an actual end date of `2023-03-31`
    """
    return (
        f"{data_report_name(region, date_start, date_end, granularity)}"
        f"_part_{part}.zip"
    )


def _date_start_to_month_dirpath(map_version: str, date_start: str) -> str:
    date_start_datetime = datetime.strptime(date_start, "%Y-%m-%d")
    year = date_start_datetime.strftime("%Y")
    month = date_start_datetime.strftime("%m")
    return month_dirpath(map_version, year, month)


def data_report_filepath(
    map_version: str,
    region: str,
    date_start: str,
    date_end: str,
    granularity: int,
    part: int,
) -> str:
    """
    the month directory is determined by date_start only
    (date_end is not taken into account when setting the month directory)

    date_end: this should refer to the next day after the actual end date,
              e.g. use `2023-04-01` for an actual end date of `2023-03-31`
    """
    return os.path.join(
        _date_start_to_month_dirpath(map_version, date_start),
        data_report_filename(region, date_start, date_end, granularity, part),
    )
