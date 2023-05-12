# inrix: Scripts for analyzing INRIX IQ Roadway Analytics data
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

## Installation (for developers)
```
mamba create -n inrix --file requirements.txt [-y]
mamba activate inrix
pip install -e path/to/data-inrix
```
`mamba` is faster than `conda`, but either works equally well in the above commands.

## Monthly API download instructions
1. Create a config file specifying what to download (see e.g. `inrix/api/configs/data-report-month.toml`). Save the file to e.g. `Q:\Data\Observed\Streets\INRIX\v2301\2023\03\data_report-month-2023-03.toml` for 2023/03 data under map version `2301`.
2. Download files with `inrix/api/download_month.py path/to/data-report-month-YYYY-MM.toml`. Running this on the servers can be helpful, because the downloads can take some time, and your laptop might lose its VPN / shared drives connection.

## Notes
- The monthly downloads of SF data is done via API starting from map version `2301` (i.e. March 2023). Before that, downloads were done manually and the directory structure may not conform to the standards set in this repo.

## Gotchas
- the end date encoded in the downloaded data (zip filenames and reportContents.json) refer to the next day, not the actual end date, e.g. an actual end date of `2023-03-31` will show up as `2023-04-01` in the filenames and reportContents.json. The API scripts modify the filenames accordingly to conform to this INRIX standard.
