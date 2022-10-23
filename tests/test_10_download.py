import pandas as pd

from c3s_eqc_toolbox_template import download


def test_split_request() -> None:

    request = {
        "product_type": "reanalysis",
        "format": "grib",
        "variable": "temperature",
        "pressure_level": [
            "1",
            "2",
            "3",
        ],
        "year": [
            "2019",
            "2020",
            "2021",
            "2022",
        ],
        "month": [
            "01",
            "02",
            "03",
            "04",
            "05",
            "06",
            "07",
            "08",
            "09",
            "10",
            "11",
            "12",
        ],
        "day": "01",
        "time": "00:00",
    }

    requests = download.split_request(request, {"month": 1})
    assert len(requests) == 12

    requests = download.split_request(request, {"month": 1, "year": 1})
    assert len(requests) == 4 * 12


def test_build_chunks() -> None:

    values = list(range(11))

    res = download.build_chunks(values, 1)
    assert res == values

    res = download.build_chunks(values, 11)
    assert res == [values]

    res = download.build_chunks(values, 3)
    assert res[-1] == [9, 10]


def test_check_non_empty() -> None:

    request = {
        "year": "2021",
        "month": ["01", "02", "03"],
        "day": ["30", "31"],
    }

    assert download.check_non_empty(request)

    request = {
        "year": "2021",
        "month": "03",
        "day": "30",
    }

    assert download.check_non_empty(request)

    request = {
        "year": "2021",
        "month": ["02", "04"],
        "day": "31",
    }

    assert not download.check_non_empty(request)

    request = {
        "year": "2021",
        "month": "02",
        "day": ["30", "31"],
    }

    assert not download.check_non_empty(request)


def test_floor_to_month_end() -> None:

    date = pd.Timestamp(2022, 1, 31)
    res = download.floor_to_month_end(date)

    assert res == date

    date = pd.Timestamp(2022, 1, 30)
    res = download.floor_to_month_end(date)

    assert res == pd.Timestamp(2021, 12, 31)


def test_floor_to_year_start() -> None:

    date = pd.Timestamp(2022, 12, 31)
    res = download.floor_to_year_start(date)

    assert res == pd.Timestamp(2022, 1, 1)

    date = pd.Timestamp(2022, 1, 1)
    res = download.floor_to_year_start(date)

    assert res == date


def test_floor_to_year_end() -> None:

    date = pd.Timestamp(2022, 12, 31)
    res = download.floor_to_year_end(date)

    assert res == date

    date = pd.Timestamp(2022, 1, 1)
    res = download.floor_to_year_end(date)

    assert res == pd.Timestamp(2021, 12, 31)


def test_extract_leading_months() -> None:

    start = pd.Timestamp(2020, 6, 1)
    stop = pd.Timestamp(2023, 12, 31)
    res = download.extract_leading_months(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == 2020
    assert res[0]["month"] == [6, 7, 8, 9, 10, 11, 12]

    # special case: 'start' is not the start of the month, then the date is rounded to the next month start
    start = pd.Timestamp(2020, 6, 2)
    stop = pd.Timestamp(2023, 12, 31)
    res = download.extract_leading_months(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == 2020
    assert res[0]["month"] == [7, 8, 9, 10, 11, 12]

    # special case: if 'start' is the start of the year, then there are no leading months
    start = pd.Timestamp(2020, 1, 1)
    stop = pd.Timestamp(2023, 6, 1)
    res = download.extract_leading_months(start, stop)

    assert len(res) == 0

    # special cases: if start.year == stop.year, then the months are trailing months when possible.

    start = pd.Timestamp(2020, 6, 1)
    stop = pd.Timestamp(2020, 10, 1)
    res = download.extract_leading_months(start, stop)

    assert len(res) == 0

    start = pd.Timestamp(2020, 6, 1)
    stop = pd.Timestamp(2020, 12, 31)
    res = download.extract_leading_months(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == 2020
    assert res[0]["month"] == [6, 7, 8, 9, 10, 11, 12]


def test_extract_trailing_months() -> None:

    start = pd.Timestamp(2020, 6, 1)
    stop = pd.Timestamp(2023, 6, 30)
    res = download.extract_trailing_months(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == 2023
    assert res[0]["month"] == [1, 2, 3, 4, 5, 6]

    # special case: 'stop' is not the end of the month, then the date is rounded to the previous month end
    start = pd.Timestamp(2020, 6, 2)
    stop = pd.Timestamp(2023, 6, 25)
    res = download.extract_trailing_months(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == 2023
    assert res[0]["month"] == [1, 2, 3, 4, 5]

    # special case: if 'stop' is the end of the year, then there are no trailing months
    start = pd.Timestamp(2020, 1, 1)
    stop = pd.Timestamp(2023, 12, 31)
    res = download.extract_trailing_months(start, stop)

    assert len(res) == 0

    # special cases: if start.year == stop.year, then the months are trailing months when possible.

    start = pd.Timestamp(2020, 6, 1)
    stop = pd.Timestamp(2020, 10, 31)
    res = download.extract_trailing_months(start, stop)

    assert len(res) == 1
    assert res[0]["month"] == [6, 7, 8, 9, 10]

    start = pd.Timestamp(2020, 6, 1)
    stop = pd.Timestamp(2020, 12, 31)
    res = download.extract_trailing_months(start, stop)

    assert len(res) == 0


def test_extract_years() -> None:

    start = pd.Timestamp(2020, 1, 15)
    stop = pd.Timestamp(2020, 12, 31)
    res = download.extract_years(start, stop)

    assert len(res) == 0

    start = pd.Timestamp(2020, 1, 1)
    stop = pd.Timestamp(2020, 12, 15)
    res = download.extract_years(start, stop)

    assert len(res) == 0

    start = pd.Timestamp(2020, 6, 1)
    stop = pd.Timestamp(2023, 6, 1)
    res = download.extract_years(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == [2021, 2022]

    start = pd.Timestamp(2022, 0, 1)
    stop = pd.Timestamp(2022, 12, 31)
    res = download.extract_years(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == 2022
