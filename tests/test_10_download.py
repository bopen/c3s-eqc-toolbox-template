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


def test_floor_to_month() -> None:

    date = pd.Period("2022-12", freq="M")
    res = download.floor_to_month(date, 1)

    assert res == pd.Period("2022-01", freq="M")

    date = pd.Period("2022-01", freq="M")
    res = download.floor_to_month(date, month=1)

    assert res == date

    date = pd.Period("2022-12", freq="M")
    res = download.floor_to_month(date, month=12)

    assert res == date

    date = pd.Period("2022-01", freq="M")
    res = download.floor_to_month(date, month=12)

    assert res == pd.Period("2021-12", freq="M")


def test_extract_leading_months() -> None:

    start = pd.Period("2020-06", freq="M")
    stop = pd.Period("2023-06", freq="M")
    res = download.extract_leading_months(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == 2020
    assert res[0]["month"] == [6, 7, 8, 9, 10, 11, 12]

    start = pd.Period("2020-06", freq="M")
    stop = pd.Period("2023-12", freq="M")
    res = download.extract_leading_months(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == 2020
    assert res[0]["month"] == [6, 7, 8, 9, 10, 11, 12]

    # special case: if 'start' is the start of the year, then there are no leading months
    start = pd.Period("2020-01", freq="M")
    stop = pd.Period("2023-06", freq="M")
    res = download.extract_leading_months(start, stop)

    assert len(res) == 0

    # special cases: if start.year == stop.year, then the months are trailing months when possible.

    start = pd.Period("2020-06", freq="M")
    stop = pd.Period("2020-10", freq="M")
    res = download.extract_leading_months(start, stop)

    assert len(res) == 0

    start = pd.Period("2020-06", freq="M")
    stop = pd.Period("2020-12", freq="M")
    res = download.extract_leading_months(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == 2020
    assert res[0]["month"] == [6, 7, 8, 9, 10, 11, 12]


def test_extract_trailing_months() -> None:

    start = pd.Period("2020-06", freq="M")
    stop = pd.Period("2023-06", freq="M")
    res = download.extract_trailing_months(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == 2023
    assert res[0]["month"] == [1, 2, 3, 4, 5, 6]

    start = pd.Period("2020-01", freq="M")
    stop = pd.Period("2023-06", freq="M")
    res = download.extract_trailing_months(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == 2023
    assert res[0]["month"] == [1, 2, 3, 4, 5, 6]

    # special case: if 'stop' is the end of the year, then there are no trailing months
    start = pd.Period("2020-01", freq="M")
    stop = pd.Period("2023-12", freq="M")
    res = download.extract_trailing_months(start, stop)

    assert len(res) == 0

    # special cases: if start.year == stop.year, then the months are trailing months when possible.
    start = pd.Period("2020-06", freq="M")
    stop = pd.Period("2020-10", freq="M")
    res = download.extract_trailing_months(start, stop)

    assert len(res) == 1
    assert res[0]["month"] == [6, 7, 8, 9, 10]

    start = pd.Period("2020-06", freq="M")
    stop = pd.Period("2020-12", freq="M")
    res = download.extract_trailing_months(start, stop)

    assert len(res) == 0


def test_extract_years() -> None:

    start = pd.Period("2020-06", freq="M")
    stop = pd.Period("2023-06", freq="M")
    res = download.extract_years(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == [2021, 2022]

    start = pd.Period("2020-01", freq="M")
    stop = pd.Period("2020-12", freq="M")
    res = download.extract_years(start, stop)

    assert len(res) == 1
    assert res[0]["year"] == [2020]

    start = pd.Period("2020-02", freq="M")
    stop = pd.Period("2020-12", freq="M")
    res = download.extract_years(start, stop)

    assert len(res) == 0

    start = pd.Period("2020-01", freq="M")
    stop = pd.Period("2020-11", freq="M")
    res = download.extract_years(start, stop)

    assert len(res) == 0


def test_update_request() -> None:

    requests = download.update_request_date({}, "2020-02", "2020-11")
    assert len(requests) == 1

    requests = download.update_request_date({}, "2020-01", "2020-12")
    assert len(requests) == 1

    requests = download.update_request_date({}, "2020-01", "2022-12")
    assert len(requests) == 1

    requests = download.update_request_date({}, "2020-02", "2022-12")
    assert len(requests) == 2

    requests = download.update_request_date({}, "2020-01", "2022-11")
    assert len(requests) == 2

    requests = download.update_request_date({}, "2020-02", "2022-11")
    assert len(requests) == 3
