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
