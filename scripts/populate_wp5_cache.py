import itertools
import multiprocessing
import os
from typing import Any, Iterable

import cacholote
import typer
from c3s_eqc_automatic_quality_control import download  # type: ignore[import]


def batched(iterable: Iterable[Any], n: int) -> Iterable[Any]:
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def retrieve(years: list[str]) -> dict[str, Any]:
    collection_id = "seasonal-monthly-single-levels"
    request = {
        "originating_centre": "cmcc",
        "system": "35",
        "variable": "2m_temperature",
        "product_type": "monthly_mean",
        "year": years,
        "month": [f"{month:02d}" for month in range(1, 12 + 1)],
        "leadtime_month": ["1"],
        "format": "grib",
    }
    xr_open_mfdataset_kwargs = {
        "concat_dim": "forecast_reference_time",
        "combine": "nested",
        "parallel": True,
    }
    download.download_and_transform(
        collection_id,
        request,
        chunks={"year": 1, "leadtime_month": 1},
        **xr_open_mfdataset_kwargs,
    )
    return request


def main(
    year_start: int = 1993,
    year_stop: int = 2016,
    processes: int = 5,
    cdsapirc: str = "",
) -> None:
    if cdsapirc:
        os.environ["CDSAPI_RC"] = os.path.expanduser(cdsapirc)

    years = [str(year) for year in range(year_start, year_stop + 1)]
    batched_years = list(batched(years, processes))
    with multiprocessing.Pool(processes) as pool:
        print(pool.map(retrieve, batched_years))

    # Sanity check and tagging
    with cacholote.config.set(tag="raw_data"):
        print(retrieve(years))


if __name__ == "__main__":
    typer.run(main)
