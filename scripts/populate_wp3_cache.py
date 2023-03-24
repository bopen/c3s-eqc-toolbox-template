import itertools
import multiprocessing
import os
from typing import Any, Iterable

import cacholote
import structlog
import typer
import xarray as xr
from c3s_eqc_automatic_quality_control import download  # type: ignore[import]

LOGGER = structlog.get_logger()


def batched(iterable: Iterable[Any], n: int) -> Iterable[tuple[Any, ...]]:
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def retrieve(years: tuple[str, ...]) -> xr.Dataset:
    kwargs = {
        "collection_id": "seasonal-monthly-single-levels",
        "requests": {
            "originating_centre": "cmcc",
            "system": "35",
            "variable": "2m_temperature",
            "product_type": "monthly_mean",
            "year": years,
            "month": [f"{month:02d}" for month in range(1, 12 + 1)],
            "leadtime_month": ["1"],
            "format": "grib",
        },
        "chunks": {"year": 1, "leadtime_month": 1},
        "backend_kwargs": {"time_dims": ("forecastMonth", "time")},
        "parallel": True,
    }
    LOGGER.info("Retrieving", **kwargs)
    ds: xr.Dataset = download.download_and_transform(**kwargs)
    return ds


def main(
    year_start: int = 1993,
    year_stop: int = 2016,
    processes: int = 5,
    cdsapirc: str = "",
    tag: str = "raw_data",
) -> None:
    if cdsapirc:
        os.environ["CDSAPI_RC"] = os.path.expanduser(cdsapirc)

    years = tuple(str(year) for year in range(year_start, year_stop + 1))
    batched_years = list(batched(years, processes // len(years)))
    with multiprocessing.Pool(processes) as pool:
        pool.map(retrieve, batched_years)

    with cacholote.config.set(tag=tag):
        ds = retrieve(years)
    LOGGER.info("Sanity check", dims=dict(ds.dims))


if __name__ == "__main__":
    typer.run(main)
