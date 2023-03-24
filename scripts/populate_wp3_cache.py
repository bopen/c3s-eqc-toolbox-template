import itertools
import multiprocessing
import os
import pprint
from typing import Any, Iterable

import cacholote
import typer
from c3s_eqc_automatic_quality_control import download  # type: ignore[import]


def batched(iterable: Iterable[Any], n: int) -> Iterable[tuple[Any, ...]]:
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def retrieve(years: tuple[str, ...]) -> dict[str, Any]:
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
        "backend_kwargs": {"time_dims": ("forecastMonth", "time")},
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
    tag: str = "raw_data",
) -> None:
    if cdsapirc:
        os.environ["CDSAPI_RC"] = os.path.expanduser(cdsapirc)

    years = tuple(str(year) for year in range(year_start, year_stop + 1))
    batched_years = list(batched(years, processes // len(years)))
    with multiprocessing.Pool(processes) as pool:
        requests = pool.map(retrieve, batched_years)
    typer.echo("\n".join(map(repr, requests)))

    typer.echo("Running sanity check.")
    with cacholote.config.set(tag=tag):
        request = retrieve(years)
    typer.echo(pprint.pformat(request))


if __name__ == "__main__":
    typer.run(main)
