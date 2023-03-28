import itertools
import multiprocessing
import os
from typing import Any, Iterable

import cacholote
import structlog
import typer
import xarray as xr
from c3s_eqc_automatic_quality_control import download, utils  # type: ignore[import]

LOGGER = structlog.get_logger()


def batched(iterable: Iterable[Any], n: int) -> Iterable[tuple[Any, ...]]:
    """Batch data into tuples of length n. The last batch may be shorter."""
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def retrieve(nominal_days: tuple[str, ...]) -> list[xr.Dataset]:
    kwargs: dict[str, Any] = {
        "collection_id": "satellite-lai-fapar",
        "requests": {
            "satellite": "proba",
            "sensor": "vgt",
            "horizontal_resolution": "1km",
            "product_version": "V2",
            "year": ["2014"],
            "month": [f"{month:02d}" for month in range(1, 12 + 1)],
            "nominal_day": nominal_days,
            "format": "zip",
            "area": [72, -13, 40, 35],
        },
        "transform_func": utils.regionalise,
        "transform_func_kwargs": {
            "lon_slice": slice(-13, 35),
            "lat_slice": slice(72, 40),
        },
        "chunks": {"year": 1, "nominal_day": 1, "variable": 1},
    }
    datasets = []
    for variable in ["fapar", "lai"]:
        kwargs["requests"]["variable"] = [variable]
        LOGGER.info("Retrieving", **kwargs)
        ds: xr.Dataset = download.download_and_transform(**kwargs)
        datasets.append(ds)
    return datasets


def main(
    nominal_day: list[int] = [3, 13, 21, 23, 24],
    processes: int = 5,
    cdsapirc: str = "",
    tag: str = "raw_data",
) -> None:
    if cdsapirc:
        os.environ["CDSAPI_RC"] = os.path.expanduser(cdsapirc)

    nominal_days = tuple(f"{day:02d}" for day in nominal_day)
    batched_nominal_days = list(batched(nominal_days, processes // len(nominal_days)))
    with multiprocessing.Pool(processes) as pool:
        pool.map(retrieve, batched_nominal_days)

    with cacholote.config.set(tag=tag):
        datasets = retrieve(nominal_days)
    LOGGER.info("Sanity check", dims=[dict(ds.dims) for ds in datasets])


if __name__ == "__main__":
    typer.run(main)
