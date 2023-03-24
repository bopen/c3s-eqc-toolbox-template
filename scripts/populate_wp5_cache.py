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


def retrieve(nominal_days: tuple[str, ...]) -> dict[str, Any]:
    collection_id = "satellite-lai-fapar"
    request = {
        "nominal_day": nominal_days,
        "variable": ["fapar", "lai"],
        "satellite": "proba",
        "sensor": "vgt",
        "horizontal_resolution": "1km",
        "product_version": "V2",
        "year": "2014",
        "month": [f"{month:02d}" for month in range(1, 12 + 1)],
        "format": "zip",
        "area": [90, -180, -90, 180],
    }
    xr_open_mfdataset_kwargs = {"parallel": True}
    download.download_and_transform(
        collection_id,
        request,
        chunks={"nominal_day": 1},
        **xr_open_mfdataset_kwargs,
    )
    return request


def main(
    nominal_day: list[int] = [3, 13, 21, 23, 24],
    processes: int = 5,
    cdsapirc: str = "",
    tag: str = "raw_data",
) -> None:
    if cdsapirc:
        os.environ["CDSAPI_RC"] = os.path.expanduser(cdsapirc)

    nominal_days = tuple(f"{day:02d}" for day in nominal_day)
    batched_nominal_days = list(batched(nominal_days, processes))
    with multiprocessing.Pool(processes) as pool:
        requests = pool.map(retrieve, batched_nominal_days)
    typer.echo("\n".join(map(repr, requests)))

    typer.echo("Running sanity check.")
    with cacholote.config.set(tag=tag):
        request = retrieve(nominal_days)
    typer.echo(pprint.pformat(request))


if __name__ == "__main__":
    typer.run(main)
