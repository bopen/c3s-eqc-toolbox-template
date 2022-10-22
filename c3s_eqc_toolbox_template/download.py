import calendar
import itertools
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import cacholote
import cads_toolbox
import pandas as pd
import xarray as xr


def ensure_list(obj: Any) -> List[Any]:

    if isinstance(obj, (list, tuple)):
        return list(obj)
    else:
        return [obj]


def check_non_empty(request: Dict[str, Any]) -> bool:
    non_empty = False

    years = ensure_list(request["year"])
    months = ensure_list(request["month"])
    days = ensure_list(request["day"])
    date = itertools.product(years, months, days)

    for year, month, day in date:
        n_days = calendar.monthrange(int(year), int(month))[1]
        if int(day) <= n_days:
            non_empty = True
            break
    return non_empty


def update_request(
    request: Dict[str, Any],
    parameters: Iterable[str],
    values: List[Any] | Tuple[Any],
) -> Dict[str, Any]:
    for parameter, value in zip(parameters, values):
        request[parameter] = value
    return request


def build_chunks(
    values: List[Any] | Any,
    chunks_size: int,
) -> List[List[Any]] | List[Any]:

    values = ensure_list(values)
    values.copy()
    if chunks_size == 1:
        return values
    else:
        chunks_list: List[List[Any]] = []
        for k, value in enumerate(values):
            if k % chunks_size == 0:
                chunks_list.append([])
            chunks_list[-1].append(value)
        return chunks_list


def split_request(
    request: Dict[str, Any],
    chunks: Dict[str, int] = {},
) -> List[Dict[str, Any]]:
    """
    Split the input request in smaller request defined by the chunks.

    Parameters
    ----------
    request: dict
        Parameters of the request
    chunks: dict
        dictionary {parameter_name: chunk_size}

    Returns
    -------
    xr.Dataset: list of requests
    """
    if len(chunks) == 0:
        requests = [request.copy()]
    else:
        requests = []
        list_values = list(
            itertools.product(
                *[
                    build_chunks(request[par], chunk_size)
                    for par, chunk_size in chunks.items()
                ]
            )
        )
        for values in list_values:
            out_request = request.copy()
            out_request = update_request(out_request, chunks, values)

            if not check_non_empty(out_request):
                continue

            requests.append(out_request)
    return requests


@cacholote.cacheable
def download_and_transform_chunk(
    collection_id: str,
    request: Dict[str, Any],
    f: Optional[Callable[[xr.Dataset], xr.Dataset]] = None,
    open_with: str = "xarray",
) -> xr.Dataset:
    open_with_allowed_values = ("xarray", "pandas")
    if open_with not in ("xarray", "pandas"):
        raise ValueError(
            f"{open_with} is not a valid value, 'open_with' can take on "
            f"one of the following values {open_with_allowed_values}"
        )

    remote = cads_toolbox.catalogue.retrieve(collection_id, request)
    if open_with == "xarray":
        ds = remote.to_xarray()
    elif open_with == "pandas":
        ds = remote.to_pandas()
    if f is not None:
        ds = f(ds)
    return ds  # type: ignore


def download_and_transform(
    collection_id: str,
    requests: List[Dict[str, Any]] | Dict[str, Any],
    chunks: Dict[str, int] = {},
    f: Optional[
        Callable[[xr.Dataset], xr.Dataset] | Callable[[pd.DataFrame], pd.DataFrame]
    ] = None,
    open_with: str = "xarray",
    **kwargs: Any,
) -> xr.Dataset | pd.DataFrame:
    """
    Download chunking along the selected parameters, apply the function f to each chunk and merge the results.

    Parameters
    ----------
    collection_id: str
        ID of the dataset.
    requests: list of dict or dict
        Parameters of the requests
    chunks: dict
        dictionary {parameter_name: chunk_size}
    f: callable
        function to apply to each single chunk
    open_with: str
        open_with indicates the data structure on which the data is loaded when opening:
        'xarray', that is a xarray.Dataset, or 'pandas', that is a pandas.Dataset.
    **kwargs:
        kwargs to be passed on to xr.merge or pd.concat function

    Returns
    -------
    xr.Dataset or pd.DataFrame: Resulting dataset or dataframe.
    """
    request_list = []

    for request in ensure_list(requests):
        request_list.extend(split_request(request, chunks))

    datasets = []
    for request_chunk in request_list:
        ds = download_and_transform_chunk(
            collection_id, request=request_chunk, f=f, open_with=open_with
        )
        datasets.append(ds)
    if open_with == "xarray":
        ds = xr.merge(datasets, **kwargs)
    else:
        ds = pd.concat(datasets, **kwargs)
    return ds
