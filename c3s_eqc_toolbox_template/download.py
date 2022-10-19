import calendar
import itertools
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Union

import cacholote
import cads_toolbox
import xarray as xr


def ensure_list(obj: Any) -> list:
    if isinstance(obj, Iterable) and not isinstance(obj, str):
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
    request: Dict[str, Any], parameters: List[str], values: List[Any]
) -> Dict[str, Any]:
    for parameter, value in zip(parameters, values):
        request[parameter] = value
    return request


def split_request_nested(
    request: Dict[str, Any],
    parameters: Optional[List[str]] = [],
):
    parameters = parameters.copy()
    if len(parameters) == 0:
        return request.copy()
    else:
        requests = []
        parameter = parameters.pop(0)
        if isinstance(parameter, Sequence) and not isinstance(parameter, str):
            list_values = list(itertools.product(*[request[p] for p in parameter]))
            for values in list_values:
                out_request = request.copy()
                out_request = update_request(out_request, parameter, values)
                if not check_non_empty(out_request):
                    continue
                out_request = split_request_nested(out_request, parameters)
                requests.append(out_request)
        else:
            for value in request[parameter]:
                out_request = request.copy()
                out_request[parameter] = value
                if not check_non_empty(out_request):
                    continue
                out_request = split_request_nested(out_request, parameters)
                requests.append(out_request)

        return requests


def build_chunks(
    values: Union[List[Union[int, str]], Union[int, str]],
    chunks_size: int,
) -> List[Union[int, str]]:

    values = ensure_list(values)
    values.copy()
    if chunks_size == 1:
        return values
    else:
        chunks_list = []
        for k, value in enumerate(values):
            if k % chunks_size == 0:
                chunks_list.append([])
            chunks_list[-1].append(value)
        return chunks_list


def split_request(
    request: Dict[str, Any],
    chunks: Optional[Dict[str, int]] = [],
):
    requests = []
    if len(chunks) == 0:
        return [request.copy()]
    else:
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


def chunked_download(
    collection_id: str,
    request: Dict[str, Any],
    target: str = None,
    chunks: Optional[Dict[str, int]] = [],
    f: Optional[Callable] = None,
):
    """
    Download chunking along the selected parameters, apply the function f to each chunk and merge the results.

    Parameters
    ----------
    collection_id: str
        ID of the dataset.
    request: dict
        Parameters of the request.
    chunks: dict
        dictionary {parameter_name: chunk_size}
    f: callable
        function to apply to each single chunk
    target: str
        output file path
    Returns
    -------
    xr.Dataset: Resulting dataset.
    """
    requests = split_request(request, chunks)
    datasets = []
    for request_chunk in requests:
        remote = cads_toolbox.catalogue.retrieve(collection_id, request_chunk)
        ds = remote.to_xarray()
        if f is not None:
            ds = cacholote.cacheable(f)(ds)
        datasets.append(ds)
    ds = xr.merge(datasets)
    if target:
        ds.to_netcdf(target)
    return ds
