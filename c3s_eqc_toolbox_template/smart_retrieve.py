import calendar
import itertools

from typing import Any, Callable, Dict, Iterable, List


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
        request: Dict[str, Any],
        parameters: List[str],
        values: List[Any]
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
            list_values = list(itertools.product(
                *[request[p] for p in parameter]
            ))
            for values in list_values:
                out_request = request.copy()
                out_request = update_request(out_request, parameter, values)
                if not check_non_empty(out_request):
                    continue
                out_request = nested_split_request(out_request, parameters)
                requests.append(out_request)
        else:
            for value in request[parameter]:
                out_request = request.copy()
                out_request[parameter] = value
                if not check_non_empty(out_request):
                    continue
                out_request = nested_split_request(out_request, parameters)
                requests.append(out_request)

        return requests


def split_request(
        request: Dict[str, Any],
        parameters: Optional[List[str]] = [],
):
    requests = []
    if len(parameters) == 0:
        return request.copy()
    else:
        list_values = list(itertools.product(
            *[request[p] for p in parameters]
        ))
        for values in list_values:
            out_request = request.copy()
            out_request = update_request(out_request, parameters, values)

            if not check_non_empty(out_request):
                continue

            requests.append(out_request)
        return requests


# TODO: change package and function  name
def chunked_download_and_reduce(
    collection_id: str,
    request: Dict[str, Any],
    parameters: Optional[List[str]] = None,
    function: Optional[Callable] = None,
    target: Optional[str] = None,
):
    datasets = []
    for small_request in split_request(request, parameters):
        remote = cads_toolbox.catalogue.retrieve(collection_id, small_request)
        ds = remote.to_xarray()
        datasets.append(function(ds))
    ds = xr.merge(datasets)
    return ds




