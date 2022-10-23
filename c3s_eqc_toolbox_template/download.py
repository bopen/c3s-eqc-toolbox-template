import calendar
import itertools
from typing import Any, Callable, Dict, List, Optional, Tuple

import cacholote
import cads_toolbox
import pandas as pd
import xarray as xr


def compute_stop_date(switch_month_day: Optional[int] = None) -> pd.Timestamp:
    today = pd.Timestamp.today()
    if switch_month_day is None:
        switch_month_day = 9

    if today.day >= switch_month_day:
        date = today - pd.DateOffset(months=1)
    else:
        date = today - pd.DateOffset(months=2)
    return pd.Timestamp(date.year, date.month, date.days_in_month)


def floor_to_month_end(date: pd.Timestamp) -> pd.Timestamp:
    if not date.is_month_end:
        date = date + pd.offsets.MonthEnd(0) - pd.DateOffset(months=1)
    return date


def floor_to_year_start(date: pd.Timestamp) -> pd.Timestamp:
    if not date.is_year_start:
        date = date + pd.offsets.YearBegin(0) - pd.DateOffset(years=1)
    return date


def floor_to_year_end(date: pd.Timestamp) -> pd.Timestamp:
    if not date.is_year_end:
        date = date + pd.offsets.YearEnd(0) - pd.DateOffset(years=1)
    return date


def extract_leading_months(
    start: pd.Timestamp, stop: pd.Timestamp
) -> List[Dict[str, List[int] | int]]:
    start = start + pd.offsets.MonthBegin(0)
    stop = floor_to_month_end(stop)

    if (start.year == stop.year) and (not stop.is_year_end):
        return []
    start = start + pd.offsets.MonthBegin(0)
    stop = floor_to_month_end(stop)

    time_ranges = []
    if not start.is_year_start:
        stop = min(stop, start + pd.offsets.YearEnd(0))
        months = list(range(start.month, stop.month + 1))
        if len(months) > 0:
            time_ranges.append(
                {
                    "year": start.year,
                    "month": months,
                    "day": list(range(1, 31 + 1)),
                }
            )
    return time_ranges


def extract_trailing_months(
    start: pd.Timestamp, stop: pd.Timestamp
) -> List[Dict[str, List[int] | int]]:
    start = start + pd.offsets.MonthBegin(0)
    stop = floor_to_month_end(stop)

    time_ranges = []
    if not stop.is_year_end:
        start = max(start, floor_to_year_start(stop))
        months = list(range(start.month, stop.month + 1))
        if len(months) > 0:
            time_ranges.append(
                {
                    "year": start.year,
                    "month": months,
                    "day": list(range(1, 31 + 1)),
                }
            )
    return time_ranges


def extract_years(
    start: pd.Timestamp, stop: pd.Timestamp
) -> List[Dict[str, List[int]]]:
    start = start + pd.offsets.YearBegin(0)
    stop = floor_to_year_end(stop)
    years = list(range(start.year, stop.year + 1))
    time_ranges = []
    if len(years) > 0:
        time_ranges.append(
            {
                "year": years,
                "month": list(range(1, 12 + 1)),
                "day": list(range(1, 31 + 1)),
            }
        )
    return time_ranges


def compute_request_date(
    start: Tuple[int, int],
    stop: Optional[Tuple[int, int]] = None,
    switch_month_day: Optional[int] = None,
) -> List[Dict[str, List[int] | int]]:
    start = pd.Timestamp(*start, 1)
    if not stop:
        stop = compute_stop_date(switch_month_day)
    else:
        stop = pd.Timestamp(*stop, 1) + pd.offsets.MonthEnd(0)

    time_range = (
        extract_leading_months(start, stop)
        + extract_years(start, stop)
        + extract_trailing_months(start, stop)
    )
    return time_range  # type: ignore


def update_request_date(
    request: Dict[str, Any],
    start: Tuple[int, int],
    stop: Optional[Tuple[int, int]] = None,
    switch_month_day: Optional[int] = None,
) -> Dict[str, Any] | List[Dict[str, Any]]:
    """
    Return the requests defined by 'request' for the period defined by start and stop.

    Parameters
    ----------
    request: dict
        Parameters of the request

    start: tuple[int, int]
        Start year, start month

    stop: tuple[int, int]
        Optional stop year, stop month. If None the stop date is computed using the 'switch_month_day'

    switch_month_day: int
        Used to compute the stop date in case stop is None. The stop date is computed as follows:
        if current day > switch_month_day then stop_month = current_month - 1
        else stop_month = current_month - 2

    Returns
    -------
    xr.Dataset: request or list of requests updated
    """
    dates = compute_request_date(start, stop, switch_month_day=switch_month_day)
    if isinstance(dates, dict):
        return {**request, **dates}
    requests = []
    for d in dates:
        requests.append({**request, **d})
    return requests


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
        Dictionary {parameter_name: chunk_size}

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
            for parameter, value in zip(chunks, values):
                out_request[parameter] = value

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
        Function to apply to each single chunk
    open_with: str
        is the backend used for opening the data file, valid values 'xarray', or 'pandas'
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
