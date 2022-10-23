import calendar
import itertools
from typing import Any, Callable, Dict, List, Optional

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
    return pd.Period(f"{date.year}-{date.month}")


def ceil_to_month(period: pd.Period, month: int = 1) -> pd.Period:

    if period.month > month:
        period = pd.Period(year=period.year + 1, month=month, freq="M")
    if period.month < month:
        period = pd.Period(year=period.year, month=month, freq="M")
    return period


def floor_to_month(period: pd.Period, month: int = 1) -> pd.Period:

    if period.month > month:
        period = pd.Period(year=period.year, month=month, freq="M")
    if period.month < month:
        period = pd.Period(year=period.year - 1, month=month, freq="M")

    return period


def extract_leading_months(
    start: pd.Period, stop: pd.Period
) -> List[Dict[str, List[int] | int]]:

    time_ranges = []
    if start.month > 1 and (start.year < stop.year or stop.month == 12):
        stop = min(stop, pd.Period(year=start.year, month=12, freq="M"))
        months = list(range(start.month, stop.month + 1))
        if len(months) > 0:
            time_ranges = [
                {
                    "year": start.year,
                    "month": months,
                    "day": list(range(1, 31 + 1)),
                }
            ]

    return time_ranges


def extract_trailing_months(
    start: pd.Period, stop: pd.Period
) -> List[Dict[str, List[int] | int]]:

    time_ranges = []
    if not stop.month == 12:
        start = max(start, floor_to_month(stop, month=1))
        months = list(range(start.month, stop.month + 1))
        if len(months) > 0:
            time_ranges = [
                {
                    "year": start.year,
                    "month": months,
                    "day": list(range(1, 31 + 1)),
                }
            ]

    return time_ranges


def extract_years(
    start: pd.Timestamp, stop: pd.Timestamp
) -> List[Dict[str, List[int]]]:

    start = ceil_to_month(start, month=1)
    stop = floor_to_month(stop, month=12)
    years = list(range(start.year, stop.year + 1))
    time_ranges = []
    if len(years) > 0:
        time_ranges = [
            {
                "year": years,
                "month": list(range(1, 12 + 1)),
                "day": list(range(1, 31 + 1)),
            }
        ]
    return time_ranges


def compute_request_date(
    start: pd.Period,
    stop: Optional[pd.Period] = None,
    switch_month_day: Optional[int] = None,
) -> List[Dict[str, List[int] | int]]:
    if not stop:
        stop = compute_stop_date(switch_month_day)

    time_range = (
        extract_leading_months(start, stop)
        + extract_years(start, stop)
        + extract_trailing_months(start, stop)
    )
    return time_range  # type: ignore


def update_request_date(
    request: Dict[str, Any],
    start: str | pd.Period,
    stop: Optional[str | pd.Period] = None,
    switch_month_day: Optional[int] = None,
) -> Dict[str, Any] | List[Dict[str, Any]]:
    """
    Return the requests defined by 'request' for the period defined by start and stop.

    Parameters
    ----------
    request: dict
        Parameters of the request

    start: str or pd.Period
        String {start_year}-{start_month} pd.Period with freq='M'

    stop: str or pd.Period
        Optional string {stop_year}-{stop_month} pd.Period with freq='M'

        If None the stop date is computed using the 'switch_month_day'

    switch_month_day: int
        Used to compute the stop date in case stop is None. The stop date is computed as follows:
        if current day > switch_month_day then stop_month = current_month - 1
        else stop_month = current_month - 2

    Returns
    -------
    xr.Dataset: request or list of requests updated
    """
    start = pd.Period(start, "M")
    if stop is None:
        stop = compute_stop_date(switch_month_day=switch_month_day)
    else:
        stop = pd.Period(stop, "M")

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
