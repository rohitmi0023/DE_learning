# Time series / date functionality
https://pandas.pydata.org/docs/user_guide/timeseries.html#time-series-date-functionality

## Overview
pandas captures 4 general time related concepts:

1. Date times: A specific date and time with timezone support. Similar to `datetime.datetime` from the standard library.
    
2. Time deltas: An absolute time duration. Similar to `datetime.timedelta` from the standard library.
    
3. Time spans: A span of time defined by a point in time and its associated frequency.
    
4. Date offsets: A relative time duration that respects calendar arithmetic. Similar to `dateutil.relativedelta.relativedelta` from the `dateutil` package.

Q. What is the capital of france?
#flashcard 

| Concept      | Scalar Class | Array Class      | pandas Data Type                         | Primary Creation Method             |
| ------------ | ------------ | ---------------- | ---------------------------------------- | ----------------------------------- |
| Date times   | `Timestamp`  | `DatetimeIndex`  | `datetime64[ns]` or `datetime64[ns, tz]` | `to_datetime` or `date_range`       |
| Time deltas  | `Timedelta`  | `TimedeltaIndex` | `timedelta64[ns]`                        | `to_timedelta` or `timedelta_range` |
| Time spans   | `Period`     | `PeriodIndex`    | `period[freq]`                           | `Period` or `period_range`          |
| Date offsets | `DateOffset` | `None`           | `None`                                   | `DateOffset`                        |

## Timestamps vs. time spans
Timestamp for pandas means using point in time.