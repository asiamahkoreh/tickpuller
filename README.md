# Tick Data Puller

Built by **Asiamah Koreh** for **Birim Capital**.

A small local web app for pulling historical FX tick data (via the [`duka`](https://pypi.org/project/duka/)
library / Dukascopy feed) for backtesting. Runs on your own machine — Flask
just serves a page in your browser so you don't need the command line for
day-to-day pulls.

## Setup

```bash
pip install flask duka
```

## Run

```bash
python app.py
```

Then open your browser to:

```
http://127.0.0.1:5000
```

## How it works

- Pick a symbol, a start date, and an end date (can be years apart).
- The app splits the range into monthly chunks and downloads each chunk
  in the background, one at a time, so it doesn't hammer the data provider.
- Progress and any errors show live on the page.
- Finished files land in `tickdata/<SYMBOL>/`, one CSV per month, plus a
  combined CSV once all chunks finish — that combined file is what you'd
  load into a backtester.

## Known issue this repo patches around

The `duka` package (as published on PyPI at the time of writing) has a bug
in `duka/core/utils.py`'s `is_dst()` function: it compares a `datetime`
against a `date`, which raises `TypeError` for any date close to "today."
If you hit `'>=' not supported between instances of 'datetime.datetime' and
'datetime.date'`, open your installed copy of that file and replace:

```python
def is_dst(day):
    return day >= find_dst_begin(day.year) and day < find_dst_end(day.year)
```

with:

```python
def is_dst(day):
    day_only = day.date() if hasattr(day, 'date') else day
    return day_only >= find_dst_begin(day.year) and day_only < find_dst_end(day.year)
```

## Notes

- Multi-year tick data for major pairs can get large (tens of GB
  depending on symbol/range) — keep an eye on disk space.
- Start big pulls with `threads=1` or `2`; higher thread counts are faster
  but more likely to trip rate limits on large multi-year jobs.
