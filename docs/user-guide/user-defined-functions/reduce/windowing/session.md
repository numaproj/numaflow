# Session

## Overview

Session window is a type of Unaligned window where the window’s end time keeps moving until there is no data for a 
given time duration.

![plot](../../../../assets/session.png)

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        window:
          session:
            timeout: duration
```

NOTE: A duration string is a possibly signed sequence of decimal numbers, each with optional fraction
and a unit suffix, such as "300ms", "1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

### timeout

The `timeout` is the duration of inactivity (no data flowing in for the particular key) after which the session is
considered to be closed.

## Example

To create a sliding window of length 1 minute which slides every 10 seconds, we can use the following snippet.

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        window:
          session:
            timeout: 60s
```

The yaml snippet above contains an example spec of a _reduce_ vertex that uses session window aggregation. As we can see,
the timeout of the window is 60s. This means we no data arrives for a particular key for 60 seconds, we will mark
it as clised.

Let's say, `time.now()` in the pipeline is `2031-09-29T18:46:30Z` the active window boundaries will be as follows (there
are total of 6 windows `60s/10s`)

```text
[2031-09-29T18:45:40Z, 2031-09-29T18:46:40Z)
[2031-09-29T18:45:50Z, 2031-09-29T18:46:50Z) # notice the 10 sec shift from the above window
[2031-09-29T18:46:00Z, 2031-09-29T18:47:00Z)
[2031-09-29T18:46:10Z, 2031-09-29T18:47:10Z)
[2031-09-29T18:46:20Z, 2031-09-29T18:47:20Z)
[2031-09-29T18:46:30Z, 2031-09-29T18:47:30Z)
```

The window start time is always be left inclusive and right exclusive. That is why `[2031-09-29T18:45:30Z, 2031-09-29T18:46:30Z)`
window is not considered active (it fell on the previous window, right exclusive) but `[2031-09-29T18:46:30Z, 2031-09-29T18:47:30Z)`
is an active (left inclusive).

The first window always ends after the sliding seconds from the `time.Now()`, the start time of the window will be the
nearest integer multiple of the slide which is less than the message's event time. So the first window starts in the
past and ends _sliding_duration (based on time progression in the pipeline and not the wall time) from present. It is
important to note that regardless of the window boundary (starting in the past or ending in the future) the target element
set totally depends on the matching time (in case of event time, all the elements with the time that falls with in the
boundaries of the window, and in case of system time, all the elements that arrive from the `present` until the end of
window `present + sliding`)

From the point above, it follows then that immediately upon startup, for the first window, fewer elements may get
aggregated depending on the current _lateness_ of the data stream.





