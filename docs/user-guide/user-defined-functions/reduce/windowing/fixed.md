# Fixed

Fixed windows (sometimes called tumbling windows) are defined by a static window size, e.g. 30 second
windows, one minute windows, etc. They are generally aligned, i.e. every window applies across all
the data for the corresponding period of time. It has a fixed size measured in time and does not
overlap. The element which belongs to one window will not belong to any other tumbling window.
For example, a window size of 20 seconds will include all entities of the stream which came in a
certain 20-second interval.

![plot](../../../../assets/fixed.png)

To enable Fixed window, we use `fixed` under `window` section.

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        window:
          fixed:
            length: duration
```

NOTE: A duration string is a possibly signed sequence of decimal numbers, each with optional fraction
and a unit suffix, such as "300ms", "1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

## Length

The `length` is the window size of the fixed window.

## Example

A 60-second window size can be defined as following.

```yaml
vertices:
  - name: my-udf
    udf:
      groupBy:
        window:
          fixed:
            length: 60s
```
