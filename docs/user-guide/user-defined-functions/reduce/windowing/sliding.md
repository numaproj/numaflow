# Sliding

Sliding window is similar to a Fixed windows, the size of the windows is measured in time and is fixed.
The important difference from the Fixed window is the fact that it allows an element to be present in
more than one window. The additional window slide parameter controls how frequently a sliding window
is started. Hence, sliding windows will be overlapping and the slide should be smaller than the window
length.

![plot](../../../../assets/sliding.png)

```yaml
window:
  sliding:
    length: duration
    slide: duration
```

NOTE: A duration string is a possibly signed sequence of decimal numbers, each with optional fraction
and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".

## Length

The `length` is the window size of the fixed window.

## Slide

`slide` is the slide parameter that controls the frequency at which the sliding window is created.

## Example

To create a sliding window of length 1-minute which slides every 10-seconds, we can use the following
snippet.

```yaml
window:
  sliding:
    length: 60s
    slide: 30s
```

