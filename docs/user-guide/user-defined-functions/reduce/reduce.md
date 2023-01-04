# Reduce UDF

Reduce is one of the most commonly used abstractions in a stream processing pipeline 
to define aggregation functions on a stream of data. It is the reduce feature
that helps us solve problems like "performs a summary operation (such as 
counting the number of occurrence of a key, yielding user login frequencies), etc."
Since the input an unbounded stream (with infinite entries), we need an
additional parameter to convert the unbounded problem to a bounded problem
and provide results on that. That bounding condition is "time", eg, "number
of users logged in per minute". 
So while processing an unbounded stream of data, we need a way to group elements 
into finite chunks using time. To build these chunks the reduce function is applied to the 
set of records produced using the concept of [windowing](./windowing/windowing.md).

