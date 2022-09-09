// Package partition contains partitioning logic. A partition identifies a set of elements with a common key and
// are bucketed in to a common window. A partition is uniquely identified using a tuple {window, key}. Type of window
// does not matter.
// partitioner is responsible for managing the persistence and processing of each partition.
// It uses PBQ for durable persistence of elements that belong to a partition and orchestrates the processing of
// elements using ProcessAndForward function.
// partitioner tracks active partitions, closes the partitions based on watermark progression and co-ordinates the
// materialization and forwarding the results to the next vertex in the pipeline.
package partition
