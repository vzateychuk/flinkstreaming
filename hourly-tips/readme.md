# Lab: Windowed Analytics (Hourly Tips)

The task of the "Hourly Tips" exercise is to identify, for each hour, the driver earning the most tips. It's easiest to approach this in two steps: first use hour-long windows that compute the total tips for each driver during the hour, and then from that stream of window results, find the driver with the maximum tip total for each hour.

Please note that the program should operate in event time.

### Input Data

The input data of this exercise is a stream of `TaxiFare` events generated by the [Taxi Fare Stream Generator](../README.md).

The `TaxiFareGenerator` annotates the generated `DataStream<TaxiFare>` with timestamps and watermarks. Hence, there is no need to provide a custom timestamp and watermark assigner in order to correctly use event time.

### Expected Output

The result of this exercise is a data stream of `Tuple3<Long, Long, Float>` records, one for each hour. Each hourly record should contain the timestamp at the end of the hour, the driverId of the driver earning the most in tips during that hour, and the actual total of their tips.

The resulting stream should be printed to standard out.

[Lab: Windowed Analytics (Hourly Tips) on GitHub    ](https://github.com/apache/flink-training/tree/release-1.16/hourly-tips)