# Lab: Filtering a Stream (Ride Cleansing)

The task of the "Taxi Ride Cleansing" exercise is to cleanse a stream of TaxiRide events by removing events that start or end outside of New York City.

The GeoUtils utility class provides a static method isInNYC(float lon, float lat) to check if a location is within the NYC area.

Input Data
--
This exercise is based on a stream of TaxiRide events, as described in Using the Taxi Data Streams.

Expected Output
--
The result of the exercise should be a DataStream<TaxiRide> that only contains events of taxi rides which both start and end in the New York City area as defined by GeoUtils.isInNYC().

The resulting stream should be printed to standard out.

[Lab: Filtering a Stream (Ride Cleansing) on github](https://github.com/apache/flink-training/tree/release-1.16/ride-cleansing)