# Lab: Stateful Enrichment (Rides and Fares)

The goal of this exercise is to join together the TaxiRide and TaxiFare records for each ride.

For each distinct rideId, there are exactly three events:

1. TaxiRide START event;
2. TaxiRide END event;
3. TaxiFare event (whose timestamp happens to match the start time);

The result should be a DataStream<RideAndFare>, with one record for each distinct rideId. Each tuple should pair the TaxiRide START event for some rideId with its matching TaxiFare.

Input Data
--
For this exercise you will work with two data streams, one with TaxiRide events generated by a TaxiRideSource and the other with TaxiFare events generated by a TaxiFareSource. See Using the Taxi Data Streams for information on how to work with these stream generators.

Expected Output
--
The result of this exercise is a data stream of RideAndFare records, one for each distinct rideId. The exercise is setup to ignore the END events, and you should join the event for the START of each ride with its corresponding fare event.

Once you have both the ride and fare that belong together, you can create the desired object for the output stream by using new RideAndFare(ride, fare)

The stream will be printed to standard out.

[Lab: Stateful Enrichment (Rides and Fares) on Github](https://github.com/apache/flink-training/tree/release-1.16/rides-and-fares)