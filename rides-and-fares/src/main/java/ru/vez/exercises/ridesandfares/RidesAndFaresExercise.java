package ru.vez.exercises.ridesandfares;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import ru.vez.common.datatypes.RideAndFare;
import ru.vez.common.datatypes.TaxiFare;
import ru.vez.common.datatypes.TaxiRide;
import ru.vez.common.sources.TaxiFareGenerator;
import ru.vez.common.sources.TaxiRideGenerator;

/**
 * The Stateful Enrichment exercise from the Flink training.
 *
 * <p>The goal for this exercise is to enrich TaxiRides with fare information.
 */
public class RidesAndFaresExercise {


    private final SourceFunction<TaxiRide> rideSource;
    private final SourceFunction<TaxiFare> fareSource;
    private final SinkFunction<RideAndFare> sink;

    /** Creates a job using the sources and sink provided. */
    public RidesAndFaresExercise(
            SourceFunction<TaxiRide> rideSource,
            SourceFunction<TaxiFare> fareSource,
            SinkFunction<RideAndFare> sink) {

        this.rideSource = rideSource;
        this.fareSource = fareSource;
        this.sink = sink;
    }

    /**
     * Creates and executes the pipeline using the StreamExecutionEnvironment provided.
     *
     * @throws Exception which occurs during job execution.
     * @return {JobExecutionResult}
     */
    public JobExecutionResult execute() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // A stream of taxi ride START events, keyed by rideId.
        DataStream<TaxiRide> rides = env.addSource(rideSource)
                                        .filter(ride -> ride.isStart)
                                        .keyBy(ride -> ride.rideId);

        // A stream of taxi fare events, also keyed by rideId.
        DataStream<TaxiFare> fares = env.addSource(fareSource)
                                        .keyBy(fare -> fare.rideId);

        // Create the pipeline.
        rides.connect(fares).flatMap(new EnrichmentFunction()).addSink(sink);

        // Execute the pipeline and return the result.
        return env.execute("Join Rides with Fares");
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        RidesAndFaresExercise job =
                new RidesAndFaresExercise(
                        new TaxiRideGenerator(),
                        new TaxiFareGenerator(),
                        new PrintSinkFunction<>());

        job.execute();
    }

    public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, RideAndFare> {

        /**
         * The ValueState handle. The first field is the TaxiRide, the second field a TaxiFare.
         */
        private transient ValueState<Tuple2<TaxiRide, TaxiFare>> rideAndFareValueState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple2<TaxiRide, TaxiFare>> descriptor = new ValueStateDescriptor<>(
                    "rideAndFare",         // the state name
                    TypeInformation.of(new TypeHint<>() {}), // type information
                    Tuple2.of(null, null)       // default value of the state, if nothing was set
            ) ;
            this.rideAndFareValueState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<RideAndFare> out) throws Exception {
            // access the state value
            Tuple2<TaxiRide, TaxiFare> current = this.rideAndFareValueState.value();

            // update the TaxiRide
            current.f0 = ride;

            // if the state contains both ride and fare, emit the RideAndFare and clear the state
            if (current.f0 != null && current.f1 != null) {
                out.collect(new RideAndFare(current.f0, current.f1));
                this.rideAndFareValueState.clear();
            } else {
                this.rideAndFareValueState.update(current);
            }
        }


        @Override
        public void flatMap2(TaxiFare fare, Collector<RideAndFare> out) throws Exception {
            // access the state value
            Tuple2<TaxiRide, TaxiFare> current = this.rideAndFareValueState.value();

            // update the TaxiRide state value
            current.f1 = fare;

            // if the state contains both ride and fare, emit the RideAndFare and clear the state
            if (current.f0 != null && current.f1 != null) {
                out.collect(new RideAndFare(current.f0, current.f1));
                this.rideAndFareValueState.clear();
            } else {
                this.rideAndFareValueState.update(current);
            }
        }
    }
}
