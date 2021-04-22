package join;

import common.datatypes.TaxiFare;
import common.datatypes.TaxiRide;
import common.sources.TaxiFareGenerator;
import common.sources.TaxiRideGenerator;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TaxiRideFareJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideGenerator());
        DataStream<TaxiFare> fares = env.addSource(new TaxiFareGenerator());
        rides.join(fares)
                .where(x -> x.rideId)
                .equalTo(x -> x.rideId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new TaxiJoinFunction())
                .print();

        env.execute();
    }

    public static class TaxiJoinFunction implements JoinFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
        @Override
        public Tuple2<TaxiRide, TaxiFare> join(TaxiRide ride, TaxiFare fare) {
            return Tuple2.of(ride, fare);
        }
    }
}
