package common.datatypes;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.lang.Object;

public class EnrichedRate extends Rate { // POJO
    public EnrichedRate() {};

    public EnrichedRate(Rate rate) {
        this.id = rate.id;
        this.timestamp = rate.timestamp;
        this.flag = rate.id % 2;
    }

    public Integer flag;

    @Override
    public boolean equals(Object other) {
        return other instanceof EnrichedRate && this.id.equals(((Rate) other).id) && this.timestamp.equals(((EnrichedRate) other).timestamp);
    }
}
