package common.datatypes;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.lang.Object;

public class RateCount { // POJO
    public RateCount() {};

    public RateCount(Integer id, Integer count) {
        this.id = id;
        this.count = count;
    }

    public Integer id;
    public Integer count;

    /*
    Must override the hashCode() implementation in order for Rate to be a key.
    See KeyBy in https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/.
     */
    @Override
    public int hashCode() {return (int) this.id.hashCode();}

    @Override
    public boolean equals(Object other) {
        return other instanceof RateCount && this.id.equals(((RateCount) other).id) && this.count.equals(((RateCount) other).count);
    }

    @Override
    public String toString() {
        return new Tuple2<>(this.id, this.count).toString();
    }
}
