package common.datatypes;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.lang.Object;

public class Rate { // POJO
    public Rate() {};

    public Rate(Integer id, Long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    public Integer id;
    public Long timestamp;

    /*
    Must override the hashCode() implementation in order for Rate to be a key.
    See KeyBy in https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/.
     */
    @Override
    public int hashCode() {return (int) this.id.hashCode();}

    @Override
    public boolean equals(Object other) {
        return other instanceof Rate && this.id.equals(((Rate) other).id) && this.timestamp.equals(((Rate) other).timestamp);
    }

    @Override
    public String toString() {
        return new Tuple2<>(this.id, this.timestamp).toString();
    }
}
