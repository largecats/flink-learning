package common.datatypes;

import java.io.Serializable;
import java.lang.Object;

//public class Color implements Serializable { // not POJO
public class Color { // POJO
    public Color() {};

    public Color(String value) {
        this.value = value;
    }

    public String value;

    /*
    Must override the hashCode() implementation in order for Color to be a key.
    See KeyBy in https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/.
     */
    @Override
    public int hashCode() {return (int) this.value.hashCode();}
}
