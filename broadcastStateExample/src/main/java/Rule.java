import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

public class Rule implements Serializable {

    public Rule() {};

    public Rule(String name, Tuple2<Shape, Shape> pattern) {
        this.name = name;
        this.first = pattern.f0;
        this.second = pattern.f1;
    }

    public String name;
    public Shape first;
    public Shape second;
}
