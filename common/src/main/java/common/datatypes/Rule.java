package common.datatypes;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

public class Rule implements Serializable {

    public Rule() {};

    public Rule(String name, Shape first, Shape second) {
        this.name = name;
        this.first = first;
        this.second = second;
    }

    public String name;
    public Shape first;
    public Shape second;
}
