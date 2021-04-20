package common.datatypes;

import java.io.Serializable;

public class Shape implements Serializable {
    public Shape() {};

    public Shape(String value) {
        this.value = value;
    }

    public String value;

    @Override
    public boolean equals(Object other) {
        return other instanceof Shape && this.value.equals(((Shape) other).value);
    }
}
