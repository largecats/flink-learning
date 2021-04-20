package common.datatypes;
import common.utils.DataGenerator;

import java.io.Serializable;

public class Item implements Serializable {

    public Item() {
        DataGenerator g = new DataGenerator(0L); // dummy rideId
        this.color = new Color(g.color());
        this.shape = new Shape(g.shape());
    }

    public Item(Color color, Shape shape) {
        this.color = color;
        this.shape = shape;
    }

    public Color color;
    public Shape shape;

    public Color getColor() {
        return color;
    }

    public Shape getShape() {
        return shape;
    }

    public long getEventTime() {
        return System.currentTimeMillis();
    }
}
