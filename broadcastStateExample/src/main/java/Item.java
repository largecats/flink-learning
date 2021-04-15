import java.io.Serializable;

public class Item implements Serializable {
    public Item() {};

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
}
