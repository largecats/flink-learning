import java.sql.Timestamp;

public class Event {
    public String uid;
    public Timestamp timestamp;

    public Event(String uid, Timestamp timestamp) {
        this.uid = uid;
        this.timestamp = timestamp;
    }
}
