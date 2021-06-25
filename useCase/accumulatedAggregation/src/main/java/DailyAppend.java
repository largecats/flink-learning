import java.sql.Timestamp;

public class DailyAppend {
    public Timestamp time;
    public Long accumulatedDistinct;

    public DailyAppend(Timestamp time, Long accumulatedDistinct) {
        this.time = time;
        this.accumulatedDistinct = accumulatedDistinct;
    }
}
