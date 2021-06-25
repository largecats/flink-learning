import java.util.HashMap;

public class DailyInfo {
    public HashMap<String, Long> users; // Map from user id to first appear timestamp in milliseconds
    public long accumulateStart; // timestamp to start accumulation in milliseconds
    public long lastModified; // last modified timestamp of the state in milliseconds
}
