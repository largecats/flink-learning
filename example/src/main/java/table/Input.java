package table;

import org.apache.flink.api.java.tuple.*;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class Input {

    public static Tuple5<String, Long, String, Double, Long>[] orders = new Tuple5[]{
            Tuple5.of("FRANCE", 10001L, "amy", 1000.0, 1L),
            Tuple5.of("US", 502L, "bob", 50.0, 3L),
            Tuple5.of("FRANCE", 401L, "frank", 190.0, 5L),
            Tuple5.of("CHINA", 101L, "xiaoming", 1000.0, 6L)
    };
    
    private static ZoneId zoneId = ZoneId.of("Asia/Shanghai");

    public static Tuple3<String, String, Long>[] clicks = new Tuple3[]{
            Tuple3.of("Mary", "./home", LocalDateTime.of(2021, 1, 1, 12, 0, 0).atZone(zoneId).toEpochSecond() * 1000), // convert to milliseconds
            Tuple3.of("Bob", "./cart", LocalDateTime.of(2021, 1, 1, 12, 0, 0).atZone(zoneId).toEpochSecond() * 1000),
            Tuple3.of("Mary", "./prod?id=1", LocalDateTime.of(2021, 1, 1, 12, 2, 0).atZone(zoneId).toEpochSecond() * 1000),
            Tuple3.of("Mary", "./prod?id=4", LocalDateTime.of(2021, 1, 1, 12, 55, 0).atZone(zoneId).toEpochSecond() * 1000),
            Tuple3.of("Bob", "./prod?id=5", LocalDateTime.of(2021, 1, 1, 13, 01, 0).atZone(zoneId).toEpochSecond() * 1000),
            Tuple3.of("Liz", "./home", LocalDateTime.of(2021, 1, 1, 13, 30, 0).atZone(zoneId).toEpochSecond() * 1000),
            Tuple3.of("Liz", "./prod?id=7", LocalDateTime.of(2021, 1, 1, 13, 59, 0).atZone(zoneId).toEpochSecond() * 1000),
            Tuple3.of("Mary", "./cart", LocalDateTime.of(2021, 1, 1, 14, 0, 0).atZone(zoneId).toEpochSecond() * 1000),
            Tuple3.of("Liz", "./home", LocalDateTime.of(2021, 1, 1, 14, 2, 0).atZone(zoneId).toEpochSecond() * 1000),
            Tuple3.of("Bob", "./prod?id=3", LocalDateTime.of(2021, 1, 1, 14, 30, 0).atZone(zoneId).toEpochSecond() * 1000),
            Tuple3.of("Bob", "./home", LocalDateTime.of(2021, 1, 1, 14, 40, 0).atZone(zoneId).toEpochSecond() * 1000),
    };
}
