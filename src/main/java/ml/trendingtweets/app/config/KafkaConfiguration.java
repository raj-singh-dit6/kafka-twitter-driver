package ml.trendingtweets.app.config;

public class KafkaConfiguration {
    public static final String SERVERS = "localhost:9092, localhost:9092, localhost:9092";
    public static final String TOPIC = "bigdata-tweets";
    public static final long SLEEP_TIMER = 1000;
}
