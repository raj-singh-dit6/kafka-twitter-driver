package ml.trendingtweets.app;

import org.apache.log4j.BasicConfigurator;

import ml.trendingtweets.app.producer.TwitterKafkaProducer;

public class App {
    public static void main(String[] args) {
    	BasicConfigurator.configure();
        System.out.println("*********************Tweets***************************");
        TwitterKafkaProducer producer = new TwitterKafkaProducer();
        producer.run();
        
//        TwitteraKafkaConsumer consumer = new TwitteraKafkaConsumer();
//        consumer.run();
    }
}
