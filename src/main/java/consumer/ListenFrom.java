package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ListenFrom {
    private final Properties consumerProps;
    private final Jedis j;

    public ListenFrom() {
        //TODO: move properties to a file; Get details from command prompt
        //TODO: Demonstrate how consumer groups work
        //TODO: Demonstrate how publish-subscribe model works
        consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "text-reading-group");
        consumerProps.put("enable.auto.commit", true);
        consumerProps.put("auto.commit.interval.ms", 1000);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //TODO: change to pool
        j = new Jedis("127.0.0.1", 6379);

    }

    public static void main(String args[]) throws IOException {
        ListenFrom l = new ListenFrom();
        //listen-consumer is not thread-safe
        KafkaConsumer<String, String> listenConsumer = new KafkaConsumer<>(l.consumerProps);
        listenConsumer.subscribe(Arrays.asList("voice"));

        Stream<String> lines = Files.lines(Paths.get("C:\\Users\\sarath chandra\\Desktop\\books\\madling\\MovieSummaries\\stopwords.txt"));
        List<String> stopwords = lines.map(String::trim).map(String::toLowerCase).distinct().collect(Collectors.toList());
        while (true) {
            Duration d = Duration.ofMillis(1000);
            ConsumerRecords<String, String> records = listenConsumer.poll(d);
            l.consume(records, stopwords);
            System.out.println(records.count());
        }

    }

    private void consume(ConsumerRecords<String, String> records, List<String> stopwords) {
        for (ConsumerRecord r : records) {
            String[] tokens = ((String) r.value()).split("\\s");
            HashSet<String> keys = new HashSet<>();
            for (String t : tokens) {
                if (!stopwords.contains(t.toLowerCase())) {
                    String key = (String) r.key();
                    //TODO: change to pipeline commands
                    j.zincrby(key, 1.0, t);
                    j.zincrby("global-token-count", 1.0, t);
                    keys.add(key);
                }
            }
            if(keys.size()>0) {
                System.out.println(keys);
                String[] a = new String[keys.size()];
                j.sadd("movies-list", keys.toArray(a));
            }

        }
    }
}

