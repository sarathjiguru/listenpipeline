package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ProduceTo {

    private final KafkaProducer<String, String> listenProducer;

    public ProduceTo() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put("acks", "all");
        producerProps.put("retries", 0);
        producerProps.put("batch.size", 16384);
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("linger.ms", 1);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //KafkaProducer is thread-safe
        listenProducer = new KafkaProducer<>(producerProps);

    }

    void action(String s) {
        String[] split = s.split("\\t");
        s = split[1];
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        listenProducer.send(new ProducerRecord<>("voice", split[0], s));
    }

    public static void main(String args[]) throws InterruptedException {
        //TODO: also show idempotent and transactional producerstext-stream
        //TODO: Get properties from a config manager file
        //TODO: if a producer should be halted; restart it again
        ProduceTo p = new ProduceTo();

        //simulate: read a file and send its content to kafka
        try (Stream<String> lines = Files.lines(Paths.get("C:\\Users\\sarath chandra\\Desktop\\books\\madling\\MovieSummaries\\plot_summaries.txt"))
        ) {
            lines.forEach(p::action);
        } catch (IOException e) {
            e.printStackTrace();
        }
        p.listenProducer.close();
    }
}
