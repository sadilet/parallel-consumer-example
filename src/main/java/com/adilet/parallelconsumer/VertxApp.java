package com.adilet.parallelconsumer;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.vertx.JStreamVertxParallelStreamProcessor;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.Map;
import java.util.Properties;

import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class VertxApp {

    static String inputTopic = "input-topic";

    Consumer<String, String> getKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("bootstrap.servers", "localhost:29092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "parallel-consumer-app-group-0");
        return new KafkaConsumer<>(properties);
    }

//    Producer<String, String> getKafkaProducer() {
//        return new KafkaProducer<>(new Properties());
//    }

    JStreamVertxParallelStreamProcessor<String, String> parallelConsumer;


    void run() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer(

        );
//        Producer<String, String> kafkaProducer = getKafkaProducer();
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .consumer(kafkaConsumer)
                .maxConcurrency(100)
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC)
                .build();

        this.parallelConsumer = JStreamVertxParallelStreamProcessor.createEosStreamProcessor(options);
        parallelConsumer.subscribe(of(inputTopic));

        postSetup();

        int port = getPort();

        // tag::example[]
        var resultStream = parallelConsumer.vertxHttpReqInfoStream(context -> {
            var consumerRecord = context.getSingleConsumerRecord();
            System.out.printf("Concurrently constructing and returning RequestInfo from record: %s%n", consumerRecord);
            Map<String, String> params = UniMaps.of("recordKey", consumerRecord.key(), "payload", consumerRecord.value());
            return new VertxParallelEoSStreamProcessor.RequestInfo("localhost", port, "/api", params); // <1>
        });
        // end::example[]

        resultStream.forEach(x -> {
            System.out.println("From result stream: " + x);
        });

    }

    protected int getPort() {
        return 8080;
    }

    void close() {
        this.parallelConsumer.closeDrainFirst();
    }

    protected void postSetup() {
        // no-op, for testing
    }

}
