package ru.danil;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Danil_Slesarenko
 */
@Slf4j
public class CommitAllMassageInTopic {

    public static void main(String[] args) {
        //set topic, url and consumer group and call method
        //first method have more logs then second, enjoy

        new CommitAllMassageInTopic().forceCommitTest();
    }

    private static final String consumerGroup = "client-ActorReplicationDto-v1";

    private static final String topic = "data-event-ActorReplicationDto";

    private static final String url = "k8s-dev.vtb-dbo.projects.epam.com:30815";

    protected DefaultKafkaConsumerFactory<String, String> consumerFactory;

    public void setUp() {

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(url, consumerGroup, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 1000);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);

    }


    public void forceCommitTest() {
        setUp();
        OffsetAndMetadata committed;
        try (Consumer<String, String> kafkaConsumer = consumerFactory.createConsumer()) {
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, String> poll = null;
            while (poll == null || poll.count() == 0) {
                poll = kafkaConsumer.poll(3000L);
            }

            log.info("POLL COUNT: " + poll.count());
            AtomicReference<String> recordTopic = new AtomicReference<>();
            AtomicInteger recordPartition = new AtomicInteger();
            AtomicBoolean atomicBoolean = new AtomicBoolean(false);
            poll.records(topic).forEach(record -> {
                log.info("TOPIC: " + record.topic());
                log.info("PARTITION: " + record.partition());
                log.info("KEY: " + record.key() + "VALUE: " + record.value());
                log.info("OFFSET: " + record.offset());
                if (!atomicBoolean.get()) {
                    recordTopic.set(record.topic());
                    recordPartition.set(record.partition());
                    atomicBoolean.set(true);
                }
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                kafkaConsumer.commitSync(offsets);
            });
            committed = kafkaConsumer
                    .committed(new TopicPartition(recordTopic.get(), recordPartition.get()));
        }
        log.info("THE LAST COMMITTED OFFSET: " + committed.offset());
        log.info("THE LAST COMMITTED METADATA: " + committed.metadata());
    }

    public void easyImplForceCommitTest() {
        Properties props = new Properties();
        props.put("bootstrap.servers", url);
        props.put("group.id", consumerGroup);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            ConsumerRecords<String, String> poll = null;
            while (poll == null || poll.count() == 0) {
                poll = consumer.poll(3000L);
            }
            log.info("POLL SIZE IS:" + poll.count());
            consumer.commitSync();
        }
    }
}
