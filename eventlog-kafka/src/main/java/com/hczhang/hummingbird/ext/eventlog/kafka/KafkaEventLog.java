package com.hczhang.hummingbird.ext.eventlog.kafka;

import com.hczhang.hummingbird.event.Event;
import com.hczhang.hummingbird.eventlog.AbstractEventLog;
import com.hczhang.hummingbird.model.AggregateRoot;
import com.hczhang.hummingbird.serializer.JsonSerializer;
import com.hczhang.hummingbird.serializer.Serializer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.util.Properties;

/**
 * Created by steven on 8/14/15.
 */
public class KafkaEventLog extends AbstractEventLog {

    private Serializer serializer;

    @Value("${kafka.topic}")
    private String topic;

    @Value("${kafka.brokers}")
    private String brokers;

    @Value("${kafka.acks}")
    private String acks;

    private ProducerConfig config;

    public KafkaEventLog() {
        serializer = new JsonSerializer();
    }

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("serializer.class","kafka.serializer.StringEncoder");
        //props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", acks);

        config = new ProducerConfig(props);

    }

    @Override
    public void recordEvent(Event event, AggregateRoot aggregateRoot) {
        KeyedMessage<String, String> message =new KeyedMessage<String, String>(topic, new String(serializer.serialize(event)));

        Producer<String,String> producer = new Producer<String, String>(config);
        producer.send(message);
        producer.close();
    }
}
