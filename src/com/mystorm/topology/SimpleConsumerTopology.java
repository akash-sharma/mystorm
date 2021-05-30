package com.mystorm.topology;

import com.mystorm.bolt.SimpleConsumerBolt;
import com.mystorm.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.TopologyBuilder;

public class SimpleConsumerTopology {

    public static final String TOPOLOGY_NAME = "SIMPLE_CONSUMER_TOPOLOGY";

    public static final String SPOUT_KAFKA_BROKER = "localhost:9092";
    public static final String SPOUT_KAFKA_TOPIC = "SIMPLE_SPOUT_TOPIC";
    public static final String SPOUT_KAFKA_CONSUMER_NAME = "SIMPLE_SPOUT_CONSUMER";

    public static void main(String[] args) {

        /*
         * -----------------------------------------------------------------------------
         * Kafka Spout consumer
         * -----------------------------------------------------------------------------
         */
        KafkaSpoutConfig<String, String> kafkaSpoutConfig =
                KafkaSpoutConfig.builder(SPOUT_KAFKA_BROKER, SPOUT_KAFKA_TOPIC)
                        .setProp("group.id", SPOUT_KAFKA_CONSUMER_NAME)
                        .setProp("key.deserializer", StringDeserializer.class)
                        .setProp("value.deserializer", StringDeserializer.class)
                        .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                        .setFirstPollOffsetStrategy(
                                KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST)
                        .setMaxUncommittedOffsets(3)
                        .setOffsetCommitPeriodMs(2000)
                        .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10000000)
                        .build();
        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);

        /*
         * -----------------------------------------------------------------------------
         * Building topology
         * -----------------------------------------------------------------------------
         */
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("SIMPLE_CONSUMER_SPOUT_ID", kafkaSpout, 1);

        builder.setBolt("SIMPLE_CONSUMER_BOLT_ID", new SimpleConsumerBolt(), 1);

        /*
         * -----------------------------------------------------------------------------
         * Topology level configs
         * -----------------------------------------------------------------------------
         */
        Config conf = new Config();
        conf.setNumWorkers(1);
        conf.setMessageTimeoutSecs(10);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);

        /*
         * -----------------------------------------------------------------------------
         * Topology submit command
         * -----------------------------------------------------------------------------
         */
        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
        } catch (Exception e) {
            System.out.println("Error in simple consumer topology : " + Utils.exceptionParser(e));
        }
    }
}