package com.mystorm.topology;

import com.mystorm.bolt.FgkoFieldProcessorBolt;
import com.mystorm.bolt.FgkoPartitionerBolt;
import com.mystorm.streams.StreamConstant;
import com.mystorm.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FieldGroupKafkaOutputTopology {

  private static final Logger LOGGER = LoggerFactory.getLogger(FieldGroupKafkaOutputTopology.class);

  public static final String TOPOLOGY_NAME = "FIELD_GROUP_KAFKA_OUTPUT_TOPOLOGY";

  public static final String SPOUT_KAFKA_BROKER = "localhost:9092";
  public static final String SPOUT_KAFKA_TOPIC = "FGKO_spout_topic";
  public static final String SPOUT_KAFKA_CONSUMER_NAME = "FGKO_spout_CONSUMER";

  public static final String FGKO_OUTPUT_KAFKA_BROKER = "localhost:9092";
  public static final String FGKO_OUTPUT_KAFKA_TOPIC = "FGKO_output_topic";

  public static final String FGKO_SPOUT_ID = "FGKO_SPOUT_ID";
  public static final String FGKO_PARTITIONER_BOLT_ID = "FGKO_PARTITIONER_BOLT_ID";
  public static final String FGKO_FIELD_PROCESSOR_BOLT_ID = "FGKO_FIELD_PROCESSOR_BOLT_ID";
  public static final String FGKO_KAFKA_OUTPUT_BOLT_ID = "FGKO_KAFKA_OUTPUT_BOLT_ID";

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
            .setMaxUncommittedOffsets(1)
            .setOffsetCommitPeriodMs(2000)
            .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10000000)
            .build();
    KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);

    /*
     * -----------------------------------------------------------------------------
     * output kafka bolt
     * -----------------------------------------------------------------------------
     */
    Properties ouputKafkaProps = new Properties();
    ouputKafkaProps.put("bootstrap.servers", FGKO_OUTPUT_KAFKA_BROKER);
    ouputKafkaProps.put("acks", "1");
    ouputKafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    ouputKafkaProps.put(
        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    KafkaBolt kafkaOutputBolt =
        new KafkaBolt()
            .withProducerProperties(ouputKafkaProps)
            .withTopicSelector(new DefaultTopicSelector(FGKO_OUTPUT_KAFKA_TOPIC))
            .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

    /*
     * -----------------------------------------------------------------------------
     * Building topology
     * -----------------------------------------------------------------------------
     */
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(FGKO_SPOUT_ID, kafkaSpout, 1);

    builder
        .setBolt(FGKO_PARTITIONER_BOLT_ID, new FgkoPartitionerBolt(), 1)
        .localOrShuffleGrouping(FGKO_SPOUT_ID);

    builder
        .setBolt(FGKO_FIELD_PROCESSOR_BOLT_ID, new FgkoFieldProcessorBolt(), 1)
        .fieldsGrouping(
            FGKO_PARTITIONER_BOLT_ID,
            StreamConstant.FGKO_FIELD_PARTITIONER_STREAM.name(),
            new Fields("customerId"));

    builder
        .setBolt(FGKO_KAFKA_OUTPUT_BOLT_ID, kafkaOutputBolt, 1)
        .fieldsGrouping(
            FGKO_KAFKA_OUTPUT_BOLT_ID,
            StreamConstant.FGKO_KAFKA_OUTPUT_STREAM.name(),
            new Fields("key"));

    /*
     * -----------------------------------------------------------------------------
     * Topology level configs
     * -----------------------------------------------------------------------------
     */
    Config conf = new Config();
    conf.setNumWorkers(1); // default value
    conf.setMessageTimeoutSecs(60); // default value
    conf.registerMetricsConsumer(LoggingMetricsConsumer.class);

    /*
     * -----------------------------------------------------------------------------
     * Topology submit command
     * -----------------------------------------------------------------------------
     */
    try {
      StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
      LOGGER.info("{} topology started", TOPOLOGY_NAME);
    } catch (Exception e) {
      LOGGER.error("Error in simple consumer topology : {}", Utils.exceptionParser(e));
    }
  }
}
