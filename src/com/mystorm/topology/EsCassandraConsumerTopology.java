package com.mystorm.topology;

import com.mystorm.bolt.EccProcessorBolt;
import com.mystorm.bolt.ElasticSearchBolt;
import com.mystorm.enums.StreamConstant;
import com.mystorm.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * -----------------------------------------------------------------------------
 * Topology with ES bolt and cassandra bolt example
 * -----------------------------------------------------------------------------
 */
public class EsCassandraConsumerTopology {

  private static final Logger LOGGER = LoggerFactory.getLogger(EsCassandraConsumerTopology.class);

  public static final String TOPOLOGY_NAME = "ES_CASSANDRA_CONSUMER_TOPOLOGY";

  public static final String SPOUT_KAFKA_BROKER = "localhost:9092";
  public static final String SPOUT_KAFKA_TOPIC = "ECC_spout_topic";
  public static final String SPOUT_KAFKA_CONSUMER_NAME = "ECC_spout_CONSUMER";

  public static final String ECC_SPOUT_ID = "ECC_SPOUT_ID";
  public static final String ECC_PROCESSOR_BOLT_ID = "ECC_PROCESSOR_BOLT";
  public static final String ECC_ELASTIC_SEARCH_BOLT_ID = "ECC_ELASTIC_SEARCH_BOLT";
  public static final String ECC_CASSANDRA_BOLT_ID = "ECC_CASSANDRA_BOLT";

  public static final String[] ES_HOSTS = "localhost:9200".split(",");
  public static final String ES_INDEX_NAME = "ecc_index";

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

    ElasticSearchBolt elasticSearchBolt = new ElasticSearchBolt(ES_HOSTS, ES_INDEX_NAME);

    /*
     * -----------------------------------------------------------------------------
     * Building topology
     * -----------------------------------------------------------------------------
     */
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(ECC_SPOUT_ID, kafkaSpout, 1);

    builder
        .setBolt(ECC_PROCESSOR_BOLT_ID, new EccProcessorBolt(), 1)
        .localOrShuffleGrouping(ECC_SPOUT_ID);

    builder
        .setBolt(
            ECC_ELASTIC_SEARCH_BOLT_ID,
            elasticSearchBolt.withTumblingWindow(BaseWindowedBolt.Duration.seconds(1)),
            1)
        .localOrShuffleGrouping(ECC_PROCESSOR_BOLT_ID, StreamConstant.ECC_COMMON_STREAM.name());

    /*builder
    .setBolt(ECC_CASSANDRA_BOLT_ID, null,1)
    .localOrShuffleGrouping(ECC_PROCESSOR_BOLT_ID, StreamConstant.ECC_COMMON_STREAM.name());*/

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
