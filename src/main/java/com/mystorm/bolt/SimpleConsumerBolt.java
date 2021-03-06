package com.mystorm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

public class SimpleConsumerBolt extends BaseRichBolt {

  private static final long serialVersionUID = 123765L;
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerBolt.class);

  private OutputCollector collector;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    LOGGER.info("SimpleConsumerBolt prepare");
    this.collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    String requestId = UUID.randomUUID().toString();
    LOGGER.info("requestId : {}, tuple : {}", requestId, tuple);

    String kafkaKey = tuple.getStringByField("key");
    String value = tuple.getStringByField("value");
    String partition = tuple.getStringByField("partition");
    String offset = tuple.getStringByField("offset");
    LOGGER.info(
        "partition : {}, offset : {}, kafkaKey : {}, value : {}",
        partition,
        offset,
        kafkaKey,
        value);

    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
}
