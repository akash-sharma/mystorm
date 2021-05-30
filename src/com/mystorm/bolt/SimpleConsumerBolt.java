package com.mystorm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
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
    LOGGER.info("SimpleConsumerBolt prepare logger");
    System.out.println("SimpleConsumerBolt prepare sys out");
    this.collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    LOGGER.info("tuple : {}", tuple);
    String requestId = UUID.randomUUID().toString();
    // TODO : add requestId to MDC
    LOGGER.info("requestId : {}, tuple : {}", requestId, tuple);

    String kafkaKey = tuple.getStringByField("key");
    String value = tuple.getStringByField("value");
    LOGGER.info("kafkaKey : {}, value : {}", kafkaKey, value);
    Utils.LOG.info("Util log, kafkaKey : {}, value : {}", kafkaKey, value);
    System.out.println("sys out, kafkaKey : " + kafkaKey + ", value : " + value);

    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
}
