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
    public static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String requestId = UUID.randomUUID().toString();
        // TODO : add requestId to MDC
        LOGGER.info("requestId : {}, tuple : {}", requestId, tuple);

        String kafkaKey = tuple.getStringByField("key");
        String value = tuple.getStringByField("value");
        LOGGER.info("kafkaKey : {}, value : {}", kafkaKey, value);

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
