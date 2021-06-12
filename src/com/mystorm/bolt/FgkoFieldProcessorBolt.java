package com.mystorm.bolt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mystorm.dto.FieldGroupDto;
import com.mystorm.service.FieldGroupService;
import com.mystorm.streams.StreamConstant;
import com.mystorm.utils.Utils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;

public class FgkoFieldProcessorBolt extends BaseRichBolt {

  private static final long serialVersionUID = 129715L;
  private static final Logger LOGGER = LoggerFactory.getLogger(FgkoFieldProcessorBolt.class);

  private static ObjectMapper MAPPER = new ObjectMapper();
  private OutputCollector collector;
  private FieldGroupService fieldGroupService;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    LOGGER.info("FgkoFieldProcessorBolt prepare");
    this.collector = outputCollector;
    fieldGroupService = new FieldGroupService();
  }

  @Override
  public void execute(Tuple tuple) {
    String requestId = UUID.randomUUID().toString();
    LOGGER.info("requestId : {}, tuple : {}", requestId, tuple);

    String topic = tuple.getStringByField("topic");
    String packetData = tuple.getStringByField("value");
    Object offset = tuple.getValueByField("offset");
    Object partition = tuple.getValueByField("partition");
    String customerId = tuple.getStringByField("customerId");

    FieldGroupDto fieldGroupDto = fieldGroupService.getFieldGroupDto(packetData);
    // TODO : We can do some logic with dto here

    String key = fieldGroupDto.getCustomerId();
    String message = getKafkaMessage(fieldGroupDto);
    final List<Object> outputTuple = Arrays.asList(key, message);
    collector.emit(StreamConstant.FGKO_KAFKA_OUTPUT_STREAM.name(), outputTuple);
    this.collector.ack(tuple);
  }

  private String getKafkaMessage(FieldGroupDto fieldGroupDto) {
    Map<String, Object> map = new HashMap<>();
    map.put("id", fieldGroupDto.getId());
    map.put("customerId", fieldGroupDto.getCustomerId());
    map.put("packetProcessingTime", LocalDateTime.now().toString());

    try {
      return MAPPER.writeValueAsString(map);
    } catch (JsonProcessingException e) {
      LOGGER.error(
          "Error in parsing getKafkaMessage map : {} as string: {}", map, Utils.exceptionParser(e));
    }
    return null;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    List<String> attributes = Arrays.asList("key", "message");
    outputFieldsDeclarer.declareStream(
        StreamConstant.FGKO_KAFKA_OUTPUT_STREAM.name(), new Fields(attributes));
  }
}
