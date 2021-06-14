package com.mystorm.bolt;

import com.mystorm.dto.FieldGroupDto;
import com.mystorm.service.FieldGroupService;
import com.mystorm.enums.StreamConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class FgkoPartitionerBolt extends BaseRichBolt {

  private static final long serialVersionUID = 823725L;
  private static final Logger LOGGER = LoggerFactory.getLogger(FgkoPartitionerBolt.class);

  private OutputCollector collector;
  private FieldGroupService fieldGroupService;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    LOGGER.info("FgkoPartitionerBolt prepare");
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

    FieldGroupDto fieldGroupDto = fieldGroupService.getFieldGroupDto(packetData);
    String customerId = requestId;
    if (fieldGroupDto != null && !StringUtils.isEmpty(fieldGroupDto.getCustomerId())) {
      customerId = fieldGroupDto.getCustomerId();
    }

    this.collector.emit(
        StreamConstant.FGKO_FIELD_PARTITIONER_STREAM.name(),
        tuple,
        Arrays.asList(customerId, topic, packetData, offset, partition));
    this.collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    List<String> attributes = Arrays.asList("customerId", "topic", "value", "offset", "partition");
    outputFieldsDeclarer.declareStream(
        StreamConstant.FGKO_FIELD_PARTITIONER_STREAM.name(), new Fields(attributes));
  }
}
