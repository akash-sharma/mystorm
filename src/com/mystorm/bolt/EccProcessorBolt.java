package com.mystorm.bolt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mystorm.dto.EccProcessorDto;
import com.mystorm.enums.StreamConstant;
import com.mystorm.utils.Utils;
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

public class EccProcessorBolt extends BaseRichBolt {

  private static final long serialVersionUID = 173815L;
  private static final Logger LOGGER = LoggerFactory.getLogger(EccProcessorBolt.class);

  private static ObjectMapper MAPPER = new ObjectMapper();
  private OutputCollector collector;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    LOGGER.info("EccProcessorBolt prepare");
    this.collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    String requestId = UUID.randomUUID().toString();
    LOGGER.info("requestId : {}, tuple : {}", requestId, tuple);

    String packetData = tuple.getStringByField("value");
    EccProcessorDto eccProcessorDto = getEccProcessorDto(packetData);
    if (eccProcessorDto == null
        || eccProcessorDto.getId() == null
        || eccProcessorDto.getTnxId() == null
        || eccProcessorDto.getUserId() == null) {
      this.collector.ack(tuple);
      return;
    }

    List<Object> output =
        Arrays.asList(
            eccProcessorDto.getId(), eccProcessorDto.getUserId(), eccProcessorDto.getTnxId());

    collector.emit(StreamConstant.ECC_COMMON_STREAM.name(), output);
    this.collector.ack(tuple);
  }

  public EccProcessorDto getEccProcessorDto(String packetData) {

    EccProcessorDto eccProcessorDto = null;
    if (!StringUtils.isEmpty(packetData)) {
      try {
        eccProcessorDto = MAPPER.readValue(packetData, EccProcessorDto.class);
      } catch (Exception e) {
        LOGGER.error(
            "error whie parsing EccProcessorDto : {} , error : {}",
            packetData,
            Utils.exceptionParser(e));
      }
    }

    LOGGER.info("eccProcessorDto : {}", eccProcessorDto);
    return eccProcessorDto;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    List<String> attributes = Arrays.asList("id", "user_id", "tnx_id");

    outputFieldsDeclarer.declareStream(
        StreamConstant.ECC_COMMON_STREAM.name(), new Fields(attributes));
  }
}
