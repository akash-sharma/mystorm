package com.mystorm.bolt;

import com.mystorm.config.EsConfig;
import com.mystorm.utils.Utils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public class ElasticSearchBolt extends BaseWindowedBolt {

  private static final long serialVersionUID = 173915L;
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchBolt.class);

  private OutputCollector collector;
  private EsConfig esConfig;
  private String esIndexName;

  public ElasticSearchBolt(String[] esHosts, String esIndexName) {
    this.esConfig = new EsConfig(esHosts);
    this.esIndexName = esIndexName;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(TupleWindow tupleWindow) {
    String requestId = UUID.randomUUID().toString();
    LOGGER.info("requestId : {}", requestId);

    try {

      BulkRequest bulkIndexRequest = new BulkRequest();
      for (Tuple tuple : tupleWindow.get()) {
        DocWriteRequest indexRequest = getEsDocument(tuple, requestId);
        if (indexRequest == null) {
          continue;
        }
        bulkIndexRequest.add(indexRequest);
      }

      if (bulkIndexRequest.requests().size() > 0) {
        BulkResponse bulkResponse = esConfig.getEsClient().bulk(bulkIndexRequest);

        for (Iterator iterator = bulkResponse.iterator(); iterator.hasNext(); ) {
          BulkItemResponse indexWriteResponse = (BulkItemResponse) iterator.next();

          if (!indexWriteResponse.isFailed()) {
            LOGGER.info(
                "record indexed with index : {} , type : {} , id : {} , version : {} , opType : {}",
                indexWriteResponse.getIndex(),
                indexWriteResponse.getType(),
                indexWriteResponse.getId(),
                indexWriteResponse.getVersion(),
                indexWriteResponse.getOpType().toString());
          }
        }
      }

      for (Tuple tuple : tupleWindow.get()) {
        collector.ack(tuple);
      }
    } catch (Exception e) {
      LOGGER.error("Error in execute ES bolt : {}", Utils.exceptionParser(e));
      for (Tuple tuple : tupleWindow.get()) {
        collector.fail(tuple);
      }
    }
  }

  public DocWriteRequest getEsDocument(Tuple input, String requestId) {

    String tnxId = input.getStringByField("tnx_id");
    Integer userId = input.getIntegerByField("user_id");

    Map<String, Object> esDocument = new HashMap<>();
    esDocument.put("id", requestId);
    esDocument.put("time", LocalDateTime.now());
    esDocument.put("tnxId", tnxId);
    esDocument.put("userId", userId);

    return new IndexRequest(esIndexName, "doc", requestId)
        .source(esDocument, XContentType.JSON)
        .timeout(TimeValue.timeValueSeconds(1));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
