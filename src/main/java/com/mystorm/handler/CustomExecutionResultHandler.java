package com.mystorm.handler;

import com.datastax.driver.core.exceptions.DriverException;
import com.mystorm.utils.Utils;
import org.apache.storm.cassandra.BaseExecutionResultHandler;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomExecutionResultHandler extends BaseExecutionResultHandler {

  private static final long serialVersionUID = 16293L;

  private static final Logger LOGGER = LoggerFactory.getLogger(CustomExecutionResultHandler.class);

  @Override
  public void onQuerySuccess(OutputCollector collector, Tuple tuple) {

    LOGGER.info("success event for tuple {}", tuple);
    // can also do collector.emit() from here
    collector.ack(tuple);
  }

  @Override
  protected void onDriverException(DriverException e, OutputCollector collector, Tuple tuple) {

    LOGGER.error("Error event for tuple : {} , error : {}", tuple, Utils.exceptionParser(e));
    collector.fail(tuple);
  }
}
