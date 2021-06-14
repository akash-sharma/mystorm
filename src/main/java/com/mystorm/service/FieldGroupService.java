package com.mystorm.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mystorm.dto.FieldGroupDto;
import com.mystorm.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldGroupService {

  private static final Logger LOGGER = LoggerFactory.getLogger(FieldGroupService.class);

  private static ObjectMapper MAPPER = new ObjectMapper();

  public FieldGroupDto getFieldGroupDto(String packetData) {

    FieldGroupDto fieldGroupDto = null;
    if (!StringUtils.isEmpty(packetData)) {
      try {
        fieldGroupDto = MAPPER.readValue(packetData, FieldGroupDto.class);
      } catch (Exception e) {
        LOGGER.error(
            "error whie parsing fieldGroupDto : {} , error : {}",
            packetData,
            Utils.exceptionParser(e));
      }
    }

    LOGGER.info("fieldGroupDto : {}", fieldGroupDto);
    return fieldGroupDto;
  }
}
