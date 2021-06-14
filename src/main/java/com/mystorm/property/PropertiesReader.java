package com.mystorm.property;

import com.mystorm.exception.PropertyReaderException;
import org.apache.commons.lang.StringUtils;

import java.io.InputStream;
import java.util.Properties;

public class PropertiesReader {

  public Properties readProperties(String filePath) {

    try {
      Properties properties = new Properties();
      if (StringUtils.isEmpty(filePath)) {
        return properties;
      }
      InputStream inputStream =
          Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath);
      properties.load(inputStream);
      return properties;
    } catch (Exception e) {
      throw new PropertyReaderException("property reader exception", e);
    }
  }
}
