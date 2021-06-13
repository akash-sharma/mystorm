package com.mystorm.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class EsConfig {

  private static EsConfig esConfig;
  private static RestHighLevelClient esClient;
  private static Object lock = new Object();

  private EsConfig(String[] esHosts) {

    HttpHost[] hostNodes = new HttpHost[esHosts.length];

    for (int index = 0; index < esHosts.length; index++) {
      String host = esHosts[index];
      hostNodes[index] = new HttpHost(host.split(":")[0], Integer.parseInt(host.split(":")[1]));
    }

    esClient =
        new RestHighLevelClient(
            RestClient.builder(hostNodes)
                .setRequestConfigCallback(
                    requestConfigBuilder ->
                        requestConfigBuilder.setConnectTimeout(7000).setSocketTimeout((int) 7000))
                .setMaxRetryTimeoutMillis(65000));
  }

  public static EsConfig getObject(String[] hosts) {

    if (esConfig == null) {
      synchronized (lock) {
        if (esConfig == null) {
          esConfig = new EsConfig(hosts);
        }
      }
    }
    return esConfig;
  }

  public static RestHighLevelClient getEsClient(String[] hosts) {
    if (esConfig == null) {
      getObject(hosts);
    }
    return esClient;
  }
}
