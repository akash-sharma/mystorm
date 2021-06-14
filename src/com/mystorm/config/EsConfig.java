package com.mystorm.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class EsConfig {

  private RestHighLevelClient esClient;

  public EsConfig(String[] esHosts) {

    HttpHost[] hostNodes = new HttpHost[esHosts.length];

    for (int index = 0; index < esHosts.length; index++) {
      String host = esHosts[index];
      String[] hostAndPort = host.split(":");
      hostNodes[index] = new HttpHost(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
    }

    esClient =
        new RestHighLevelClient(
            RestClient.builder(hostNodes)
                .setRequestConfigCallback(
                    requestConfigBuilder ->
                        requestConfigBuilder.setConnectTimeout(7000).setSocketTimeout((int) 7000))
                .setMaxRetryTimeoutMillis(65000));
  }

  public RestHighLevelClient getEsClient() {
    return esClient;
  }
}
