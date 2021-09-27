package service.elasticsearch;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static utils.configuration.PropertiesUtils.getProperty;

@Slf4j
public class ElasticSearchClientService {

  private final String hostname = getProperty("elastic-search-hostname");
  private final String username = getProperty("elastic-search-username");
  private final String password = getProperty("elastic-search-password");


  public RestHighLevelClient createClient() {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
    RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
        .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

    return new RestHighLevelClient(builder);
  }

  public void pushMessage(RestHighLevelClient client, ConsumerRecord<String, String> message) throws IOException {
    final IndexRequest indexRequest = createIndexRequest(client, message);
    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
    log.info("Response id: {}, result: {}", indexResponse.getId(), indexResponse.getResult().name());
  }

  public IndexRequest createIndexRequest(RestHighLevelClient client, ConsumerRecord<String, String> message) {
    String id = extractIdFromMessage(message);
    return new IndexRequest("twitter",
                            "tweets",
                            id)//id for idempotent elastic search consumer
        .source(message.value(), XContentType.JSON);
  }

  private String extractIdFromMessage(ConsumerRecord<String, String> message) {
    JsonParser jsonParser = new JsonParser();
    return jsonParser.parse(message.value())
        .getAsJsonObject()
        .get("id_str") //id from twitter
        .getAsString();
  }

}
