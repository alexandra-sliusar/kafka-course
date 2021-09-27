package service.elasticsearch;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import service.kafka.KafkaConsumerService;

import java.time.Duration;
import java.util.List;

@Slf4j
public class ElasticSearchConsumer {

  private final KafkaConsumerService kafkaConsumerService;
  private final ElasticSearchClientService elasticSearchClientService;

  private static final List<String> TOPICS = List.of("twitter_tweets");

  public static void main(String[] args) {
    new ElasticSearchConsumer().run();
  }

  @SneakyThrows
  public void run() {
    final KafkaConsumer<String, String> kafkaConsumer = kafkaConsumerService.createKafkaConsumer();
    kafkaConsumer.subscribe(TOPICS);

    try (RestHighLevelClient client = elasticSearchClientService.createClient()) {
      while (true) {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
        log.info("Received: {} records", records.count());
        if (records.isEmpty()) {
          break;
        }

        BulkRequest bulkRequest = new BulkRequest();
        for (ConsumerRecord<String, String> record : records) {
          log.info("Record received. Key {}, partition {}, offset {}\nValue {}\n",
                   record.key(), record.partition(), record.offset(), record.value());
//          elasticSearchClientService.pushMessage(client, record);
          final IndexRequest indexRequest = elasticSearchClientService.createIndexRequest(client, record);
          bulkRequest.add(indexRequest);
        }

        client.bulk(bulkRequest, RequestOptions.DEFAULT);
        //committing offsets
        kafkaConsumer.commitSync();
        log.info("Offsets committed");
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }


  ElasticSearchConsumer() {
    this.kafkaConsumerService = new KafkaConsumerService();
    this.elasticSearchClientService = new ElasticSearchClientService();
  }
}
