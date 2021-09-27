package service;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static utils.configuration.PropertiesUtils.getProperty;

public class KafkaStreamsApplication {

  private static final JsonParser jsonParser = new JsonParser();


  public static void main(String[] args) {
    //create properties
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getProperty("kafka-bootstrap-server"));
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

    //create topology
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    final KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
    final KStream<String, String> filteredStream = inputTopic.filter((k, tweet) -> extractUserFollowers(tweet) > 1000);
    filteredStream.to("important_tweets");

    //build the topology
    KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),
                                                 properties);

    //start
    kafkaStreams.start();
  }

  private static Integer extractUserFollowers(String message) {
    try {
      return jsonParser.parse(message)
          .getAsJsonObject()
          .get("user")
          .getAsJsonObject()
          .get("followers_count")
          .getAsInt();
    }
    catch (Exception e) {
      return 0;
    }
  }

}
