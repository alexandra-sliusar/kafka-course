package service.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static utils.configuration.PropertiesUtils.getProperty;

@Slf4j
public class TwitterClientService {

  private final String consumerKey = getProperty("twitter-consumer-key");
  private final String consumerSecret = getProperty("twitter-consumer-secret");
  private final String token = getProperty("twitter-api-token");
  private final String tokenSecret = getProperty("twitter-token-secret");


  public BlockingQueue<String> createMessageQueue(Integer capacity) {
    return new LinkedBlockingQueue<>(capacity);
  }

  public BasicClient createTwitterClient(BlockingQueue<String> msgQueue) {
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    List<String> terms = Lists.newArrayList("playstation");
    hosebirdEndpoint.trackTerms(terms);

    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

    return new ClientBuilder()
        .name("Hosebird-Client-01")
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue))
        .build();
  }

}
