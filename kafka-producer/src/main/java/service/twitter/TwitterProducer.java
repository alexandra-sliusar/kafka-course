package service.twitter;

import com.twitter.hbc.httpclient.BasicClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import service.kafka.KafkaProducerService;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TwitterProducer {

  private final TwitterClientService twitterClientService;
  private final KafkaProducerService kafkaProducerService;

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {
    log.info("Start of application");
    BlockingQueue<String> msgQueue = twitterClientService.createMessageQueue(100);

    final BasicClient twitterClient = twitterClientService.createTwitterClient(msgQueue);

    //create a kafka producer
    KafkaProducer<String, String> kafkaProducer = kafkaProducerService.createKafkaProducer();

    //loop to send tweets to kafka
    twitterClient.connect();

    while (!twitterClient.isDone()) {
      try {
        final String message = msgQueue.poll(5, TimeUnit.SECONDS);
        if (message != null) {
          log.info(message);
          kafkaProducerService.send(kafkaProducer, message);
        }
      }
      catch (InterruptedException e) {
        e.printStackTrace();
        break;
      }
    }

    kafkaProducer.close();
    twitterClient.stop();
    log.info("End of application");
  }


  public TwitterProducer() {
    twitterClientService = new TwitterClientService();
    kafkaProducerService = new KafkaProducerService();
  }
}
