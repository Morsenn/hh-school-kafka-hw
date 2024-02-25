package ru.hh.kafkahw;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import ru.hh.kafkahw.internal.KafkaProducer;
import ru.hh.kafkahw.internal.Service;
import ru.hh.kafkahw.producerinterceptor.ExactlyOnceProducerInterceptor;
import ru.hh.kafkahw.senders.ExactlyOnceSend;

@RunWith(SpringRunner.class)
@Import(KafkaTest.KafkaTestConfiguration.class)
@SpringBootTest(classes = KafkaHwApplication.class)
@DirtiesContext
class KafkaTest {

  @Autowired
  private Service service;

  @Autowired
  private Sender sender;

  @Test
  public void testAtMostOnce() throws Exception {
    Set<String> messages = IntStream.range(1, 101)
        .mapToObj(i -> UUID.randomUUID().toString())
        .collect(Collectors.toSet());
    messages.forEach(message -> sender.doSomething("topic1", message));
    Thread.sleep(5000);
    messages.forEach(message -> {
      assertTrue(1 >= service.count("topic1", message));
    });
  }

  @Test
  public void testAtLeastOnce() throws Exception {
    Set<String> messages = IntStream.range(1, 101)
        .mapToObj(i -> UUID.randomUUID().toString())
        .collect(Collectors.toSet());
    messages.forEach(message -> sender.doSomething("topic2", message));
    Thread.sleep(5000);
    messages.forEach(message -> {
      assertTrue(1 <= service.count("topic2", message));
    });
  }

  @Test
  public void testExactlyOnce() throws Exception {
    Set<String> messages = IntStream.range(1, 101)
        .mapToObj(i -> UUID.randomUUID().toString())
        .collect(Collectors.toSet());
    messages.forEach(message -> sender.doSomething("topic3", message));
    Thread.sleep(5000);
    messages.forEach(message -> {
      assertEquals(1, service.count("topic3", message));
    });
  }

  @TestConfiguration
  public static class KafkaTestConfiguration {

    @Bean
    KafkaContainer kafkaContainer() {
      KafkaContainer kafka =
          new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3")).withEmbeddedZookeeper();
      kafka.start();
      return kafka;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
        KafkaContainer kafka,
        MainErrorHandler errorHandler) {
      ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
      factory.setConsumerFactory(consumerFactory(kafka));
      factory.setCommonErrorHandler(errorHandler.getErrorHandler());
      factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
      return factory;
    }

    @Bean
    public ConsumerFactory<Integer, String> consumerFactory(KafkaContainer kafka) {
      return new DefaultKafkaConsumerFactory<>(consumerConfigs(kafka));
    }

    @Bean
    public Map<String, Object> consumerConfigs(KafkaContainer kafka) {
      Map<String, Object> props = new HashMap<>();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
      return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory(KafkaContainer kafka) {
      Map<String, Object> configProps = new HashMap<>();
      configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      configProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);
      return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean(name = "exactlyOnceProcessingTopics")
    public List<String> processingTopics() {
      return List.of("topic3");
    }

    @Bean
    ExactlyOnceProducerInterceptor producerInterceptor(@Qualifier("exactlyOnceProcessingTopics") List<String> topics) {
      return new ExactlyOnceProducerInterceptor(topics);
    }

    @Bean
    ExactlyOnceSend exactlyOnceSend(
        KafkaProducer producer,
        @Qualifier("exactlyOnceProcessingTopics") List<String> topics) {
      return new ExactlyOnceSend(producer, topics);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(
        KafkaContainer kafka,
        ExactlyOnceProducerInterceptor producerInterceptor
    ) {
      KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory(kafka));
      template.setProducerInterceptor(producerInterceptor);
      return template;
    }
    @Bean
    public KafkaAdmin admin(KafkaContainer kafka) {
      // Этот бин нужен для создания топика с 2 партициями
      Map<String, Object> configs = new HashMap<>();
      configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
      return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic exactlyOnceTopic() {
      return TopicBuilder.name("topic3")
          .partitions(2)
          .build();
    }

  }
}
