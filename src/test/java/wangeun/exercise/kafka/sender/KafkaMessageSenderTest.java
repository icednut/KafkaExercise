package wangeun.exercise.kafka.sender;

import kafka.producer.KeyedMessage;
import org.junit.*;
import wangeun.exercise.kafka.sender.KafkaMessageSender;

import java.util.Properties;

/**
 * @Author wangeun.lee@sk.com
 */
public class KafkaMessageSenderTest {
	private KafkaMessageSender sender;

	@Before
	public void setUp() throws Exception {
		Properties configProperties = new Properties();
		configProperties.put("metadata.broker.list", "localhost:9092");
		configProperties.put("request.required.acks", "1");
		configProperties.put("producer.type", "sync");
		configProperties.put("serializer.class", "kafka.serializer.StringEncoder");

		sender = new KafkaMessageSender();
		sender.setConfigProperties(configProperties);
	}

	@Test
	public void testCustomMessageProducer_성공() throws Exception {
		KeyedMessage<Integer, String> message = new KeyedMessage<Integer, String>("testTopic", "Hello World!!");

		sender.execute(message);
	}
}
