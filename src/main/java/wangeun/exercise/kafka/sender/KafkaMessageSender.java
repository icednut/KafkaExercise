package wangeun.exercise.kafka.sender;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Author wangeun.lee@sk.com
 */
@Data
public class KafkaMessageSender {
	private Properties configProperties;

	public void execute(KeyedMessage message) {
		ProducerConfig producerConfig = new ProducerConfig(configProperties);
		Producer<Integer, String> producer = new Producer<Integer, String>(producerConfig);
		List<KeyedMessage<Integer, String>> messageList = new ArrayList<KeyedMessage<Integer, String>>();

		messageList.add(message);
		producer.send(messageList);
	}
}
