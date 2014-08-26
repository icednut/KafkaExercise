package wangeun.exercise.kafka.receiver;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @Author wangeun.lee@sk.com
 */
@Data
public class KafkaMessageReceiver {
	private static final int THREAD_COUNT = 1;
	private Properties configProperties;
	private String topicName;

	public void execute() {
		ConsumerConfig config = new ConsumerConfig(configProperties);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topicName, THREAD_COUNT);

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streamList = consumerMap.get(topicName);

		for (KafkaStream<byte[], byte[]> stream : streamList) {
			ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();
			System.out.println("streamList size == " + streamList.size());

//			while (consumerIterator.hasNext()) {
//				System.out.println("Read Start");
//				System.out.println("Consumer Message: " + new String(consumerIterator.next().message()));
//				System.out.println("Read End!!");
//			}

			for (MessageAndMetadata<byte[], byte[]> msgAndMetadata : stream) {
				System.out.println("Consumer Message: " + new String(msgAndMetadata.message()));
			}
		}
	}
}
