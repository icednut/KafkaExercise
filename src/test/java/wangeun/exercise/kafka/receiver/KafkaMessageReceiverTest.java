package wangeun.exercise.kafka.receiver;

import org.junit.*;
import wangeun.exercise.kafka.receiver.KafkaMessageReceiver;

import java.util.Properties;

/**
 * @Author wangeun.lee@sk.com
 */
public class KafkaMessageReceiverTest {
	private KafkaMessageReceiver receiver;

	@Before
	public void setUp() throws Exception {
		Properties configProperties = new Properties();
		configProperties.put("group.id", "testGroupId");
		configProperties.put("zookeeper.connect", "localhost:2181");

		receiver = new KafkaMessageReceiver();
		receiver.setConfigProperties(configProperties);
		receiver.setTopicName("testTopic");
	}

	@Test
	public void testCustomMessageConsumerTest_성공() throws Exception {
		receiver.execute();
	}
}
