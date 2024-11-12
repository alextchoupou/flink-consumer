
package group.ventis;

import group.ventis.deserializer.JSONValueDeserializationSchema;
import group.ventis.dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String topic = "financial_transaction";

		KafkaSource<Transaction> kafkaSource = KafkaSource.<Transaction>builder()
				.setBootstrapServers("broker:29092")
				.setTopics(topic)
				.setGroupId("flink-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONValueDeserializationSchema())
				.build();

		DataStream<Transaction> transactionDataStream = env.fromSource(kafkaSource,
				WatermarkStrategy.noWatermarks(), "kafka source");

		transactionDataStream.print("Financial Transaction :");

		env.execute("Ventis Flink Job");

	}
}