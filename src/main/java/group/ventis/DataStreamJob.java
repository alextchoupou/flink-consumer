
package group.ventis;

import group.ventis.deserializer.JSONValueDeserializationSchema;
import group.ventis.dto.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.impl.client.HttpClients;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.search.SearchRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.search.SearchResponse;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RequestOptions;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RestHighLevelClient;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.index.query.QueryBuilders;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.search.builder.SearchSourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.TimeZone;

import static group.ventis.utils.JsonUtil.toJson;

public class DataStreamJob {

	//private static final String JDBC_URL = "jdbc:postgresql://postgres:5432/ventis";
	//private static final String USERNAME = "ventis";
	//private static final String PASSWORD = "ventis";

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String topic = "financials_operations";

		KafkaSource<Operation> kafkaSource = KafkaSource.<Operation>builder()
				.setBootstrapServers("broker:29092")
				.setTopics(topic)
				.setGroupId("flink-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONValueDeserializationSchema())
				.build();

		DataStream<Operation> transactionDataStream = env
				.fromSource(
						kafkaSource,
						WatermarkStrategy
								.<Operation>forMonotonousTimestamps()
								.withTimestampAssigner((operation, timestamp) -> operation.getDate().getTime()),
						"kafka source");


		transactionDataStream.print("Transaction : ");
		DateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss z yyyy");
		formatter.setTimeZone(TimeZone.getDefault());

		DataStream<Operation> formattedTransactionStream = transactionDataStream.map(operation -> {
			if (operation.getDate() != null) {
				// Parse la chaîne de caractères en objet Date
				Date parsedDate = formatter.parse(String.valueOf(operation.getDate()));
				operation.setDate(parsedDate);
			}
			return operation;
		});
		formattedTransactionStream.map( operation -> {
			try ( RestHighLevelClient client = new RestHighLevelClient(
						HttpClients.createDefault(), new HttpHost("elasticsearch", 9200, "http"), null))
				{
					long now = Instant.now().toEpochMilli();
					long past24h = now - 24 * 60 * 60 * 1000;

					SearchRequest searchRequest = new SearchRequest("raw_operations");
					SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
					searchSourceBuilder.query(QueryBuilders.boolQuery()
							.filter(QueryBuilders.termQuery("accountNumber", operation.getAccountNumber()))
							.filter(QueryBuilders.rangeQuery("date").gte(past24h).lte(now))
					);
					searchSourceBuilder.size(0);
					searchRequest.source(searchSourceBuilder);

					SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
					long count = searchResponse.getHits().getTotalHits().value;
					operation.setOperationCountLast24h(count);
				} catch (Exception exception) {
					exception.printStackTrace();
					operation.setOperationCountLast24h(0);
				}
			return operation;
		}).addSink(
				new Elasticsearch7SinkBuilder<Operation>()
						.setHosts(new HttpHost("elasticsearch", 9200, "http"))
						.setConnectionUsername("elastic")
						.setConnectionPassword("btracking")
						.setBulkFlushMaxActions(1)
						.setEmitter((transaction, runtimeContext, requestIndexer) -> {
							String json = toJson(transaction);
							requestIndexer.add(Requests.indexRequest()
									.index("raw_operations")
									.id(transaction.getId())
									.source(json, XContentType.JSON));
						}).build()
		).name("Elasticsearch Sink: Insert data into raw_operations index");

		// formattedTransactionStream.print("Formatted Transaction : ");

		formattedTransactionStream.map(operation -> operation)
						.sinkTo(
								new Elasticsearch7SinkBuilder<Operation>()
										.setHosts(new HttpHost("elasticsearch", 9200, "http"))
										.setConnectionUsername("elastic")
										.setConnectionPassword("btracking")
										.setBulkFlushMaxActions(1)
										.setEmitter((transaction, runtimeContext, requestIndexer) -> {
											String json = toJson(transaction);
											IndexRequest indexRequest = Requests.indexRequest()
													.index("operations")
													.id(transaction.getId())
													.source(json, XContentType.JSON);
											requestIndexer.add(indexRequest);
										}).build()
						).name("Elasticsearch Sink: Insert data into operations index");

		env.enableCheckpointing(5000);
		env.execute("Real time Flink Job");
	}
}