
package group.ventis;

import group.ventis.deserializer.JSONValueDeserializationSchema;
import group.ventis.dto.SalesPerCategory;
import group.ventis.dto.SalesPerDay;
import group.ventis.dto.SalesPerMonth;
import group.ventis.dto.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Date;

import static group.ventis.utils.JsonUtil.toJson;

public class DataStreamJob {

	private static final String JDBC_URL = "jdbc:postgresql://postgres:5432/ventis";
	private static final String USERNAME = "ventis";
	private static final String PASSWORD = "ventis";

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

		transactionDataStream.print("Transaction : ");

		JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl(JDBC_URL)
				.withDriverName("org.postgresql.Driver")
				.withUsername(USERNAME)
				.withPassword(PASSWORD)
				.build();
		JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
				.withBatchSize(1000)
				.withBatchIntervalMs(200)
				.build();
		// Sink to Postgres: Create a table if it does not exist
		transactionDataStream.addSink(JdbcSink.sink(
				" CREATE TABLE IF NOT EXISTS transactions ("
						+ " transaction_id VARCHAR(255) PRIMARY KEY,"
						+ " product_id VARCHAR(255),"
						+ " product_name VARCHAR(255),"
						+ " product_category VARCHAR(255),"
						+ " product_price DOUBLE PRECISION,"
						+ " product_brand VARCHAR(255),"
						+ " product_quantity INTEGER,"
						+ " total_amount DOUBLE PRECISION,"
						+ " currency VARCHAR(255),"
						+ " customer_id VARCHAR(255),"
						+ " transaction_date TIMESTAMP,"
						+ " payment_method VARCHAR(255)"
						+ ")",
				(JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
				},
				executionOptions,
				jdbcConnectionOptions
		)).name("JDBC Sink: Create transactions table");

		// sales per category
		transactionDataStream.addSink(JdbcSink.sink(
				"CREATE TABLE IF NOT EXISTS sales_per_category ("
						+ " transaction_date DATE,"
						+ " product_category VARCHAR(255),"
						+ " total_sales DOUBLE PRECISION,"
						+ " PRIMARY KEY (transaction_date, product_category)"
						+ ")",
				(JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
				},
				executionOptions,
				jdbcConnectionOptions
		)).name("JDBC Sink: Create sales_per_category table");

		// sales per day
		transactionDataStream.addSink(JdbcSink.sink(
				"CREATE TABLE IF NOT EXISTS sales_per_day ("
						+ " transaction_date DATE,"
						+ " total_sales DOUBLE PRECISION,"
						+ " PRIMARY KEY (transaction_date)"
						+ ")",
				(JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
				},
				executionOptions,
				jdbcConnectionOptions
		)).name("JDBC Sink: Create sales_per_day table");

		// sales per month
		transactionDataStream.addSink(JdbcSink.sink(
				"CREATE TABLE IF NOT EXISTS sales_per_month ("
						+ " year INTEGER,"
						+ " month INTEGER,"
						+ " total_sales DOUBLE PRECISION,"
						+ " PRIMARY KEY (year, month)"
						+ ")",
				(JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
				},
				executionOptions,
				jdbcConnectionOptions
		)).name("JDBC Sink: Create sales_per_month table");

		transactionDataStream.addSink(JdbcSink.sink(
				"INSERT INTO transactions (transaction_id, product_id, product_name, product_category, product_price, product_brand, product_quantity," +
						" total_amount, currency, customer_id, transaction_date, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
						"ON CONFLICT (transaction_id) DO UPDATE SET product_id = EXCLUDED.product_id, product_name = EXCLUDED.product_name, " +
						"product_category = EXCLUDED.product_category, product_price = EXCLUDED.product_price, product_brand = EXCLUDED.product_brand, " +
						"product_quantity = EXCLUDED.product_quantity, total_amount = EXCLUDED.total_amount, currency = EXCLUDED.currency, " +
						"customer_id = EXCLUDED.customer_id, transaction_date = EXCLUDED.transaction_date, payment_method = EXCLUDED.payment_method " +
						"WHERE transactions.transaction_id = EXCLUDED.transaction_id",
				(JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
					preparedStatement.setString(1, transaction.getTransactionId());
					preparedStatement.setString(2, transaction.getProductId());
					preparedStatement.setString(3, transaction.getProductName());
					preparedStatement.setString(4, transaction.getProductCategory());
					preparedStatement.setDouble(5, transaction.getProductPrice());
					preparedStatement.setString(6, transaction.getProductBrand());
					preparedStatement.setInt(7, transaction.getProductQuantity());
					preparedStatement.setDouble(8, transaction.getTotalAmount());
					preparedStatement.setString(9, transaction.getCurrency());
					preparedStatement.setString(10, transaction.getCustomerId());
					preparedStatement.setTimestamp(11, transaction.getTransactionDate());
					preparedStatement.setString(12, transaction.getPaymentMethod());
				},
				executionOptions,
				jdbcConnectionOptions
		)).name("JDBC Sink: Insert data into transactions table");

		transactionDataStream.map(
						transaction -> {
							Date transactionDate = new Date(System.currentTimeMillis());
							String category = transaction.getProductCategory();
							double totalAmount = transaction.getTotalAmount();
							return new SalesPerCategory(transactionDate, category, totalAmount);
						}
				).keyBy(SalesPerCategory::getCategory)
				.reduce((salesPerCategory, value) -> {
					salesPerCategory.setTotalAmount(salesPerCategory.getTotalAmount() + value.getTotalAmount());
					return salesPerCategory;
				}).addSink(JdbcSink.sink(
						"INSERT INTO sales_per_category (transaction_date, product_category, total_sales) VALUES (?, ?, ?) "
								+ "ON CONFLICT (transaction_date, product_category) DO UPDATE SET total_sales = EXCLUDED.total_sales "
								+ "WHERE sales_per_category.transaction_date = EXCLUDED.transaction_date AND "
								+ "sales_per_category.product_category = EXCLUDED.product_category",
						(JdbcStatementBuilder<SalesPerCategory>) (preparedStatement, salesPerCategory) -> {
							preparedStatement.setDate(1, new Date(System.currentTimeMillis()));
							preparedStatement.setString(2, salesPerCategory.getCategory());
							preparedStatement.setDouble(3, salesPerCategory.getTotalAmount());
						},
						executionOptions,
						jdbcConnectionOptions
				)).name("JDBC Sink: Insert data into sales_per_category table");

		// sales per day
		transactionDataStream.map(
						transaction -> {
							Date transactionDate = new Date(System.currentTimeMillis());
							double totalAmount = transaction.getTotalAmount();
							return new SalesPerDay(transactionDate, totalAmount);
						}
				).keyBy(SalesPerDay::getTransactionDate)
				.reduce((salesPerDay, value) -> {
					salesPerDay.setTotalAmount(salesPerDay.getTotalAmount() + value.getTotalAmount());
					return salesPerDay;
				}).addSink(JdbcSink.sink(
						"INSERT INTO sales_per_day (transaction_date, total_sales) VALUES (?, ?) "
								+ "ON CONFLICT (transaction_date) DO UPDATE SET total_sales = EXCLUDED.total_sales "
								+ "WHERE sales_per_day.transaction_date = EXCLUDED.transaction_date",
						(JdbcStatementBuilder<SalesPerDay>) (preparedStatement, salesPerDay) -> {
							preparedStatement.setDate(1, new Date(System.currentTimeMillis()));
							preparedStatement.setDouble(2, salesPerDay.getTotalAmount());
						},
						executionOptions,
						jdbcConnectionOptions
				)).name("JDBC Sink: Insert data into sales_per_day table");

		// sales per month
		transactionDataStream.map(
						transaction -> {
							Date transactionDate = new Date(System.currentTimeMillis());
							int month = transactionDate.toLocalDate().getMonthValue();
							int year = transactionDate.toLocalDate().getYear();
							double totalAmount = transaction.getTotalAmount();
							return new SalesPerMonth(month, year, totalAmount);
						}
				).keyBy(SalesPerMonth::getMonth)
				.reduce((salesPerMonth, value) -> {
					salesPerMonth.setTotalAmount(salesPerMonth.getTotalAmount() + value.getTotalAmount());
					return salesPerMonth;
				}).addSink(JdbcSink.sink(
						"INSERT INTO sales_per_month (year, month,  total_sales) VALUES (?, ?, ?) "
								+ "ON CONFLICT (year, month) DO UPDATE SET total_sales = EXCLUDED.total_sales "
								+ "WHERE sales_per_month.year = EXCLUDED.year AND sales_per_month.month = EXCLUDED.month",
						(JdbcStatementBuilder<SalesPerMonth>) (preparedStatement, salesPerMonth) -> {
							preparedStatement.setInt(1, salesPerMonth.getYear());
							preparedStatement.setInt(2, salesPerMonth.getMonth());
							preparedStatement.setDouble(3, salesPerMonth.getTotalAmount());
						},
						executionOptions,
						jdbcConnectionOptions
				)).name("JDBC Sink: Insert data into sales_per_month table");

		transactionDataStream.sinkTo(
				new Elasticsearch7SinkBuilder<Transaction>()
						.setHosts(new HttpHost("elasticsearch", 9200, "http"))
						.setBulkFlushMaxActions(1)
						.setEmitter((transaction, runtimeContext, requestIndexer) -> {

							String json = toJson(transaction);
							IndexRequest indexRequest = Requests.indexRequest()
									.index("transactions")
									.id(transaction.getTransactionId())
									.source(json, XContentType.JSON);
							requestIndexer.add(indexRequest);
						}).build()
		).name("Elasticsearch Sink: Insert data into transactions index");

		env.enableCheckpointing(5000);
		env.execute("Real time Flink Job");

	}
}