package group.ventis.utils;

import group.ventis.dto.Operation;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.auth.AuthScope;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.search.SearchRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.search.SearchResponse;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RequestOptions;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RestClient;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RestClientBuilder;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.RestHighLevelClient;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.index.query.QueryBuilders;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.search.builder.SearchSourceBuilder;

import java.time.Instant;

public class OperationEnricher extends RichMapFunction<Operation, Operation> {
   private transient RestHighLevelClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "btracking"));

        RestClientBuilder restClientBuilder = RestClient.builder(
                        new HttpHost("elasticsearch", 9200, "http"))
                .setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        client = new RestHighLevelClient(restClientBuilder);
    }

    @Override
    public Operation map(Operation operation) throws Exception {
        try {
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
            operation.setOperationCountLast24h(searchResponse.getHits().getTotalHits().value);
        } catch (Exception exception) {
            exception.printStackTrace();
            operation.setOperationCountLast24h(0);
        }
        return operation;
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }
}
