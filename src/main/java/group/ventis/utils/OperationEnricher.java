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
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.index.query.BoolQueryBuilder;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.index.query.QueryBuilders;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.search.aggregations.AggregationBuilders;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.search.aggregations.Aggregations;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.search.aggregations.metrics.Sum;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.search.aggregations.metrics.ValueCount;
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
            long past1min = now - 60 * 1000;
            long past5min = now - 5 * 60 * 1000;
            long past10min = now - 10 * 60 * 1000;
            long past1h = now - 60 * 60 * 1000;
            long past24h = now - 24 * 60 * 60 * 1000;
            long past1month = now - 30L * 24 * 60 * 60 * 1000;
            long past2month = now - 2 * 30L * 24 * 60 * 60 * 1000;
            long past3month = now - 3 * 30L * 24 * 60 * 60 * 1000;
            long past6month = now - 6 * 30L * 24 * 60 * 60 * 1000;

            long[] periods = {past1min, past5min, past10min, past1h, past24h, past1month, past2month, past3month, past6month};
            String[] periodNames = {"1m", "5m", "10m", "1h", "24h", "1Month", "2Months", "3Months", "6Months"};

            long[] totalCounts = new long[periods.length];
            long[] creditCounts = new long[periods.length];
            long[] debitCounts = new long[periods.length];

            for (int i = 0; i < periods.length; i++) {

                if ("DEBIT".equals(operation.getSign())) {
                    long startTime = periods[i];

                    // Construire la requête Elasticsearch
                    SearchRequest sumRequest = new SearchRequest("raw_operations");
                    SearchSourceBuilder sumSourceBuilder = new SearchSourceBuilder();
                    sumSourceBuilder.query(QueryBuilders.boolQuery()
                            .filter(QueryBuilders.termQuery("accountNumber.keyword", operation.getAccountNumber()))
                            .filter(QueryBuilders.termQuery("sign.keyword", "DEBIT"))
                            .filter(QueryBuilders.rangeQuery("date").gte(startTime).lte(now))
                    );
                    sumSourceBuilder.aggregation(AggregationBuilders.sum("sum_debit_"+ periodNames[i]).field("montant"));
                    sumRequest.source(sumSourceBuilder);

                    // Exécuter la requête
                    SearchResponse sumResponse = client.search(sumRequest, RequestOptions.DEFAULT);
                    Aggregations aggregations = sumResponse.getAggregations();

                    double debitSum = 0.0;
                    if (sumResponse.getHits().getTotalHits().value > 0) {
                        Sum sum = aggregations.get("sum_debit");
                        debitSum = sum != null ? sum.getValue() : 0.0;

                        // Inclure la transaction actuelle si elle est un débit
                        debitSum += operation.getMontant();
                    } else {
                        debitSum = operation.getMontant();
                    }
                    // Assigner la somme calculée à l'objet Operation
                    switch (periodNames[i]) {
                        case "1m":
                            operation.setDebitSumLast1Min(debitSum);
                            break;
                        case "5m":
                            operation.setDebitSumLast5Min(debitSum);
                            break;
                        case "10m":
                            operation.setDebitSumLast10Min(debitSum);
                            break;
                        case "1h":
                            operation.setDebitSumLast1h(debitSum);
                            break;
                        case "24h":
                            operation.setDebitSumLast24h(debitSum);
                            break;
                        case "1Month":
                            operation.setDebitSumLast1Month(debitSum);
                            break;
                        case "2Months":
                            operation.setDebitSumLast2Month(debitSum);
                            break;
                        case "3Months":
                            operation.setDebitSumLast3Month(debitSum);
                            break;
                        case "6Months":
                            operation.setDebitSumLast6Month(debitSum);
                            break;

                    }
                }

                // Requête pour le nombre total d'opérations
                SearchRequest searchRequestTotal = new SearchRequest("raw_operations");
                SearchSourceBuilder searchSourceBuilderTotal = new SearchSourceBuilder();
                searchSourceBuilderTotal.query(QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery("accountNumber.keyword", operation.getAccountNumber()))
                        .filter(QueryBuilders.rangeQuery("date").gte(periods[i]).lte(now))
                );
                searchSourceBuilderTotal.aggregation(AggregationBuilders.count("operation_count_" + periodNames[i]).field("id.keyword"));
                searchRequestTotal.source(searchSourceBuilderTotal);

                // Requête pour le nombre d'opérations de débit
                SearchRequest searchRequestDebit = new SearchRequest("raw_operations");
                SearchSourceBuilder searchSourceBuilderDebit = new SearchSourceBuilder();
                searchSourceBuilderDebit.query(QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery("accountNumber.keyword", operation.getAccountNumber()))
                        .filter(QueryBuilders.termQuery("sign.keyword", "DEBIT"))
                        .filter(QueryBuilders.rangeQuery("date").gte(periods[i]).lte(now))
                );
                searchSourceBuilderDebit.aggregation(AggregationBuilders.count("debit_operation_count_" + periodNames[i]).field("id.keyword"));
                searchRequestDebit.source(searchSourceBuilderDebit);

                // Requête pour le nombre d'opérations de crédit
                SearchRequest searchRequestCredit = new SearchRequest("raw_operations");
                SearchSourceBuilder searchSourceBuilderCredit = new SearchSourceBuilder();
                searchSourceBuilderCredit.query(QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery("accountNumber.keyword", operation.getAccountNumber()))
                        .filter(QueryBuilders.termQuery("sign.keyword", "CREDIT"))
                        .filter(QueryBuilders.rangeQuery("date").gte(periods[i]).lte(now))
                );
                searchSourceBuilderCredit.aggregation(AggregationBuilders.count("credit_operation_count_" + periodNames[i]).field("id.keyword"));
                searchRequestCredit.source(searchSourceBuilderCredit);

                // Exécution des requêtes et récupération des résultats
                SearchResponse responseTotal = client.search(searchRequestTotal, RequestOptions.DEFAULT);
                SearchResponse responseDebit = client.search(searchRequestDebit, RequestOptions.DEFAULT);
                SearchResponse responseCredit = client.search(searchRequestCredit, RequestOptions.DEFAULT);

                long totalOperations = ((ValueCount) responseTotal.getAggregations().get("operation_count_" + periodNames[i])).getValue();
                long debitOperations = ((ValueCount) responseDebit.getAggregations().get("debit_operation_count_" + periodNames[i])).getValue();
                long creditOperations = ((ValueCount) responseCredit.getAggregations().get("credit_operation_count_" + periodNames[i])).getValue();

                // Mise à jour des totaux en incluant l'opération actuelle
                if ("DEBIT".equals(operation.getSign())) {
                    totalOperations += 1; // Inclure l'opération actuelle
                    debitOperations += 1; // Inclure l'opération actuelle
                } else if ("CREDIT".equals(operation.getSign())) {
                    totalOperations += 1; // Inclure l'opération actuelle
                    creditOperations += 1; // Inclure l'opération actuelle
                }

                // Mise à jour des attributs de l'objet Operation
                switch (periodNames[i]) {
                    case "1m":
                        operation.setOperationsCountLast1Min(totalOperations);
                        operation.setDebitCountLast1Min(debitOperations);
                        operation.setCreditCountLast1Min(creditOperations);
                        break;
                    case "5m":
                        operation.setOperationsCountLast5Min(totalOperations);
                        operation.setDebitCountLast5Min(debitOperations);
                        operation.setCreditCountLast5Min(creditOperations);
                        break;
                    case "10m":
                        operation.setOperationsCountLast10Min(totalOperations);
                        operation.setDebitCountLast10Min(debitOperations);
                        operation.setCreditCountLast10Min(creditOperations);
                        break;
                    case "1h":
                        operation.setOperationCountLast1h(totalOperations);
                        operation.setDebitCountLast1h(debitOperations);
                        operation.setCreditCountLast1h(creditOperations);
                        break;
                    case "24h":
                        operation.setOperationCountLast24h(totalOperations);
                        operation.setDebitCountLast24h(debitOperations);
                        operation.setCreditCountLast24h(creditOperations);
                        break;
                    case "1Month":
                        operation.setOperationCountLast1Month(totalOperations);
                        operation.setDebitCountLast1Month(debitOperations);
                        operation.setCreditCountLast1Month(creditOperations);
                        break;
                    case "2Months":
                        operation.setOperationCountLast2Month(totalOperations);
                        operation.setDebitCountLast2Month(debitOperations);
                        operation.setCreditCountLast2Month(creditOperations);
                        break;
                    case "3Months":
                        operation.setOperationCountLast3Month(totalOperations);
                        operation.setDebitCountLast3Month(debitOperations);
                        operation.setCreditCountLast3Month(creditOperations);
                        break;
                    case "6Months":
                        operation.setOperationCountLast6Month(totalOperations);
                        operation.setDebitCountLast6Month(debitOperations);
                        operation.setCreditCountLast6Month(creditOperations);
                        break;
                }
            }


            /*

            // Requête pour calculer la somme des débits au cours des 24h
            SearchRequest sumRequest = new SearchRequest("raw_operations");
            SearchSourceBuilder sumSourceBuilder = new SearchSourceBuilder();
            sumSourceBuilder.query(QueryBuilders.boolQuery()
                    .filter(QueryBuilders.termQuery("accountNumber.keyword", operation.getAccountNumber()))
                    .filter(QueryBuilders.termQuery("sign.keyword", "DEBIT"))
                    .filter(QueryBuilders.rangeQuery("date").gte(past24h).lte(now))
            );
            sumSourceBuilder.aggregation(AggregationBuilders.sum("sum_debit").field("montant"));
            sumRequest.source(sumSourceBuilder);

            SearchResponse sumResponse = client.search(sumRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = sumResponse.getAggregations();
            if (sumResponse.getHits().getTotalHits().value > 0) {
                Sum sum = aggregations.get("sum_debit");
                double debitSum = sum != null ? sum.getValue() : 0.0;

                if ("DEBIT".equals(operation.getSign())) {
                    debitSum += operation.getMontant();
                }
                operation.setDebitSumLast24h(debitSum);
                System.out.println("Sum of DEBIT for account " + operation.getAccountNumber() + " in the last 24 hours: " + debitSum);
            } else {
                operation.setDebitSumLast24h(operation.getSign().equals("DEBIT") ? operation.getMontant() : 0.0);
            }
            */

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
