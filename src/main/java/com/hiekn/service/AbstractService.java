package com.hiekn.service;

import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.CompositeQueryRequest;
import com.hiekn.search.bean.request.CompositeRequestItem;
import com.hiekn.search.bean.request.Operator;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.SearchResultBean;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class AbstractService {

    protected TransportClient esClient;

    public void makeFilters(QueryRequest request, BoolQueryBuilder boolQuery) {
        if (request.getFilters() != null) {
            System.out.println(request.getFilters());
            List<KVBean<String, List<String>>> filters = request.getFilters();
            for (KVBean<String, List<String>> filter : filters) {
                if ("earliest_publication_date".equals(filter.getK())) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        filterQuery.should(QueryBuilders.rangeQuery(filter.getK()).gt(Long.valueOf(v + "0000"))
                                .lt(Long.valueOf(v + "9999")));
                    }
                    boolQuery.must(filterQuery);
                } else if ("_type".equals(filter.getK()) || filter.getK().startsWith("annotation_")) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        filterQuery.should(QueryBuilders.termQuery(filter.getK(), v));
                    }
                    boolQuery.must(filterQuery);
                }
            }
        }
    }

    public abstract SearchResultBean doCompositeSearch(CompositeQueryRequest request) throws Exception;

    public abstract BoolQueryBuilder buildQuery(QueryRequest request);

    public abstract void searchSimilarData(String docId, SearchResultBean result) throws Exception;

    protected List<AnalyzeResponse.AnalyzeToken> esSegment(String kw, String index){
        //利用es分词
        IndicesAdminClient indicesClient = this.esClient.admin().indices();
        AnalyzeRequestBuilder arb = indicesClient.prepareAnalyze(kw);
        arb.setIndex(index);
        arb.setAnalyzer("ik_max_word");
        AnalyzeResponse response = arb.execute().actionGet();
        List<AnalyzeResponse.AnalyzeToken> segList = response.getTokens();
        return segList;
    }
    /**
     *
     * @param boolQuery
     * @param reqItem
     * @param esField
     * @param needPrefix 1=prefix
     */
    protected void buildQueryCondition(BoolQueryBuilder boolQuery, CompositeRequestItem reqItem, String esField, boolean needPrefix, boolean ignoreStrCase) {
        List<String> values = reqItem.getKv().getV();
        buildQueryCondition(boolQuery, reqItem, esField, needPrefix, ignoreStrCase, Arrays.asList(values.toArray()));
    }

    /**
     * Need provide values
     * @param boolQuery
     * @param reqItem
     * @param esField
     * @param needPrefix
     * @param ignoreStrCase
     * @param values
     */
    protected void buildQueryCondition(BoolQueryBuilder boolQuery, CompositeRequestItem reqItem, String esField, boolean needPrefix, boolean ignoreStrCase, List<Object> values) {
        BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        for (Object value : values) {
            if (ignoreStrCase) {
                String str = value.toString().toLowerCase();
                doBuildQueryCondition(reqItem, esField, needPrefix, termQuery, str);
                str = value.toString().toUpperCase();
                doBuildQueryCondition(reqItem, esField, needPrefix, termQuery, str);
            } else {
                doBuildQueryCondition(reqItem, esField, needPrefix, termQuery, value);
            }
        }
        if (Operator.OR.equals(reqItem.getOp()) || "all".equals(reqItem.getKv().getK())) {
            boolQuery.should(termQuery);
        } else if (Operator.AND.equals(reqItem.getOp())){
            boolQuery.must(termQuery);
        } else if (Operator.NOT.equals(reqItem.getOp())) {
            boolQuery.mustNot(termQuery);
        }
    }

    protected void setOperator(BoolQueryBuilder boolQuery, CompositeRequestItem reqItem, BoolQueryBuilder allQueryBuilder) {
        if (Operator.AND.equals(reqItem.getOp())) {
            boolQuery.must(allQueryBuilder);
        }else if (Operator.OR.equals(reqItem.getOp())) {
            boolQuery.should(allQueryBuilder);
        }else {
            boolQuery.mustNot(allQueryBuilder);
        }
    }

    protected void doBuildQueryCondition(CompositeRequestItem reqItem, String esField, boolean needPrefix, BoolQueryBuilder termQuery, Object value) {
        if (needPrefix) {
            termQuery.should(QueryBuilders.prefixQuery(esField, value.toString()));
        }else if (Integer.valueOf(1).equals(reqItem.getPrecision())) {
            termQuery.should(QueryBuilders.termQuery(esField, value));
        } else if (Integer.valueOf(2).equals(reqItem.getPrecision())){
            termQuery.should(QueryBuilders.wildcardQuery(esField, "*" + value + "*"));
        }
    }

    protected void doBuildDateCondition(BoolQueryBuilder boolQuery, CompositeRequestItem reqItem, String esField) {
        Map<String,String> dates = reqItem.getKvDate().getV();
        BoolQueryBuilder appDateQuery = QueryBuilders.boolQuery();
        for (Map.Entry<String,String> entry: dates.entrySet()) {
            if ("start".equalsIgnoreCase(entry.getKey())) {
                appDateQuery.must(QueryBuilders.rangeQuery(esField).gte(Helper.fromDateString(entry.getValue())));
            }else if ("end".equalsIgnoreCase(entry.getKey())) {
                appDateQuery.must(QueryBuilders.rangeQuery(esField).lte(Helper.fromDateString(entry.getValue())));
            }
        }

        if(Operator.AND.equals(reqItem.getOp())){
            boolQuery.must(appDateQuery);
        }else {
            boolQuery.should(appDateQuery);
        }
    }
}
