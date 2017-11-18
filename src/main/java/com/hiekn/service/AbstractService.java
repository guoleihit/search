package com.hiekn.service;

import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.SearchResultBean;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

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

    public abstract SearchResultBean doSearch(QueryRequest request) throws Exception;
}
