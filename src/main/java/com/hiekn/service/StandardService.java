package com.hiekn.service;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.alibaba.fastjson.JSONObject;
import com.hiekn.search.bean.request.CompositeRequestItem;
import com.hiekn.search.bean.request.Operator;
import com.hiekn.search.bean.request.StandardQueryRequest;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.SearchResultBean;
import com.hiekn.search.bean.result.StandardDetail;
import com.hiekn.search.bean.result.StandardItem;
import com.hiekn.util.CommonResource;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import static com.hiekn.service.Helper.*;
import static com.hiekn.service.Helper.setHighlightElements;

public class StandardService extends AbstractService{

    public StandardService (TransportClient client) {
        esClient = client;
    }

    public ItemBean extractItem(SearchHit hit) {
        StandardItem item = new StandardItem();
        Map<String, Object> source = hit.getSource();
        item.setDocId(hit.getId().toString());

        Object titleObj = source.get("name");
        if (titleObj != null) {
            item.setTitle(titleObj.toString());
        }
        Object absObj = source.get("abs");
        if (absObj != null) {
            item.setAbs(absObj.toString());
        }

        Object inventorsObj = source.get("persons");
        List<String> inventors = toStringList(inventorsObj);
        if (!inventors.isEmpty()) {
            item.setAuthors(inventors);
        }

        if (source.get("earliest_publication_date") != null) {
            item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
        }


        item.setYield(getString(source.get("yield")));

        Object quotes = source.get("quote");
        List<String> quoteList = toStringList(quotes);
        if (!quoteList.isEmpty()) {
            item.setQuote(quoteList);
        }

        Object terms = source.get("term");
        List<String> termList = toStringList(terms);
        if (!termList.isEmpty()) {
            item.setTerm(termList);
        }

        item.setPubDep(getString(source.get("pub_dep")));
        item.setTelephone(getString(source.get("telephone")));
        item.setAddress(getString(source.get("address")));
        item.setISBN(getString(source.get("ISBN")));
        item.setFormat(getString(source.get("format")));
        item.setBY(getString(source.get("BY")));
        item.setPrintNum(getString(source.get("print_num")));
        item.setWordNum(getString(source.get("word_num")));
        item.setPage(getString(source.get("page")));
        item.setPdfPage(getString(source.get("Pdf_page")));
        item.setPrice(getString(source.get("price")));
        item.setCcs(getString(source.get("css")));
        item.setSubNum(getString(source.get("sub_num")));
        item.setNum(getString(source.get("num")));
        item.setInterNum(getString(source.get("inter_num")));
        item.setInterName(getString(source.get("inter_name")));
        item.setYield(getString(source.get("yield")));



        //highlight
        if (hit.getHighlightFields() != null) {
            for (Map.Entry<String, HighlightField> entry : hit.getHighlightFields().entrySet()) {
                Text[] frags = entry.getValue().getFragments();
                switch (entry.getKey()) {
                    case "name":
                        if (frags != null && frags.length > 0) {
                            item.setTitle(frags[0].string());
                        }
                        break;
                    case "num":
                        if (frags != null && frags.length > 0) {
                            item.setNum(frags[0].string());
                        }
                        break;
                }
            }
        }
        return item;
    }


    public ItemBean extractDetail(SearchHit hit) {
        StandardDetail item = new StandardDetail();
        Map<String, Object> source = hit.getSource();
        item.setDocId(hit.getId().toString());

        Object titleObj = source.get("name");
        if (titleObj != null) {
            item.setTitle(titleObj.toString());
        }
        Object absObj = source.get("abs");
        if (absObj != null) {
            item.setAbs(absObj.toString());
        }

        Object inventorsObj = source.get("author");
        List<String> inventors = toStringList(inventorsObj);
        if (!inventors.isEmpty()) {
            item.setAuthors(inventors);
        }

        if (source.get("earliest_publication_date") != null) {
            item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
        }

        item.setType(getString(source.get("type")));
        item.setLanguage(getString(source.get("language")));
        item.setCcs(getString(source.get("ccs")));
        item.setIcs(getString(source.get("ics")));
        item.setNum(getString(source.get("num")));
        item.seteName(getString(source.get("e_name")));
        if (source.get("carryon_date")!=null) {
        		item.setCarryonDate(getString(toDateString(source.get("carryon_date").toString(), "-")));
        }
        item.setManageDep(getString(source.get("manage_dep")));
        item.setState(getString(source.get("state")));
        item.setAuthorDep(getString(source.get("author_dep")));
        item.setSubNum(getString(source.get("sub_num")));
        item.setInterNum(getString(source.get("inter_num")));
        item.setInterName(getString(source.get("inter_name")));
        item.setConsistent(getString(source.get("consistent")));
        item.setPubDep(getString(source.get("pub_dep")));
        item.setRelation(getString(source.get("relation")));
        
        return item;
    }

    public BoolQueryBuilder buildQuery(QueryRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);

        TermQueryBuilder titleTerm = QueryBuilders.termQuery("name", request.getKw()).boost(2);
        TermQueryBuilder abstractTerm = QueryBuilders.termQuery("abs", request.getKw());
        TermQueryBuilder authorTerm = QueryBuilders.termQuery("author.keyword", request.getKw()).boost(1.5f);
        TermQueryBuilder kwsTerm = QueryBuilders.termQuery("keywords", request.getKw()).boost(1.5f);

        BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        termQuery.should(titleTerm);
        termQuery.should(abstractTerm);
        termQuery.should(authorTerm);
        termQuery.should(kwsTerm);

        boolQuery.must(termQuery);
        boolQuery.filter(QueryBuilders.termQuery("_type", "standard_data"));
        return boolQuery;
    }

    public BoolQueryBuilder buildEnhancedQuery(StandardQueryRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if (request.getConditions()!=null && !request.getConditions().isEmpty()) {
            for (CompositeRequestItem reqItem: request.getConditions()) {
                String key = null;
                if(reqItem.getKv()!=null) {
                    key = reqItem.getKv().getK();
                }

                String dateKey = null;
                if(reqItem.getKvDate()!=null) {
                    dateKey = reqItem.getKvDate().getK();
                }


                if ("standardNum".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "num", false, false);
                }else if ("title".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "name", false,false);
                }else if ("all".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "num", false, false);
                    buildQueryCondition(boolQuery, reqItem, "name", false,false);
                    buildQueryCondition(boolQuery, reqItem, "num", true,true);
                }else if ("standardType".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "num", true,true);
                }else if ("pubDate".equals(dateKey)) {
                    doBuildDateCondition(boolQuery,reqItem, "earliest_publication_date");
                }
            }
        }
        return boolQuery;
    }

    @Override
    public SearchResultBean doSearch(QueryRequest request) throws ExecutionException, InterruptedException {
        BoolQueryBuilder boolQuery = buildEnhancedQuery((StandardQueryRequest) request);
        SearchRequestBuilder srb = esClient.prepareSearch(CommonResource.STANDARD_INDEX);
        HighlightBuilder highlighter = new HighlightBuilder().field("name");
        srb.highlighter(highlighter).setQuery(boolQuery).setFrom(request.getPageNo() - 1)
                .setSize(request.getPageSize());

        System.out.println(srb.toString());
        SearchResponse response =  srb.execute().get();
        SearchResultBean result = new SearchResultBean(request.getKw());
        result.setRsCount(response.getHits().totalHits);
        for (SearchHit hit : response.getHits()) {
            ItemBean item = extractItem(hit);
            result.getRsData().add(item);
        }
        return result;
    }
}
