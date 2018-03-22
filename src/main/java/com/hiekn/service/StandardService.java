package com.hiekn.service;

import com.google.common.collect.Maps;
import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.*;
import com.hiekn.search.bean.result.*;
import com.hiekn.search.exception.ServiceException;
import com.hiekn.util.CommonResource;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.hiekn.service.Helper.*;
import static com.hiekn.util.CommonResource.STANDARD_INDEX;

public class StandardService extends AbstractService{

    public StandardService (TransportClient client, IGeneralSSEService sse, String kgName) {
        esClient = client;
        generalSSEService = sse;
        this.kgName = kgName;
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

        Object inventorsObj = source.get("draft_person");
        List<String> inventors = toStringList(inventorsObj);
        if (!inventors.isEmpty()) {
            item.setAuthors(inventors);
        }

        if (source.get("earliest_publication_date") != null) {
            item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
        }

        if (source.get("carryon_date")!=null) {
            item.setCarryonDate(getString(toDateString(source.get("carryon_date").toString(), "-")));
        }

        item.setYield(getString(source.get("yield")));

        Object quotes = source.get("quote");
        if (quotes instanceof List) {
            item.setQuote((List)quotes);
        }

        Object terms = source.get("terms");
        if (terms instanceof List) {
            item.setTerm((List)terms);
        }

        item.setPubDep(getString(source.get("pub_dep")));
        item.setTelephone(getString(source.get("telephone")));
        item.setAddress(getString(source.get("address")));
        item.setISBN(getString(source.get("isbn")));
        item.setFormat(getString(source.get("format")));
        item.setEdition(getString(source.get("edition")));
        item.setPrintNum(getString(source.get("print_num")));
        item.setWordNum(getString(source.get("word_num")));
        item.setPage(getString(source.get("page")));
        item.setPdfPage(getString(source.get("pdf_page")));
        item.setPrice(getString(source.get("price")));
        item.setCcs(getString(source.get("css")));
        item.setSubNum(getString(source.get("sub_num")));
        item.setNum(getString(source.get("num")));
        item.setInterNum(getString(source.get("inter_num")));
        item.setInterName(getString(source.get("inter_name")));
        item.setYield(getString(source.get("yield")));
        item.setState(getString(source.get("stdstate")));


        //highlight
        if (hit.getHighlightFields() != null) {
            for (Map.Entry<String, HighlightField> entry : hit.getHighlightFields().entrySet()) {
                Text[] frags = entry.getValue().getFragments();
                switch (entry.getKey()) {
                    case "name":
                    case "name.smart":
                        if (frags != null && frags.length > 0) {
                            item.setTitle(frags[0].string());
                        }
                        break;
                    case "yield":
                        if (frags != null && frags.length > 0) {
                            for (Text frag: frags) {
                                String fragStr = frag.string();
                                String noEmStr = fragStr.replaceAll("<em>", "");
                                noEmStr = noEmStr.replaceAll("</em>", "");
                                String abs = item.getYield();
                                abs = abs.replace(noEmStr, fragStr);
                                item.setYield(abs);
                            }
                        }
                        break;
                    case "draft_person":
                        if (frags != null && frags.length > 0) {
                            ListIterator<String> itr = item.getAuthors().listIterator();
                            setHighlightElements(frags, itr);
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

        item.setGraphId(getString(source.get("kg_id")));
        Object inventorsObj = source.get("draft_person");
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
        item.seteName(getString(source.get("en_name")));
        if (source.get("carryon_date")!=null) {
        		item.setCarryonDate(getString(toDateString(source.get("carryon_date").toString(), "-")));
        }
        item.setManageDep(getString(source.get("manage_dep")));
        item.setState(getString(source.get("stdstate")));
        item.setAuthorDep(getString(source.get("author_dep")));
        item.setSubNum(getString(source.get("sub_num")));
        item.setInterNum(getString(source.get("inter_num")));
        item.setInterName(getString(source.get("inter_name")));
        item.setConsistent(getString(source.get("consistent")));
        item.setPubDep(getString(source.get("pub_dep")));
        item.setRelation(getString(source.get("relation")));
        
        return item;
    }

    public QueryBuilder buildQuery(QueryRequestInternal request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);

        if (!StringUtils.isEmpty(request.getId())) {
            boolQuery.must(QueryBuilders.termQuery("annotation_tag.id", Long.valueOf(request.getId())));
            boolQuery.filter(QueryBuilders.termQuery("_type", "standard_data")).boost(3f);
            return boolQuery;
        }

        if (!StringUtils.isEmpty(request.getCustomQuery())) {
            BoolQueryBuilder query = buildCustomQuery(request);
            query.filter(QueryBuilders.termQuery("_type", "standard_data"));
            boolQuery.should(query);
            if(StringUtils.isEmpty(request.getKw())){
                return boolQuery;
            }
        }

        BoolQueryBuilder boolTitleQuery = null;
        if (request.getUserSplitSegList()!=null && !request.getUserSplitSegList().isEmpty()) {
            boolTitleQuery = createSegmentsTermQuery(request, STANDARD_INDEX, "name");
        }

        QueryBuilder titleTerm = createTermsQuery("name", request.getUserSplitSegList(), 2f);
        QueryBuilder yieldTerm = createTermsQuery("yield", request.getUserSplitSegList(), 1);
        QueryBuilder titleSmartTerm = createTermsQuery("name.smart", request.getUserSplitSegList(), 2f);
        QueryBuilder yieldSmartTerm = createTermsQuery("yield.smart", request.getUserSplitSegList(), 1);

        QueryBuilder authorTerm = createTermsQuery("draft_person", request.getUserSplitSegList(),1.5f);
        QueryBuilder kwsTerm = createTermsQuery("terms.term", request.getUserSplitSegList(),1.5f);

        BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        should(termQuery,titleTerm);
        should(termQuery,yieldTerm);
        should(termQuery,titleSmartTerm);
        should(termQuery,yieldSmartTerm);
        should(termQuery,authorTerm);
        should(termQuery,kwsTerm);
        should(termQuery,boolTitleQuery);
        if (request.getRecognizedPerson() != null) {
            should(termQuery, QueryBuilders.termQuery("draft_person", request.getRecognizedPerson()).boost(1.5f));
        }
        if (request.getRecognizedOrg() != null) { // TODO
            should(termQuery, QueryBuilders.termQuery("author_dep", request.getRecognizedOrg()).boost(1.5f));
        }

        boolQuery.must(termQuery);
        boolQuery.filter(QueryBuilders.termQuery("_type", "standard_data"));
        return boolQuery;
    }

    @Override
    public void searchSimilarData(String docId, SearchResultBean result) throws Exception {

    }

    public QueryBuilder buildEnhancedQuery(CompositeQueryRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeStandardFilters(request, boolQuery);
        makeFilters(request, boolQuery);
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
                    buildLongTextQueryCondition(boolQuery, reqItem, STANDARD_INDEX,"name", false,false ,null);
                }else if ("all".equals(key)) {
                    BoolQueryBuilder allQueryBuilder = makeFiledAllQueryBuilder(reqItem, Operator.OR);
                    setOperator(boolQuery,reqItem, allQueryBuilder);
                }else if ("author".equals(key)){
                    buildQueryCondition(boolQuery, reqItem, "draft_person", false, false);
                }else if ("authorOrg".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "author_dep", false, false);
                }else if ("status".equals(key)) {
                    // TODO 标准状态: implement,publish,annull
                    buildQueryCondition(boolQuery, reqItem, "stdstate", false, false);
                }else if ("term".equals(key)) {
                    // TODO 标准术语
                    buildQueryCondition(boolQuery, reqItem, "terms.term", false, false);
                }else if ("standardType".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "type", true,true);
                }else if ("pubDate".equals(dateKey)) {
                    doBuildDateCondition(boolQuery,reqItem, "earliest_publication_date");
                }else if (!StringUtils.isEmpty(key) || !StringUtils.isEmpty(dateKey)) {
                    // 搜索未知域，期望搜索本资源失败
                    if(Operator.AND.equals(reqItem.getOp()))
                        return null;
                }
            }
        }
        boolQuery.filter(QueryBuilders.termQuery("_type", "standard_data"));
        return boolQuery;
    }

    private void makeStandardFilters(CompositeQueryRequest request, BoolQueryBuilder boolQuery) {
        if (request.getFilters() != null) {
            System.out.println(request.getFilters());
            List<KVBean<String, List<String>>> filters = request.getFilters();
            for (KVBean<String, List<String>> filter : filters) {
                if ("base_class".equals(filter.getK()) || filter.getK().startsWith("class_")) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        filterQuery.should(QueryBuilders.termQuery(filter.getK(), v));
                    }
                    boolQuery.must(filterQuery);
                }else if ("status".equals(filter.getK()) ) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        if ("其他".equals(v)){
                            filterQuery.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("stdstate")));
                            filterQuery.should(QueryBuilders.termQuery("stdstate",""));
                            filterQuery.should(QueryBuilders.termQuery("stdstate","其他"));
                        }else {
                            filterQuery.should(QueryBuilders.termQuery("stdstate", v));
                        }
                    }
                    boolQuery.must(filterQuery);
                }
            }
        }
    }

    @Override
    public SearchResultBean doCompositeSearch(CompositeQueryRequest request) throws ExecutionException, InterruptedException {
        QueryBuilder boolQuery = buildEnhancedQuery(request);
        if (boolQuery==null) {
            throw new ServiceException(Code.SEARCH_UNKNOWN_FIELD_ERROR.getCode());
        }
        SearchRequestBuilder srb = esClient.prepareSearch(CommonResource.STANDARD_INDEX);
        HighlightBuilder highlighter = new HighlightBuilder().field("name");
        srb.highlighter(highlighter).setQuery(boolQuery).setFrom(request.getPageNo() - 1)
                .setSize(request.getPageSize());

        boolean noClassFound = true;
        if (request.getFilters() != null) {
            for (KVBean<String, List<String>> filter : request.getFilters()) {
                if (filter.getK()!=null && (filter.getK().startsWith("base_")) || (filter.getK().startsWith("class_"))) {
                    noClassFound = false;
                    break;
                }
            }
        }

        // 标准基础分类
        AggregationBuilder baseClasses = AggregationBuilders.terms("base_class").field("base_class");
        srb.addAggregation(baseClasses);

        // 标准技术分类
        String className = getClassFieldName(request);
        if (!StringUtils.isEmpty(className)) {
            AggregationBuilder classes = AggregationBuilders.terms(className).field(className);
            srb.addAggregation(classes);
        }

        // 发表时间
        AggregationBuilder aggPubYear = AggregationBuilders.histogram("publication_year")
                .field("earliest_publication_date").interval(10000).minDocCount(1).order(Histogram.Order.KEY_DESC);
        srb.addAggregation(aggPubYear);


        // 标准状态
        AggregationBuilder status = AggregationBuilders.terms("status").field("stdstate").missing("其他");
        srb.addAggregation(status);

        Helper.addSortByPubDate(request, srb);

        System.out.println(srb.toString());
        SearchResponse response =  srb.execute().get();
        SearchResultBean result = new SearchResultBean(request.getKw());
        result.setRsCount(response.getHits().totalHits);
        for (SearchHit hit : response.getHits()) {
            ItemBean item = extractItem(hit);
            result.getRsData().add(item);
        }

        Terms baseClassAggs = response.getAggregations().get("base_class");
        KVBean<String, Map<String, ?>> baseClassFilter = new KVBean<>();
        baseClassFilter.setD("标准技术基础分类");
        baseClassFilter.setK("base_class");
        Map<String, Long> baseClassValueMap = new HashMap<>();
        for (Terms.Bucket bucket : baseClassAggs.getBuckets()) {
            baseClassValueMap.put(bucket.getKeyAsString(), bucket.getDocCount());
        }
        baseClassValueMap.put("_end",-1l);
        baseClassFilter.setV(baseClassValueMap);
        result.getFilters().add(baseClassFilter);

        if (!StringUtils.isEmpty(className)) {
            Terms classAggs = response.getAggregations().get(className);
            KVBean<String, Map<String, ?>> classFilter = new KVBean<>();
            classFilter.setD("标准技术专业分类");
            classFilter.setK(className);
            Map<String, Long> classValueMap = new HashMap<>();
            for (Terms.Bucket bucket : classAggs.getBuckets()) {
                classValueMap.put(bucket.getKeyAsString(), bucket.getDocCount());
            }
            if(className.indexOf("2")>0){
                classValueMap.put("_end",-1l);
            }
            classFilter.setV(classValueMap);
            result.getFilters().add(classFilter);
        }

        Helper.setYearAggFilter(result,response,"publication_year", "发表年份","earliest_publication_date");
        Helper.setTermAggFilter(result,response,"status", "专利状态","stdstate");
        return result;
    }

    public static String getClassFieldName(QueryRequest request) {
        String annotationField = "class_1";
        int level = 1;
        if (request.getFilters() != null) {
            for (KVBean<String, List<String>> filter : request.getFilters()) {
                if ("class_1".equals(filter.getK())) {
                    annotationField = "class_2";
                } else if ("class_2".equals(filter.getK())) {
                    return null;
                }
            }
        }
        return annotationField;
    }

    @Override
    BoolQueryBuilder makeFiledAllQueryBuilder(CompositeRequestItem reqItem, Operator op) {
        BoolQueryBuilder allQueryBuilder = QueryBuilders.boolQuery();
        buildQueryCondition(allQueryBuilder, reqItem, "num", false, false, op);
        //buildQueryCondition(allQueryBuilder, reqItem, "num", false, false, op);
        buildLongTextQueryCondition(allQueryBuilder, reqItem, STANDARD_INDEX,"name", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "num", true,true, op);
        buildQueryCondition(allQueryBuilder, reqItem, "draft_person", false, false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "terms.term", false, false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "author_dep", false, false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "type", false, false, op);
        return allQueryBuilder;
    }

    @Override
    public Map<String, String> formatCite(ItemBean bean, Integer format, List<String> customizedFields) throws Exception {
        String type = "[S]";
        String pubDate = bean.getPubDate();
        StringBuilder citeBuilder = new StringBuilder();
        citeBuilder.append(bean.getTitle());

        if (bean instanceof StandardItem) {
            StandardItem item = (StandardItem)bean;
            citeBuilder.append(":").append(item.getNum()).append(type).append(".");
            if (!StringUtils.isEmpty(pubDate)) {
                citeBuilder.append("[").append(pubDate).append("]");
            }

            Map<String, String> results = new HashMap<>();
            results.put("cite",citeBuilder.toString());
            results.put("title", item.getTitle());
            results.put("docType", DocType.STANDARD.getName());
            results.put("docId", item.getDocId());
            return results;
        }

        return Maps.newHashMap();
    }
}
