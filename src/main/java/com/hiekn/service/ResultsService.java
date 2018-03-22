package com.hiekn.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.CompositeQueryRequest;
import com.hiekn.search.bean.request.CompositeRequestItem;
import com.hiekn.search.bean.request.Operator;
import com.hiekn.search.bean.request.QueryRequestInternal;
import com.hiekn.search.bean.result.*;
import com.hiekn.search.exception.ServiceException;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.join.ScoreMode;
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
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.concurrent.Future;

import static com.hiekn.service.Helper.*;
import static com.hiekn.util.CommonResource.PAPER_INDEX;
import static com.hiekn.util.CommonResource.RESULTS_INDEX;

public class ResultsService extends AbstractService {

    private AbstractService paperService;

    public ResultsService(TransportClient esClient, IGeneralSSEService generalSSEService, String kgName) {
        this.esClient = esClient;
        this.generalSSEService = generalSSEService;
        this.kgName = kgName;
    }

    public ItemBean extractItem(SearchHit hit) {
        ResultsItem item = new ResultsItem();
        Map<String, Object> source = hit.getSource();

        doSetItemData(hit, item, source);

        //highlight
        if (hit.getHighlightFields() != null) {
            for (Map.Entry<String, HighlightField> entry : hit.getHighlightFields().entrySet()) {
                Text[] frags = entry.getValue().getFragments();
                switch (entry.getKey()) {
                    case "title":
                    case "title.smart":
                        if (frags != null && frags.length > 0 && frags[0].string().length()>1) {
                            item.setTitle(frags[0].string());
                        }
                        break;
                    case "abs":
                    case "abs.smart":
                        if (frags != null && frags.length > 0) {
                            item.setAbs(frags[0].string());
                        }
                        break;
                    case "complete_persons":
                        if (frags != null && frags.length > 0) {
                            ListIterator<String> itr = item.getAuthors().listIterator();
                            setHighlightElements(frags, itr);
                        }
                        break;
                    case "complete_department":
                        if (frags != null && frags.length > 0) {
                            List<String> values = Lists.newArrayList(item.getComplete_department());
                            ListIterator<String> itr = values.listIterator();
                            setHighlightElements(frags, itr);
                            item.setComplete_department(values);
                        }
                        break;
                }
            }
        }
        return item;
    }

    private void doSetItemData(SearchHit hit, ResultsItem item, Map<String, Object> source) {
        item.setDocId(hit.getId());
        item.setTitle(getString(source.get("title")));
        item.setComplete_department(toStringList(source.get("complete_department")));
        item.setOrigin(getString(source.get("origin")));
        item.setNo(getString(source.get("no")));
        item.setProvince_city(getString(source.get("province_city")));
        item.setTech_type(getString(source.get("type")));
        item.setAbs(getString(source.get("abs")));
        item.setAuthors(toStringList(source.get("complete_persons")));
        if(source.get("earliest_publication_date")!=null) {
            item.setPubDate(Helper.toDateString(source.get("earliest_publication_date").toString(), "-"));
        }
        if(StringUtils.isEmpty(item.getPubDate()) && source.get("year")!=null){
            item.setPubDate(getString(source.get("year")));
        }

        item.setDocType(DocType.RESULTS);
        item.setResultsType(getString(source.get("resultsType")));
    }

    public ItemBean extractDetail(SearchHit hit) {
        ResultsDetail item = new ResultsDetail();
        Map<String, Object> source = hit.getSource();
        doSetItemData(hit, item, source);
        item.setUrl(getString(source.get("url")));
        item.setAttribute(getString(source.get("attribute")));
        item.setAuthorized_department(getString(source.get("authorized_department")));
        item.setProject_number(getString(source.get("project_number")));
        item.setKeywords(toStringList(source.get("keywords")));
        item.setAward_department(getString(source.get("award_department")));
        item.setAuthorized_department(getString(source.get("authorized_department")));
        item.setReview_level(getString(source.get("review_level")));

        item.setPro_type(getString(source.get("pro_type"))); // 推广形式
        item.setProject_begin(getString(source.get("project_begin")));
        item.setProject_end(getString(source.get("project_end")));
        item.setInnovation_point(toStringList(source.get("innovation_point")));
        item.setProject_background(getString(source.get("project_background")));

        if (item.getInnovation_point() == null || item.getInnovation_point().isEmpty()) {
            item.setInnovation_point(new ArrayList<>());
            item.getInnovation_point().add(getString(source.get("innovation_point")));
        }
        if(source.get("related_patent") != null && source.get("related_patent") instanceof List) {
            List patents = new ArrayList();
            patents.addAll((List)source.get("related_patent"));
            item.setRelated_patent(patents);
            item.getRelated_patent().sort(getMapComparator("publish_date"));
        }
        if(source.get("related_standard") != null && source.get("related_standard") instanceof List) {
            List standards = new ArrayList();
            standards.addAll((List)source.get("related_standard"));
            item.setRelated_standard(standards);
            item.getRelated_standard().sort(getMapComparator("publish_date"));

        }
        if(source.get("related_scholar") != null && source.get("related_scholar") instanceof List) {
            item.setRelated_scholar((List)source.get("related_scholar"));
        }
        if(source.get("related_term") != null && source.get("related_term") instanceof List) {
            item.setRelated_term((List)source.get("related_term"));
        }
        //TODO 成果密级、获奖信息、鉴定负责人、经济效益、社会效益、推广应用情况

        return item;
    }

    @Override
    public SearchResultBean doCompositeSearch(CompositeQueryRequest request) throws Exception {
        QueryBuilder boolQuery = buildEnhancedQuery(request);
        if (boolQuery==null) {
            throw new ServiceException(Code.SEARCH_UNKNOWN_FIELD_ERROR.getCode());
        }
        SearchRequestBuilder srb = esClient.prepareSearch(RESULTS_INDEX);
        HighlightBuilder highlighter = new HighlightBuilder().field("title").field("abs")
                .field("abs.smart").field("title.smart")
                .field("keywords").field("complete_persons").field("complete_department");

        srb.highlighter(highlighter).setQuery(boolQuery).setFrom(request.getPageNo() - 1)
                .setSize(request.getPageSize());

        Helper.addSortByPubDate(request, srb);

        String annotationField = getAnnotationFieldName(request);
        if (annotationField != null) {
            AggregationBuilder knowledge = AggregationBuilders.terms("knowledge_class").field(annotationField);
            srb.addAggregation(knowledge);
        }

        // 发表时间
        AggregationBuilder aggPubYear = AggregationBuilders.histogram("publication_year")
                .field("earliest_publication_date").interval(10000).minDocCount(1).order(Histogram.Order.KEY_DESC);
        srb.addAggregation(aggPubYear);

        // 省市
        AggregationBuilder addressProvince =  AggregationBuilders.terms("address_province")
                .field("province_city").minDocCount(1);
        srb.addAggregation(addressProvince);

        // 成果水平 TODO
        AggregationBuilder resultsLevel =  AggregationBuilders.terms("results_level")
                .field("review_level").minDocCount(1);
        srb.addAggregation(resultsLevel);

        // 成果类型
        AggregationBuilder resultsType =  AggregationBuilders.terms("results_type")
                .field("resultsType").minDocCount(1);
        srb.addAggregation(resultsType);

        System.out.println(srb.toString());
        SearchResponse response =  srb.execute().get();
        SearchResultBean result = new SearchResultBean(request.getKw());
        result.setRsCount(response.getHits().totalHits);
        for (SearchHit hit : response.getHits()) {
            ItemBean item = extractItem(hit);
            result.getRsData().add(item);
        }

        String annotation = getAnnotationFieldName(request);
        setKnowledgeAggResult(request, response, result, annotation);

        Helper.setYearAggFilter(result,response,"publication_year", "发表年份","earliest_publication_date");
        Helper.setTermAggFilter(result,response,"address_province","所在省市","province_city");
        Helper.setTermAggFilter(result,response,"results_level","成果水平","results_level",
                Arrays.asList("特等奖","一等奖","二等奖","二等","二等奖;三等奖","三等奖","三等","中国电力科技三等奖","特别奖","特别"));
        Helper.setTermAggFilter(result,response,"results_type","成果类型","results_type");
        return result;
    }

    @Override
    public QueryBuilder buildQuery(QueryRequestInternal request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);

        if (!StringUtils.isEmpty(request.getId())) {
            boolQuery.must(QueryBuilders.nestedQuery("annotation_tag", QueryBuilders.termQuery("annotation_tag.id", Long.valueOf(request.getId())).boost(8f), ScoreMode.Max));
            boolQuery.filter(QueryBuilders.termQuery("_type", "results_data")).boost(3f);
            return boolQuery;
        }

        if (!StringUtils.isEmpty(request.getCustomQuery())) {
            BoolQueryBuilder query = buildCustomQuery(request);
            query.filter(QueryBuilders.termQuery("_type", "results_data"));
            boolQuery.should(query);
            if(StringUtils.isEmpty(request.getKw())){
                return boolQuery;
            }
        }

        String userInputPersonName = request.getRecognizedPerson();
        String userInputOrgName = request.getRecognizedOrg();

        BoolQueryBuilder boolTitleQuery = null;
        if (request.getUserSplitSegList()!=null && !request.getUserSplitSegList().isEmpty()) {
            boolTitleQuery = createSegmentsTermQuery(request, RESULTS_INDEX, "title");
        }

        QueryBuilder titleTerm = createTermsQuery("title", request.getUserSplitSegList(), 1f);
        QueryBuilder abstractTerm = createTermsQuery("abs", request.getUserSplitSegList(), 1f);
        QueryBuilder authorTerm = createTermsQuery("complete_persons", request.getUserSplitSegList(), 3f);
        QueryBuilder orgsTerm = createTermsQuery("complete_department", request.getUserSplitSegList(), 3f);
        QueryBuilder kwsTerm = createTermsQuery("keywords", request.getUserSplitSegList(), 3f);
        QueryBuilder annotationTagTerm = createNestedQuery("annotation_tag","annotation_tag.name", request.getUserSplitSegList(), 1f);

        BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        if (request.getKwType() == null || request.getKwType() == 0) {
            should(termQuery, titleTerm);
            should(termQuery,boolTitleQuery);
            should(termQuery,abstractTerm);
            should(termQuery,kwsTerm);
            should(termQuery,annotationTagTerm);
            if(userInputPersonName != null){
                should(termQuery, QueryBuilders.termQuery("complete_persons", userInputPersonName).boost(5f));
            }else{
                should(termQuery,authorTerm);
            }

            if(userInputOrgName != null){
                should(termQuery, QueryBuilders.termQuery("complete_department", userInputOrgName).boost(5f));
            }else{
                should(termQuery,orgsTerm);
            }
        } else if(request.getKwType() == 1){
            if (userInputPersonName != null) {
                BoolQueryBuilder personQuery = QueryBuilders.boolQuery();
                personQuery.must(QueryBuilders.termQuery("complete_persons", userInputPersonName).boost(6f));
                if (userInputOrgName != null) {
                    personQuery.should(QueryBuilders.termQuery("complete_department", userInputOrgName));
                }
                should(personQuery, annotationTagTerm);
                should(personQuery, titleTerm);
                should(personQuery, boolTitleQuery);
                should(personQuery, abstractTerm);
                should(personQuery, kwsTerm);
                should(termQuery, personQuery);
            }else {
                should(termQuery, authorTerm);
            }
        } else if (request.getKwType() == 2) {
            if (userInputOrgName != null) {
                BoolQueryBuilder orgQuery = QueryBuilders.boolQuery();
                orgQuery.must(QueryBuilders.termQuery("complete_department", userInputOrgName).boost(6f));
                if (userInputPersonName != null) {
                    orgQuery.should(QueryBuilders.termQuery("complete_persons", userInputPersonName));
                }
                should(orgQuery, annotationTagTerm);
                should(orgQuery, titleTerm);
                should(orgQuery, boolTitleQuery);
                should(orgQuery, abstractTerm);
                should(orgQuery, kwsTerm);
                should(termQuery, orgQuery);
            }else {
                should(termQuery, orgsTerm);
            }
        } else if (request.getKwType() == 3) {
            should(termQuery, titleTerm);
            should(termQuery, abstractTerm);
            should(termQuery, kwsTerm);
            should(termQuery, annotationTagTerm);
            should(termQuery, boolTitleQuery);
        }


        boolQuery.must(termQuery);
        boolQuery.filter(QueryBuilders.termQuery("_type", "results_data")).boost(2f);
        return boolQuery;
    }

    @Override
    public QueryBuilder buildEnhancedQuery(CompositeQueryRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);
        makeResultsFilter(request, boolQuery);

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

                if ("title".equals(key)) {
                    buildLongTextQueryCondition(boolQuery, reqItem, RESULTS_INDEX, "title", false, false, null);
                }else if ("abs".equals(key)) {
                    buildLongTextQueryCondition(boolQuery, reqItem, RESULTS_INDEX,"abs", false, false, null);
                }else if ("keyword".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "keywords", false,false);
                }else if ("author".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "complete_persons", false,false);
                }else if ("org".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "complete_department", false,false);
                }else if ("pubDate".equals(dateKey)) {
                    doBuildDateCondition(boolQuery, reqItem, "earliest_publication_date");
                }else if ("all".equals(key)) {
                    BoolQueryBuilder allQueryBuilder = makeFiledAllQueryBuilder(reqItem, Operator.OR);
                    setOperator(boolQuery, reqItem, allQueryBuilder);
                }else if (!StringUtils.isEmpty(key) || !StringUtils.isEmpty(dateKey)) {
                    // 搜索未知域，期望搜索本资源失败
                    if(Operator.AND.equals(reqItem.getOp()))
                        return null;
                }
            }
        }

        boolQuery.filter(QueryBuilders.termQuery("_type", "results_data")).boost(4f);
        return boolQuery;
    }

    private void makeResultsFilter(CompositeQueryRequest request, BoolQueryBuilder boolQuery) {
        if (request.getFilters() != null) {
            List<KVBean<String, List<String>>> filters = request.getFilters();
            for (KVBean<String, List<String>> filter : filters) {
                if ("address_province".equals(filter.getK())) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        filterQuery.should(QueryBuilders.termQuery(filter.getK(),v));
                    }
                    boolQuery.must(filterQuery);
                }else if ("results_level".equals(filter.getK())) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        //TODO 成果水平过滤
                        if ("其他".equals(v)){
                            filterQuery.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("review_level")));
                            filterQuery.should(QueryBuilders.termQuery("review_level",""));
                            filterQuery.should(QueryBuilders.termQuery("review_level","其他"));
                        }else {
                            filterQuery.should(QueryBuilders.termQuery("review_level", v));
                        }
                    }
                    boolQuery.must(filterQuery);
                }else if ("results_type".equals(filter.getK())) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        filterQuery.should(QueryBuilders.termQuery("resultsType",v));
                    }
                    boolQuery.must(filterQuery);
                }
            }
        }
    }

    @Override
    public void searchSimilarData(String docId, SearchResultBean result) throws Exception {
        ItemBean item = result.getRsData().get(0);
        if(!(item instanceof ResultsDetail)){
            return;
        }
        ResultsDetail detail = (ResultsDetail) item;

        List<Object> relatedPatent = new ArrayList<>();
        List<Object> relatedStandard = new ArrayList<>();
        List<Object> relatedScholar = new ArrayList<>();
        List<String> relatedKeyword = new ArrayList<>();
        if ("KEJI".equals(detail.getResultsType())) {
            relatedPatent = detail.getRelated_patent();
            relatedStandard = detail.getRelated_standard();
            relatedScholar = detail.getRelated_scholar();
            relatedKeyword = detail.getRelated_term();
        }else if("GUOWANG".equals(detail.getResultsType())) {

        }

        KVBean<String, List<Object>> similarPatents = new KVBean<>();
        similarPatents.setD("相似专利");
        similarPatents.setK("similarPatent");
        similarPatents.setV(new ArrayList<>());
        result.getSimilarData().add(similarPatents);
        similarPatents.getV().addAll(relatedPatent);

        KVBean<String, List<Object>> similarStandards = new KVBean<>();
        similarStandards.setD("相似标准");
        similarStandards.setK("similarStandard");
        similarStandards.setV(new ArrayList<>());
        result.getSimilarData().add(similarStandards);
        similarStandards.getV().addAll(relatedStandard);

        KVBean<String, List<Object>> similarAuthors = new KVBean<>();
        similarAuthors.setD("相关学者");
        similarAuthors.setK("related_authors");
        similarAuthors.setV(new ArrayList<>());
        result.getSimilarData().add(similarAuthors);
        similarAuthors.getV().addAll(relatedScholar);

        KVBean<String, List<Object>> similarKeywords = new KVBean<>();
        similarKeywords.setD("相关关键词");
        similarKeywords.setK("related_keywords");
        similarKeywords.setV(new ArrayList<>());
        result.getSimilarData().add(similarKeywords);
        similarKeywords.getV().addAll(relatedKeyword);


        Future<SearchResponse> similarPaperFuture = doSearchSimilarData("title.smart", detail.getTitle(), PAPER_INDEX);
        Future<SearchResponse> similarResultsFuture = doSearchSimilarData("title.smart", detail.getTitle(), RESULTS_INDEX);

        SearchResponse similarPaperResp = similarPaperFuture.get();
        if (similarPaperResp != null) {
            KVBean<String, List<Object>> similarPaper = new KVBean<>();
            similarPaper.setD("相似论文");
            similarPaper.setK("similarPapers");
            similarPaper.setV(new ArrayList<>());
            result.getSimilarData().add(similarPaper);
            for (SearchHit hit : similarPaperResp.getHits()) {
                ItemBean paperItem = paperService.extractItem(hit);
                if(docId.equals(paperItem.getDocId())){
                    continue;
                }
                similarPaper.getV().add(paperItem);
            }
            similarPaper.getV().sort(getItemBeanComparatorForPubDate());
        }

        SearchResponse similarResultsResp = similarResultsFuture.get();
        if (similarResultsResp != null) {
            KVBean<String, List<Object>> similarResults = new KVBean<>();
            similarResults.setD("相似成果");
            similarResults.setK("similarResults");
            similarResults.setV(new ArrayList<>());
            result.getSimilarData().add(similarResults);
            for (SearchHit hit : similarResultsResp.getHits()) {
                ItemBean resultsItem = extractItem(hit);
                if(docId.equals(resultsItem.getDocId())){
                    continue;
                }
                similarResults.getV().add(resultsItem);
            }
            similarResults.getV().sort(getItemBeanComparatorForPubDate());
        }
    }

    private Future<SearchResponse> doSearchSimilarData(String field, String value, String index) {
        QueryBuilder similarQuery = QueryBuilders.matchQuery(field,value);
        SearchRequestBuilder spq = esClient.prepareSearch(index);
        HighlightBuilder titleHighlighter = new HighlightBuilder().field(field);
        spq.highlighter(titleHighlighter).setQuery(similarQuery).setFrom(0).setSize(6);
        Future<SearchResponse> similarFuture = spq.execute();
        return similarFuture;
    }

    @Override
    BoolQueryBuilder makeFiledAllQueryBuilder(CompositeRequestItem reqItem, Operator op) {
        BoolQueryBuilder allQueryBuilder = QueryBuilders.boolQuery();

        buildLongTextQueryCondition(allQueryBuilder, reqItem, RESULTS_INDEX,"title", false,false, op);
        buildLongTextQueryCondition(allQueryBuilder, reqItem, RESULTS_INDEX,"abs", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "_kg_annotation_1.name", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "_kg_annotation_2.name", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "_kg_annotation_3.name", false,false, op);
        if (reqItem.getKv() != null && reqItem.getKv().getV() != null && reqItem.getKv().getV().size() > 0) {
            QueryBuilder annotation = createNestedQuery("annotation_tag", "annotation_tag.name", reqItem.getKv().getV(), 1f);
            setOperator(allQueryBuilder, op, annotation);
        }
        buildQueryCondition(allQueryBuilder, reqItem, "keywords", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "complete_persons", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "complete_department", false,false, op);
        return allQueryBuilder;
    }

    @Override
    public Map<String, String> formatCite(ItemBean bean, Integer format, List<String> customizedFields) throws Exception {
        String type = "[R]";
        String authors = Helper.toStringFromList(bean.getAuthors(),",");
        String pubDate = bean.getPubDate();
        StringBuilder citeBuilder = new StringBuilder();
        citeBuilder.append(authors).append(".").append(bean.getTitle());

        if (bean instanceof ResultsItem) {
            ResultsItem item = (ResultsItem)bean;
            citeBuilder.append(":").append(item.getNo()).append(type).append(".").append(pubDate);

            Map<String, String> results = new HashMap<>();
            results.put("cite",citeBuilder.toString());
            setCiteInfo(bean, format, customizedFields, authors, results);

            if (Integer.valueOf(3).equals(format) && customizedFields != null) {
                for (String field : customizedFields) {
                    if ("orgs".equals(field)) {
                        results.put("orgs", Helper.toStringFromList(item.getComplete_department(), ","));
                    }
                }
            }

            return results;
        }

        return Maps.newHashMap();
    }

    public AbstractService getPaperService() {
        return paperService;
    }

    public void setPaperService(AbstractService paperService) {
        this.paperService = paperService;
    }
}
