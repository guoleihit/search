package com.hiekn.service;

import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.CompositeQueryRequest;
import com.hiekn.search.bean.request.CompositeRequestItem;
import com.hiekn.search.bean.request.Operator;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.*;
import com.hiekn.search.exception.ServiceException;
import org.apache.commons.lang3.StringUtils;
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

import static com.hiekn.service.Helper.*;
import static com.hiekn.service.Helper.setHighlightElements;
import static com.hiekn.util.CommonResource.RESULTS_INDEX;

public class ResultsService extends AbstractService {

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
                        if (frags != null && frags.length > 0 && frags[0].string().length()>1) {
                            item.setTitle(frags[0].string());
                        }
                        break;
                    case "abs":
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
                            item.setComplete_department(frags[0].string());
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
        item.setComplete_department(getString(source.get("complete_department")));
        item.setFrom(getString(source.get("from")));
        item.setNo(getString(source.get("no")));
        item.setProvince_city(getString(source.get("province_city")));
        item.setTech_type(getString(source.get("type")));
        item.setAbs(getString(source.get("abs")));
        item.setAuthors(toStringList(source.get("complete_persons")));
        if(source.get("earliest_publication_date")!=null) {
            item.setPubDate(Helper.toDateString(source.get("earliest_publication_date").toString(), "-"));
        }

        item.setDocType(DocType.RESULTS);
    }

    public ItemBean extractDetail(SearchHit hit) {
        ResultsDetail item = new ResultsDetail();
        Map<String, Object> source = hit.getSource();
        doSetItemData(hit, item, source);

        item.setAttribute(getString(source.get("attribute")));
        item.setAuthorized_department(getString(source.get("authorized_department")));
        item.setProject_number(getString(source.get("project_number")));
        item.setKeywords(toStringList(source.get("keywords")));
        item.setAward_department(getString(source.get("award_department")));
        item.setAuthorized_department(getString(source.get("authorized_department")));
        item.setReview_level(getString(source.get("review_level")));

        item.setPro_type(getString(source.get("pro_type"))); // 推广形式
        //TODO 成果密级、获奖信息、鉴定负责人、经济效益、社会效益、推广应用情况

        return item;
    }

    @Override
    public SearchResultBean doCompositeSearch(CompositeQueryRequest request) throws Exception {
        BoolQueryBuilder boolQuery = buildEnhancedQuery(request);
        if (boolQuery==null) {
            throw new ServiceException(Code.SEARCH_UNKNOWN_FIELD_ERROR.getCode());
        }
        SearchRequestBuilder srb = esClient.prepareSearch(RESULTS_INDEX);
        HighlightBuilder highlighter = new HighlightBuilder().field("title").field("abs")
                .field("keywords").field("complete_persons");

        srb.highlighter(highlighter).setQuery(boolQuery).setFrom(request.getPageNo() - 1)
                .setSize(request.getPageSize());

        if (Integer.valueOf(1).equals(request.getSort())) {
            srb.addSort(SortBuilders.fieldSort("earliest_publication_date").order(SortOrder.DESC));
        }

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
                .field("type").minDocCount(1);
        srb.addAggregation(resultsLevel);

        System.out.println(srb.toString());
        SearchResponse response =  srb.execute().get();
        SearchResultBean result = new SearchResultBean(request.getKw());
        result.setRsCount(response.getHits().totalHits);
        for (SearchHit hit : response.getHits()) {
            ItemBean item = extractItem(hit);
            result.getRsData().add(item);
        }

        String annotation = getAnnotationFieldName(request);
        setKnowledgeAggResult(response, result, annotation);

        Helper.setYearAggFilter(result,response,"publication_year", "发表年份","earliest_publication_date");
        Helper.setTermAggFilter(result,response,"address_province","所在省市","province_city");
        Helper.setTermAggFilter(result,response,"results_level","成果水平","results_level");
        return result;
    }

    @Override
    public BoolQueryBuilder buildQuery(QueryRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);

        if (!StringUtils.isEmpty(request.getId())) {
            boolQuery.must(QueryBuilders.termQuery("annotation_tag.id", Long.valueOf(request.getId())).boost(8f));
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

        Map<String, String> result = intentionRecognition(request);
        String userInputPersonName = result.get("人物");
        String userInputOrgName = result.get("机构");

        BoolQueryBuilder boolTitleQuery = null;
        // 已经识别出人和机构，或者用户输入的不是人也不是机构
        if (request.getKwType() != 1 && request.getKwType() != 2 || userInputOrgName != null || userInputPersonName!=null) {
            boolTitleQuery = createSegmentsTermQuery(request, RESULTS_INDEX, "title");
        }

        QueryBuilder titleTerm = createTermsQuery("title", request.getUserSplitSegList(), 1f);
        QueryBuilder abstractTerm = createTermsQuery("abs", request.getUserSplitSegList(), 1f);
        QueryBuilder authorTerm = createTermsQuery("complete_persons", request.getUserSplitSegList(), 3f);
        QueryBuilder orgsTerm = createTermsQuery("complete_department", request.getUserSplitSegList(), 3f);
        QueryBuilder kwsTerm = createTermsQuery("keywords", request.getUserSplitSegList(), 3f);
        QueryBuilder annotationTagTerm = createTermsQuery("annotation_tag.name", request.getUserSplitSegList(), 3f);

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
    public BoolQueryBuilder buildEnhancedQuery(CompositeQueryRequest request) {
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
                        filterQuery.should(QueryBuilders.termQuery("type",v));
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
        //TODO

        KVBean<String, List<Object>> similarPatents = new KVBean<>();
        similarPatents.setD("相似专利");
        similarPatents.setK("similarPatent");
        similarPatents.setV(new ArrayList<>());
        result.getSimilarData().add(similarPatents);
        //TODO
        similarPatents.getV().add(fakePatent());

        KVBean<String, List<Object>> similarStandards = new KVBean<>();
        similarStandards.setD("相似标准");
        similarStandards.setK("similarStandard");
        similarStandards.setV(new ArrayList<>());
        result.getSimilarData().add(similarStandards);
        //TODO
        similarStandards.getV().add(fakeStandard());

        KVBean<String, List<Object>> similarAuthors = new KVBean<>();
        similarAuthors.setD("相关学者");
        similarAuthors.setK("related_authors");
        similarAuthors.setV(new ArrayList<>());
        result.getSimilarData().add(similarAuthors);
        similarAuthors.getV().addAll(Arrays.asList("谢开","姚远"));

        KVBean<String, List<Object>> similarKeywords = new KVBean<>();
        similarKeywords.setD("相关关键词");
        similarKeywords.setK("related_keywords");
        similarKeywords.setV(new ArrayList<>());
        result.getSimilarData().add(similarKeywords);
        similarKeywords.getV().addAll(Arrays.asList("直流系统","维护"));

    }

    @Override
    BoolQueryBuilder makeFiledAllQueryBuilder(CompositeRequestItem reqItem, Operator op) {
        BoolQueryBuilder allQueryBuilder = QueryBuilders.boolQuery();

        buildLongTextQueryCondition(allQueryBuilder, reqItem, RESULTS_INDEX,"title", false,false, op);
        buildLongTextQueryCondition(allQueryBuilder, reqItem, RESULTS_INDEX,"abs", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "_kg_annotation_1.name", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "_kg_annotation_2.name", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "_kg_annotation_3.name", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "annotation_tag.name", false,true, op);
        buildQueryCondition(allQueryBuilder, reqItem, "keywords", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "complete_persons", false,false, op);

        return allQueryBuilder;
    }

    private ItemBean fakePatent(){
        PatentItem item = new PatentItem();
        item.setDocId("cn201610460900");
        item.setTitle("一种交直流混联电网");
        item.setAbs("本实用新型涉及电力设备技术领域，具体涉及一种交直流混联电网，包括若干个变电站，所述变电站中至少有一个为柔性变电站，每个变电站包括高压交流系统、高压直流系统、低压交流系统、低压直流系统中的部分或全部，每个柔性变电站中的高压交流系统、高压直流系统、低压交流系统、低压直流系统分别互联成网，本技术方案对现在技术中的配电网的功能做进一步完善，达到了配电网在提供高压交流电和低压交流电的同时又能提供高压直流电和低压直流电的目的，解决现有配电网只可提供模式单一的交流电、无法直接提供直流电的问题，实现配电网不同电压等级交直流系统之间的柔性互联，潮流灵活调节和故障的快速隔离。");
        item.setAuthors(Arrays.asList(
                "滕乐天",
                "邓占锋",
                "谢开",
                "赵国亮",
                "王志凯",
                "刘海军",
                "才志远"));
        item.setPubDate("2017-4-5");
        item.setAgencies(Arrays.asList("北京三聚阳光知识产权代理有限公司"));
        item.setApplicants(Arrays.asList("全球能源互联网研究院","国家电网公司"));
        item.setApplicationDate("2016-7-22");
        item.setApplicationNumber("CN201620786365.6");
        item.setPublicationNumber("CN206076972U");
        item.setAgents(Arrays.asList("马永芬"));
        item.setMainIPC("h02j5/00");
        item.setType("实用新型");
        item.setLegalStatus("授权");
        item.setDocType(DocType.PATENT);
        return item;
    }

    private ItemBean fakeStandard(){
        StandardItem item = new StandardItem();
        item.setDocId("AV_r1P9nzdSYo0sa9p4o");
        item.setTitle("电工术语  电力牵引");
        item.setPubDate("2003-5-26");
        item.setDocType(DocType.STANDARD);
        item.setNum("GB/T 2900.36—2003");
        item.setSubNum("GB 3367.10—1984； GB 3367.9—1984； GB 2900.36—1996");
        item.setCarryonDate("2003-10-1");
        return item;
    }
}
