package com.hiekn.service;

import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.CompositeQueryRequest;
import com.hiekn.search.bean.request.CompositeRequestItem;
import com.hiekn.search.bean.request.Operator;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.*;
import com.hiekn.search.exception.ServiceException;
import com.hiekn.util.CommonResource;
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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.hiekn.service.Helper.*;
import static com.hiekn.util.CommonResource.PATENT_INDEX;

public class PatentService extends AbstractService {

    public PatentService(TransportClient client, IGeneralSSEService sse, String kgName) {
        esClient = client;
        generalSSEService = sse;
        this.kgName = kgName;

        patentTypeNameMap = new ConcurrentHashMap<>();
        patentTypeNameMap.put("1", "发明专利");
        patentTypeNameMap.put("2", "实用新型");
        patentTypeNameMap.put("3", "外观专利");

        patentNameTypeMap = new ConcurrentHashMap<>();
        patentNameTypeMap.put("发明专利", 1);
        patentNameTypeMap.put("实用新型", 2);
        patentNameTypeMap.put("外观专利", 3);
        
        
        legalStatusValueNameMap = new ConcurrentHashMap<>();
        legalStatusValueNameMap.put("1", "公开");
        legalStatusValueNameMap.put("2", "授权");
        legalStatusValueNameMap.put("3", "实效");

        legalStatusNameValueMap = new ConcurrentHashMap<>();
        legalStatusNameValueMap.put("公开", 1);
        legalStatusNameValueMap.put("授权", 2);
        legalStatusNameValueMap.put("实效", 3);
    }

    private Map<String, String> patentTypeNameMap;

    private Map<String, Integer> patentNameTypeMap;

    private Map<String, String> legalStatusValueNameMap;

    private Map<String, Integer> legalStatusNameValueMap;
    
    @SuppressWarnings("rawtypes")
    public ItemBean extractDetail(SearchHit hit) {
        PatentDetail item = new PatentDetail();

        Map<String, Object> source = hit.getSource();
        item.setDocId(hit.getId());

        Object titleObj = source.get("title");
        if (titleObj != null && titleObj instanceof Map) {
            item.setTitle(((Map) titleObj).get("original") != null ? ((Map) titleObj).get("original").toString() : "");
        }
        Object absObj = source.get("abstract");
        if (absObj != null && absObj instanceof Map) {
            item.setAbs(((Map) absObj).get("original") != null ? ((Map) absObj).get("original").toString() : "");
        }
        Object agenciesObj = source.get("agencies");
        List<String> agencies = toStringList(agenciesObj);
        if (!agencies.isEmpty()) {
            item.setAgencies(agencies);
        }

        Object agents = source.get("agents");
        List<String> agentList = toStringList(agents);
        if (!agentList.isEmpty()) {
            item.setAgents(agentList);
        }

        item.setGraphId(getString(source.get("kg_id")));
        Object countries = source.get("countries");
        List<String> countryList = toStringListByKey(countries,"countrie");
        if (!countryList.isEmpty()) {
            item.setCountries(countryList);
        }

        Object priorities = source.get("priorities");
        List<String> priorityList = toStringListByKey(priorities,"priority_number");
        if (!priorityList.isEmpty()) {
            item.setPriorities(priorityList);
        }

        if (source.get("legal_status") != null) {
            item.setLegalStatus(legalStatusValueNameMap.get(source.get("legal_status").toString()));
        }


        if (source.get("application_number") != null) {
            item.setApplicationNumber(source.get("application_number").toString());
        }
        if (source.get("publication_number") != null) {
            item.setPublicationNumber(source.get("publication_number").toString());
        }
        if (source.get("application_date") != null) {
            item.setApplicationDate(toDateString(source.get("application_date").toString(), "-"));
        }
        if (source.get("earliest_publication_date") != null) {
            item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
        }
        if (source.get("earliest_priority_date") != null) {
            item.setEarliestPrioritiesDate(toDateString(source.get("earliest_priority_date").toString(), "-"));
        }
        Object applicantsObj = source.get("applicants");

        List<Map<String, Object>> applicants = new ArrayList<>();
        if (applicantsObj != null && applicantsObj instanceof List) {
            for (Object applicant : (List) applicantsObj) {
                Map<String, Object> app = new HashMap<>();
                if (applicant == null) {
                    continue;
                }
                if (((Map) applicant).get("name") != null) {
                    Object nameObj = ((Map) applicant).get("name");
                    if (nameObj instanceof Map) {
                        app.put("name", ((Map) nameObj).get("original"));
                    } else {
                        app.put("name", nameObj);
                    }
                }
                if (((Map) applicant).get("address") != null) {
                    Object addressObj = ((Map) applicant).get("address");
                    if (addressObj instanceof Map) {
                        app.put("address", ((Map) addressObj).get("original"));
                    } else {
                        app.put("address", addressObj);
                    }
                }
                if (((Map) applicant).get("countries") != null) {
                    Object countriesObj = ((Map) applicant).get("countries");
                    if (countriesObj instanceof List) {
                        List<String> c = toStringListByKey(countriesObj, "countrie");
                        if(c!=null && !c.isEmpty()) {
                            if (item.getCountries() != null && !item.getCountries().isEmpty()) {
                                item.getCountries().addAll(c);
                            }else{
                                item.setCountries(c);
                            }
                        }
                    }
                }
                if (((Map) applicant).get("type") != null) {
                    app.put("type", ((Map) applicant).get("type"));
                }
                applicants.add(app);
            }
        }

        if (!applicants.isEmpty()) {
            item.setApplicants(applicants);
        }

        Object inventorsObj = source.get("inventors");
        List<String> inventors = getStringListFromNameOrgObject(inventorsObj);
        if (!inventors.isEmpty()) {
            item.setAuthors(inventors);
        }

        Object mainIpcObj = source.get("main_ipc");
        if (mainIpcObj != null && mainIpcObj instanceof Map) {
            item.setMainIPC(String.valueOf(((Map) mainIpcObj).get("ipc")));
        }

        Object ipcsObj = source.get("ipcs");
        List<String> ipcs = toStringListByKey(ipcsObj, "ipc");
        if (!ipcs.isEmpty()) {
            item.setIpces(ipcs);
        }
        try {
            if (source.get("fulltext_pages") != null) {
                item.setPages(Integer.valueOf(source.get("fulltext_pages").toString()));
            }
        } catch (Exception e) {
            item.setPages(0);
        }

        if (source.get("type") != null) {
            item.setType(patentTypeNameMap.get(source.get("type").toString()));
        }

        return item;
    }


    @SuppressWarnings("rawtypes")
    public ItemBean extractItem(SearchHit hit) {
        PatentItem item = new PatentItem();
        Map<String, Object> source = hit.getSource();
        // use application_number.lowercase as doc id for detail search
        item.setDocId(hit.getId());

        Object titleObj = source.get("title");
        if (titleObj != null && titleObj instanceof Map) {
            item.setTitle(((Map) titleObj).get("original") != null ? ((Map) titleObj).get("original").toString() : "");
        }
        Object absObj = source.get("abstract");
        if (absObj != null && absObj instanceof Map) {
            item.setAbs(((Map) absObj).get("original") != null ? ((Map) absObj).get("original").toString() : "");
        }

        Object mainIpcObj = source.get("main_ipc");
        if (mainIpcObj != null && mainIpcObj instanceof Map) {
            item.setMainIPC(String.valueOf(((Map) mainIpcObj).get("ipc")));
        }

        Object agenciesObj = source.get("agencies");
        List<String> agencies = toStringList(agenciesObj);
        if (!agencies.isEmpty()) {
            item.setAgencies(agencies);
        }

        Object agents = source.get("agents");
        List<String> agentList = toStringList(agents);
        if (!agentList.isEmpty()) {
            item.setAgents(agentList);
        }

        Object applicantsObj = source.get("applicants");
        List<String> applicants = getStringListFromNameOrgObject(applicantsObj);
        if (!applicants.isEmpty()) {
            item.setApplicants(applicants);
        }

        Object inventorsObj = source.get("inventors");
        List<String> inventors = getStringListFromNameOrgObject(inventorsObj);
        if (!inventors.isEmpty()) {
            item.setAuthors(inventors);
        }

        if (source.get("application_number") != null) {
            item.setApplicationNumber(source.get("application_number").toString());
        }
        if (source.get("publication_number") != null) {
            item.setPublicationNumber(source.get("publication_number").toString());
        }
        if (source.get("application_date") != null) {
            item.setApplicationDate(toDateString(source.get("application_date").toString(), "-"));
        }
        if (source.get("earliest_publication_date") != null) {
            item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
        }

        if (source.get("type") != null) {
            item.setType(patentTypeNameMap.get(source.get("type").toString()));
        }

        if (source.get("legal_status") != null) {
            item.setLegalStatus(legalStatusValueNameMap.get(source.get("legal_status").toString()));
        }

        //highlight
        if (hit.getHighlightFields() != null) {
            for (Map.Entry<String, HighlightField> entry : hit.getHighlightFields().entrySet()) {
                Text[] frags = entry.getValue().getFragments();
                switch (entry.getKey()) {
                    case "title.original":
                        if (frags != null && frags.length > 0) {
                            item.setTitle(frags[0].string());
                        }
                        break;
                    case "abstract.original":
                        if (frags != null && frags.length > 0) {
                            item.setAbs(frags[0].string());
                        }
                        break;
                    case "applicants.name.original.keyword":
                        if (frags != null && frags.length > 0) {
                            ListIterator<String> itr = item.getApplicants().listIterator();
                            setHighlightElements(frags, itr);
                        }
                        break;
                    case "inventors.name.original.keyword":
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

    public BoolQueryBuilder buildQuery(QueryRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);

        if (!StringUtils.isEmpty(request.getId())) {
            boolQuery.must(QueryBuilders.termQuery("annotation_tag.id", Long.valueOf(request.getId())).boost(8f));
            boolQuery.filter(QueryBuilders.termQuery("_type", "patent_data"));
            return boolQuery;
        }

        Map<String, String> result = intentionRecognition(request);
        String userInputPersonName = result.get("人物");
        String userInputOrgName = result.get("机构");

        BoolQueryBuilder boolTitleQuery = null;
        // 已经识别出人和机构，或者用户输入的不是人也不是机构
        if (request.getKwType() != 1 && request.getKwType() != 2 || userInputOrgName != null || userInputPersonName!=null) {
            boolTitleQuery = createSegmentsTermQuery(request, PATENT_INDEX, "title.original");
        }

        QueryBuilder titleTerm = createTermsQuery("title.original",request.getUserSplitSegList(), 4f);
        QueryBuilder abstractTerm = createTermsQuery("abstract.original", request.getUserSplitSegList(), 1f);
        QueryBuilder inventorTerm = createTermsQuery("inventors.name.original.keyword", request.getUserSplitSegList(), 4f);
        QueryBuilder applicantTerm = createTermsQuery("applicants.name.original.keyword", request.getUserSplitSegList(), 4f);
        QueryBuilder agenciesTerm = createTermsQuery("agencies_standerd.agency", request.getUserSplitSegList(),4f);
        QueryBuilder annotationTagTerm = createTermsQuery("annotation_tag.name", request.getUserSplitSegList(), 4f);

        BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        if (request.getKwType() == null || request.getKwType() == 0) {
            should(termQuery, titleTerm);
            if(boolTitleQuery!=null) {
                termQuery.should(boolTitleQuery);
            }
            should(termQuery, abstractTerm);
            should(termQuery, inventorTerm);
            should(termQuery, agenciesTerm);
            should(termQuery, applicantTerm);
            should(termQuery, annotationTagTerm);

            if(userInputOrgName != null){
                should(termQuery, QueryBuilders.termQuery("inventors.name.original.keyword", userInputPersonName).boost(5f));
            }else{
                should(termQuery,inventorTerm);
            }

            if(userInputPersonName != null){
                should(termQuery, QueryBuilders.termQuery("applicants.name.original.keyword", userInputOrgName).boost(5f));
            }else{
                should(termQuery,applicantTerm);
            }
        } else if (request.getKwType() == 1) {
            if (userInputPersonName != null) {
                BoolQueryBuilder personQuery = QueryBuilders.boolQuery();
                personQuery.must(QueryBuilders.termQuery("inventors.name.original.keyword", userInputPersonName).boost(6f));
                if (userInputOrgName != null) {
                    personQuery.should(QueryBuilders.termQuery("applicants.name.original.keyword", userInputOrgName));
                }
                should(personQuery, annotationTagTerm);
                should(personQuery, titleTerm);
                should(personQuery, boolTitleQuery);
                should(personQuery, abstractTerm);
                should(termQuery, personQuery);
            }else {
                should(termQuery, applicantTerm);
                should(termQuery, inventorTerm);
            }
        } else if (request.getKwType() == 2) {
            if (userInputOrgName != null) {
                BoolQueryBuilder orgQuery = QueryBuilders.boolQuery();
                orgQuery.must(QueryBuilders.termQuery("applicants.name.original.keyword", userInputOrgName).boost(6f));
                if (userInputPersonName != null) {
                    orgQuery.should(QueryBuilders.termQuery("inventors.name.original.keyword", userInputPersonName));
                }
                should(orgQuery, titleTerm);
                should(orgQuery, annotationTagTerm);
                should(orgQuery, boolTitleQuery);
                should(orgQuery, abstractTerm);
                termQuery.should(orgQuery);
            } else {
                should(termQuery, agenciesTerm);
                should(termQuery, applicantTerm);
            }
        } else if (request.getKwType() == 3) {
            should(termQuery,titleTerm);
            should(termQuery,abstractTerm);
            should(termQuery,annotationTagTerm);
            should(termQuery,boolTitleQuery);
        }

        boolQuery.must(termQuery);
        boolQuery.filter(QueryBuilders.termQuery("_type", "patent_data"));
        return boolQuery;

    }

    public BoolQueryBuilder buildEnhancedQuery(CompositeQueryRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makePatentFilter(request, boolQuery);
        makeFilters(request, boolQuery);

        if (request.getConditions()!=null && !request.getConditions().isEmpty()) {
            for (CompositeRequestItem reqItem: request.getConditions()) {
                String key = null;
                if(reqItem.getKv()!=null) {
                    key = reqItem.getKv().getK();
                    if (reqItem.getKv().getV() == null || reqItem.getKv().getV().isEmpty()) {
                        continue;
                    }
                }

                String dateKey = null;
                if(reqItem.getKvDate()!=null) {
                    dateKey = reqItem.getKvDate().getK();
                    if (reqItem.getKvDate().getV() == null || reqItem.getKvDate().getV().isEmpty()) {
                        continue;
                    }
                }

                if ("appNum".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "application_number.keyword", false, true);
                }else if ("pubNum".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "publication_number.keyword", false, true);
                } else if ("title".equals(key)) { // 待修改
                    buildLongTextQueryCondition(boolQuery, reqItem,  PATENT_INDEX,"title.original", false, false, null);
                }else if ("ipc".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "main_ipc.ipc.keyword", false,true);
                }else if ("legal_status".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "legal_status", false,false, toLegalStatus(reqItem.getKv().getV()));
                }else if ("author".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "inventors.name.original.keyword", false,false);
                }else if ("applicant".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "applicants.name.original.keyword", false,false);
                }else if ("abs".equals(key)) {
                    buildLongTextQueryCondition(boolQuery, reqItem, PATENT_INDEX,"abstract.original", false , false, null);
                }else if ("type".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "type", false,false, toPatentTypes(reqItem.getKv().getV()));
                }else if ("appDate".equals(dateKey)) {
                    doBuildDateCondition(boolQuery, reqItem, "application_date");
                }else if ("pubDate".equals(dateKey)) {
                    doBuildDateCondition(boolQuery, reqItem, "earliest_publication_date");
                }else if ("all".equals(key)) {
                    BoolQueryBuilder allQueryBuilder = QueryBuilders.boolQuery();
                    buildLongTextQueryCondition(allQueryBuilder, reqItem,PATENT_INDEX,"title.original", false,false, Operator.OR);
                    buildLongTextQueryCondition(allQueryBuilder, reqItem, PATENT_INDEX,"abstract.original", false,false, Operator.OR);
                    buildQueryCondition(allQueryBuilder, reqItem, "main_ipc.ipc.keyword", false,true, Operator.OR);
                    buildQueryCondition(allQueryBuilder, reqItem, "inventors.name.original.keyword", false,false, Operator.OR);
                    buildQueryCondition(allQueryBuilder, reqItem, "applicants.name.original.keyword", false,false, Operator.OR);
                    setOperator(boolQuery,reqItem,allQueryBuilder);
                }else if (!StringUtils.isEmpty(key) || !StringUtils.isEmpty(dateKey)) {
                    // 搜索未知域，期望搜索本资源失败
                    if(Operator.AND.equals(reqItem.getOp()))
                        return null;
                }
            }
        }

        boolQuery.filter(QueryBuilders.termQuery("_type", "patent_data")).boost(2f);
        return boolQuery;

    }

    private void makePatentFilter(QueryRequest request, BoolQueryBuilder boolQuery) {
        if (request.getFilters() != null) {
            List<KVBean<String, List<String>>> filters = request.getFilters();
            for (KVBean<String, List<String>> filter : filters) {
                if ("type".equals(filter.getK())) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        filterQuery.should(QueryBuilders.termQuery(filter.getK(), patentNameTypeMap.get(v)));
                    }
                    boolQuery.must(filterQuery);
                } else if ("main_ipc".equals(filter.getK())) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        filterQuery.should(QueryBuilders.termQuery("main_ipc.ipc.keyword",v));
                    }
                    boolQuery.must(filterQuery);
                } else if (filter.getK()!=null && (filter.getK().startsWith("ipc_")||filter.getK().startsWith("address_"))) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        filterQuery.should(QueryBuilders.termQuery(filter.getK(),v));
                    }
                    boolQuery.must(filterQuery);
                } else if ("application_date".equals(filter.getK())) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        filterQuery.should(QueryBuilders.rangeQuery(filter.getK()).gt(Long.valueOf(v + "0000"))
                                .lt(Long.valueOf(v + "9999")));
                    }
                    boolQuery.must(filterQuery);
                } else if ("legal_status".equals(filter.getK())) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        filterQuery.should(QueryBuilders.termQuery(filter.getK(), legalStatusNameValueMap.get(v)));
                    }
                    boolQuery.must(filterQuery);
                }
            }
        }
    }

    @Override
    public SearchResultBean doCompositeSearch(CompositeQueryRequest request) throws ExecutionException, InterruptedException {
        BoolQueryBuilder boolQuery = buildEnhancedQuery(request);
        if (boolQuery==null) {
            throw new ServiceException(Code.SEARCH_UNKNOWN_FIELD_ERROR.getCode());
        }
        SearchRequestBuilder srb = esClient.prepareSearch(CommonResource.PATENT_INDEX);

        HighlightBuilder highlighter = new HighlightBuilder().field("title.original")
                .field("abstract.original")
                .field("applicants.name.original.keyword")
                .field("inventors.name.original.keyword");

        srb.highlighter(highlighter).setQuery(boolQuery).setFrom(request.getPageNo() - 1)
                .setSize(request.getPageSize());

        // 专利类型
        AggregationBuilder docTypes = AggregationBuilders.terms("patent_type").field("type");
        srb.addAggregation(docTypes);

        // IPC
        String ipcField = getIPCFieldName(request);
        if (ipcField != null) {
            AggregationBuilder ipcs = AggregationBuilders.terms(ipcField).field(ipcField);
            srb.addAggregation(ipcs);
        }

        // 公开时间
        AggregationBuilder aggPubYear = AggregationBuilders.histogram("publication_year")
                .field("earliest_publication_date").interval(10000).minDocCount(1).order(Histogram.Order.KEY_DESC);
        srb.addAggregation(aggPubYear);

        // 申请时间
        AggregationBuilder aggAppYear = AggregationBuilders.histogram("application_year")
                .field("application_date").interval(10000).minDocCount(1).order(Histogram.Order.KEY_DESC);
        srb.addAggregation(aggAppYear);

        // 省市
        AggregationBuilder addressProvince =  AggregationBuilders.terms("address_province")
                .field("applicants.address.original_province").minDocCount(1);
        srb.addAggregation(addressProvince);

        // 法律状态
        AggregationBuilder legalStatusAgg =  AggregationBuilders.terms("legal_status")
                .field("legal_status").minDocCount(1);
        srb.addAggregation(legalStatusAgg);

        AggregationBuilder ipcs = AggregationBuilders.terms("main_ipc").field("main_ipc.ipc.keyword");
        srb.addAggregation(ipcs);

        // 排序
        if (Integer.valueOf(1).equals(request.getSort())) {
            srb.addSort(SortBuilders.fieldSort("earliest_publication_date").order(SortOrder.DESC));
        }else if (Integer.valueOf(2).equals(request.getSort())) {
        		srb.addSort(SortBuilders.fieldSort("application_date").order(SortOrder.DESC));
        }

        System.out.println(srb.toString());
        SearchResponse response = srb.execute().get();
        SearchResultBean result = new SearchResultBean(request.getKw());
        result.setRsCount(response.getHits().totalHits);
        for (SearchHit hit : response.getHits()) {
            ItemBean item = extractItem(hit);
            result.getRsData().add(item);
        }

        Terms patentTypes = response.getAggregations().get("patent_type");
        KVBean<String, Map<String, ?>> patentTypeFilter = new KVBean<>();
        patentTypeFilter.setD("专利类型");
        patentTypeFilter.setK("type");
        Map<String, Long> patentMap = new HashMap<>();
        for (Terms.Bucket bucket : patentTypes.getBuckets()) {
            String key = patentTypeNameMap.get(bucket.getKeyAsString());
            if (key != null) {
                patentMap.put(key, bucket.getDocCount());
            }
        }
        patentMap.put("_end", -1l);
        patentTypeFilter.setV(patentMap);
        result.getFilters().add(patentTypeFilter);

        Terms legalStatuses = response.getAggregations().get("legal_status");
        KVBean<String, Map<String, ?>> legalStatusFilter = new KVBean<>();
        legalStatusFilter.setD("法律状态");
        legalStatusFilter.setK("legal_status");
        Map<String, Long> legalMap = new HashMap<>();
        for (Terms.Bucket bucket : legalStatuses.getBuckets()) {
            String key = legalStatusValueNameMap.get(bucket.getKeyAsString());
            if (key != null) {
                legalMap.put(key, bucket.getDocCount());
            }
        }
        legalMap.put("_end", -1l);
        legalStatusFilter.setV(legalMap);
        result.getFilters().add(legalStatusFilter);


        String ipcName = getIPCFieldName(request);
        if(ipcName != null) {
            Terms mainIpcTypes = response.getAggregations().get(ipcName);
            KVBean<String, Map<String, ?>> mainIpcTypeFilter = new KVBean<>();
            mainIpcTypeFilter.setD("IPC分类");
            mainIpcTypeFilter.setK(ipcName);
            Map<String, Long> ipcMap = new HashMap<>();
            for (Terms.Bucket bucket : mainIpcTypes.getBuckets()) {
                ipcMap.put(bucket.getKeyAsString(), bucket.getDocCount());
            }
            if(ipcName.indexOf("3")>0) {
                ipcMap.put("_end", -1l);
            }
            mainIpcTypeFilter.setV(ipcMap);
            result.getFilters().add(mainIpcTypeFilter);
        }

        Helper.setYearAggFilter(result,response,"publication_year", "发表年份","earliest_publication_date");
        Helper.setYearAggFilter(result,response,"application_year", "申请时间","application_date");
        Helper.setTermAggFilter(result,response,"address_province","所在省市","applicants.address.original_province");

        Terms mainIpcTypes = response.getAggregations().get("main_ipc");
        KVBean<String, Map<String, ?>> mainIpcTypeFilter = new KVBean<>();
        mainIpcTypeFilter.setD("主IPC分类");
        mainIpcTypeFilter.setK("main_ipc");
        Map<String, Long> ipcMap = new HashMap<>();
        for (Terms.Bucket bucket : mainIpcTypes.getBuckets()) {
            ipcMap.put(bucket.getKeyAsString(), bucket.getDocCount());
        }
        ipcMap.put("_end", -1l);
        mainIpcTypeFilter.setV(ipcMap);
        result.getFilters().add(mainIpcTypeFilter);

        return result;
    }


    public void searchSimilarData(String docId, SearchResultBean result) throws InterruptedException, ExecutionException {
        String title = result.getRsData().get(0).getTitle();
        QueryBuilder similarPatentsQuery = QueryBuilders.matchQuery("title.original",title).analyzer("ik_max_word");
        SearchRequestBuilder spq = esClient.prepareSearch(PATENT_INDEX);
        HighlightBuilder titleHighlighter = new HighlightBuilder().field("title.original");
        spq.highlighter(titleHighlighter).setQuery(similarPatentsQuery).setFrom(0).setSize(6);
        Future<SearchResponse> similarPatentFuture = spq.execute();

        List<String> inventors = result.getRsData().get(0).getAuthors();
        QueryBuilder inventorsPatentsQuery = QueryBuilders.termsQuery("inventors.name.original.keyword",inventors);
        HighlightBuilder inventorHighlighter = new HighlightBuilder().field("inventors.name.original.keyword");
        SearchRequestBuilder ipq = esClient.prepareSearch(PATENT_INDEX);
        ipq.highlighter(inventorHighlighter).setQuery(inventorsPatentsQuery).setFrom(0).setSize(6);
        Future<SearchResponse>  inventorsPatentFuture = ipq.execute();
        //System.out.println(ipq.toString());

        List<String> applicants = ((PatentDetail)result.getRsData().get(0)).getApplicants().stream().map((app)->{
            return app.get("name")==null?"":app.get("name").toString();
        }).collect(Collectors.toList());
        QueryBuilder applicantPatentsQuery = QueryBuilders.termsQuery("applicants.name.original.keyword",applicants);
        HighlightBuilder applicantsHighlighter = new HighlightBuilder().field("applicants.name.original.keyword");
        SearchRequestBuilder apq = esClient.prepareSearch(PATENT_INDEX);
        apq.highlighter(applicantsHighlighter).setQuery(applicantPatentsQuery).setFrom(0).setSize(6);
        Future<SearchResponse> appPatentFuture = apq.execute();
        //System.out.println(apq.toString());

        SearchResponse similarPatentResp, inventorsPatentResp, appPatentResp = null;
        if((similarPatentResp = similarPatentFuture.get())!=null
                && (inventorsPatentResp = inventorsPatentFuture.get())!=null
                && (appPatentResp = appPatentFuture.get())!=null){

            KVBean<String, List<Object>> similarPatents = new KVBean<>();
            similarPatents.setD("相似专利");
            similarPatents.setK("similarPatents");
            similarPatents.setV(new ArrayList<>());
            result.getSimilarData().add(similarPatents);
            for (SearchHit hit : similarPatentResp.getHits()) {
                ItemBean item = extractItem(hit);
                if(docId.equals(item.getDocId())){
                    continue;
                }
                similarPatents.getV().add(item);
            }

            KVBean<String, List<Object>> inventorsPatents = new KVBean<>();
            inventorsPatents.setD("发明人专利");
            inventorsPatents.setK("inventorsPatents");
            inventorsPatents.setV(new ArrayList<>());
            result.getSimilarData().add(inventorsPatents);
            for (SearchHit hit : inventorsPatentResp.getHits()) {
                ItemBean item = extractItem(hit);
                if(docId.equals(item.getDocId())){
                    continue;
                }
                inventorsPatents.getV().add(item);
            }

            KVBean<String, List<Object>> applicantsPatents = new KVBean<>();
            applicantsPatents.setD("申请人专利");
            applicantsPatents.setK("applicantsPatents");
            applicantsPatents.setV(new ArrayList<>());
            result.getSimilarData().add(applicantsPatents);
            for (SearchHit hit : appPatentResp.getHits()) {
                ItemBean item = extractItem(hit);
                if(docId.equals(item.getDocId())){
                    continue;
                }
                applicantsPatents.getV().add(item);
            }
        }
    }

    private List<Object> toPatentTypes(List<String> names){
        List<Integer> types = new ArrayList<>();
        for (String name: names) {
            Integer type = this.patentNameTypeMap.get(name);
            if (type != null)
            types.add(type);
        }
        return Arrays.asList(types.toArray());
    }

    private List<Object> toLegalStatus(List<String> names){
        List<Integer> types = new ArrayList<>();
        for (String name: names) {
            Integer type = this.legalStatusNameValueMap.get(name);
            if (type != null)
                types.add(type);
        }
        return Arrays.asList(types.toArray());
    }

    public static String getIPCFieldName(QueryRequest request) {
        String annotationField = "ipc_1";
        if (request.getFilters() != null) {
            for (KVBean<String, List<String>> filter : request.getFilters()) {
                if ("ipc_1".equals(filter.getK())) {
                    annotationField = "ipc_2";
                } else if ("ipc_2".equals(filter.getK())) {
                    annotationField = "ipc_3";
                }else if ("ipc_3".equals(filter.getK())) {
                    return null;
                }
            }
        }
        return annotationField;
    }
}
