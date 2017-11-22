package com.hiekn.search.rest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.google.gson.reflect.TypeToken;
import com.hiekn.plantdata.bean.graph.SchemaBean;
import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.prompt.PromptBean;
import com.hiekn.search.bean.request.PatentQueryRequest;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.request.StandardQueryRequest;
import com.hiekn.search.bean.result.*;
import com.hiekn.search.exception.BaseException;
import com.hiekn.service.*;
import com.hiekn.util.JSONUtils;
import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import javax.annotation.Resource;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hiekn.service.Helper.getString;
import static com.hiekn.util.CommonResource.*;

@Controller
@Path("/p")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
@Api(tags = {"搜索"})
public class SearchRestApi implements InitializingBean {

    @Value("${kg_name}")
    private String kgName;
    @Resource
    private IGeneralSSEService generalSSEService;

    private static Logger log = LoggerFactory.getLogger(SearchRestApi.class);

    @Resource
    private TransportClient esClient;

    private PaperService paperService = null;
    private PatentService patentService = null;
    private PictureService pictureService = null;
    private StandardService standardService = null;
    private BaikeService baikeService = new BaikeService();

    @GET
    @Path("/detail")
    @ApiOperation(value = "")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<SearchResultBean> detail(@QueryParam("docId") String docId, @QueryParam("docType") DocType docType,
                                             @QueryParam("tt") Long tt) throws Exception {
        log.info("docId=" + docId + ",docType" + docType);
        if (StringUtils.isEmpty(docId)) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }
        SearchResultBean result = new SearchResultBean(docId);
        BoolQueryBuilder docQuery = buildQueryDetail(docId);
        String index = PATENT_INDEX;
        if (DocType.PICTURE.equals(docType)) {
            index = PICTURE_INDEX;
        } else if (DocType.PAPER.equals(docType)) {
            index = PAPER_INDEX;
        } else if (DocType.STANDARD.equals(docType)) {
            index = STANDARD_INDEX;
        }
        SearchRequestBuilder srb = esClient.prepareSearch(index);
        System.out.println(srb.toString());
        srb.setQuery(docQuery).setFrom(0).setSize(1);
        SearchResponse docResp = srb.get();
        if (docResp.getHits().getHits().length > 0) {
            SearchHit hit = docResp.getHits().getAt(0);
            ItemBean item = extractDetail(hit, docType);
            result.getRsData().add(item);
        }
        return new RestResp<>(result, tt);
    }

    private ItemBean extractDetail(SearchHit hit, DocType docType) {
        switch (docType) {
            case PICTURE:
                return pictureService.extractDetail(hit);
            case PAPER:
                return paperService.extractDetail(hit);
            case STANDARD:
                return standardService.extractDetail(hit);
            case PATENT:
            default:
                return patentService.extractDetail(hit);
        }
    }

    @GET
    @Path("/baike")
    @ApiOperation(value = "搜索百科")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<SearchResultBean> baike(@QueryParam("baike") String baike, @QueryParam("pageNo") Integer pageNo,
                                            @QueryParam("pageSize") Integer pageSize, @QueryParam("tt") Long tt) throws Exception {
        log.info("search baike item:" + baike);
        if (pageNo == null) {
            pageNo = 1;
        }
        if (pageSize == null) {
            pageSize = 10;
        }
        SearchResultBean result = new SearchResultBean(baike);

        BoolQueryBuilder baikeQuery = baikeService.buildQuery(baike);
        QueryRequest request = new QueryRequest();
        request.setKw(baike);
        request.setPageNo(pageNo);
        request.setPageSize(pageSize);
        SearchResponse baikeResp = searchBaikeIndex(request, baikeQuery);
        if (baikeResp.getHits().getHits().length > 0) {
            SearchHit hit = baikeResp.getHits().getAt(0);
            BaikeItem item = baikeService.extractItem(hit);
            result.getRsData().add(item);
        }
        return new RestResp<>(result, request.getTt());
    }

    @POST
    @Path("/kw")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "搜索", notes = "搜索过滤及排序")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<SearchResultBean> kw(@ApiParam(value = "检索请求") QueryRequest request)
            throws InterruptedException, ExecutionException {
        if (StringUtils.isEmpty(request.getKw()) && StringUtils.isEmpty(request.getAndKwList())
                && StringUtils.isEmpty(request.getAtLeastOneKw())
                && StringUtils.isEmpty(request.getExactKwList())
                && StringUtils.isEmpty(request.getNoneKwList())) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }
        log.info(com.hiekn.util.JSONUtils.toJson(request));

        SearchResultBean result = new SearchResultBean(request.getKw());
        String[] kws = request.getKw().trim().split(" ");
        if (kws.length > 1) {
            request.setKw(kws[0]);
            request.setOtherKw(kws[1]);
        }

        List<String> indices = new ArrayList<>();
        List<AbstractService> services = new ArrayList<>();
        if (request.getDocType() == null && (request.getDocTypeList() == null || request.getDocTypeList().isEmpty())) {
            services.addAll(Arrays.asList(new AbstractService[]{patentService, paperService, standardService, pictureService}));
            indices.addAll(Arrays.asList(new String[]{PAPER_INDEX, PATENT_INDEX, STANDARD_INDEX, PICTURE_INDEX}));
        } else if(request.getDocType()!=null) {
            setSearchResource(request.getDocType(),services,indices);
        }else if (request.getDocTypeList() != null && !request.getDocTypeList().isEmpty()) {
            for (DocType doc: request.getDocTypeList()) {
                setSearchResource(doc,services,indices);
            }
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        for (AbstractService service: services) {
            boolQuery.should(service.buildQuery(request));
        }
        SearchResponse response = searchIndexes(request,boolQuery,indices);

        assert response != null;
        result.setRsCount(response.getHits().totalHits);
        setResultData(result, response);

        Histogram yearAgg = response.getAggregations().get("publication_year");
        KVBean<String, Map<String, ?>> yearFilter = new KVBean<>();
        yearFilter.setD("发表年份");
        yearFilter.setK("earliest_publication_date");
        Map<String, Long> yearMap = new HashMap<>();
        for (Histogram.Bucket bucket : yearAgg.getBuckets()) {
            if (bucket.getKey() instanceof Number) {
                Double year = Double.valueOf(bucket.getKeyAsString());
                year = year / 10000;
                yearMap.put(String.valueOf(year.intValue()), bucket.getDocCount());
            }
        }
        yearFilter.setV(yearMap);
        result.getFilters().add(yearFilter);

        Terms docTypes = response.getAggregations().get("document_type");
        KVBean<String, Map<String, ? extends Object>> docTypeFilter = new KVBean<>();
        docTypeFilter.setD("资源类型");
        docTypeFilter.setK("_type");
        Map<String, Long> docMap = new HashMap<>();
        for (Terms.Bucket bucket : docTypes.getBuckets()) {
            docMap.put(bucket.getKeyAsString(), bucket.getDocCount());
        }
        docTypeFilter.setV(docMap);
        result.getFilters().add(docTypeFilter);

        String annotation = getAnnotationFieldName(request);
        if (annotation != null) {
            Terms knowledgeClasses = response.getAggregations().get("knowledge_class");
            KVBean<String, Map<String, ? extends Object>> knowledgeClassFilter = new KVBean<>();
            knowledgeClassFilter.setD("知识体系");
            knowledgeClassFilter.setK(annotation);
            Map<String, Long> knowledgeMap = new HashMap<>();
            for (Terms.Bucket bucket : knowledgeClasses.getBuckets()) {
                knowledgeMap.put(bucket.getKeyAsString(), bucket.getDocCount());
            }
            knowledgeClassFilter.setV(knowledgeMap);
            result.getFilters().add(knowledgeClassFilter);
        }

        return new RestResp<>(result, request.getTt());
    }

    private void setSearchResource(DocType docType, List<AbstractService> services, List<String> indices){
        if (DocType.PATENT.equals(docType)) {
            services.add(patentService);
            indices.add(PATENT_INDEX);
        } else if (DocType.PAPER.equals(docType)) {
            services.add(paperService);
            indices.add(PAPER_INDEX);
        } else if (DocType.STANDARD.equals(docType)) {
            services.add(standardService);
            indices.add(STANDARD_INDEX);
        } else if (DocType.PICTURE.equals(docType)) {
            services.add(pictureService);
            indices.add(PICTURE_INDEX);
        }

    }
    private BoolQueryBuilder buildQueryDetail(String docId) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.idsQuery().addIds(docId));
        return boolQuery;
    }

    private SearchResponse searchBaikeIndex(QueryRequest request, BoolQueryBuilder boolQuery)
            throws InterruptedException, ExecutionException {
        SearchRequestBuilder srb = esClient.prepareSearch(BAIKE_INDEX);
        srb.setQuery(boolQuery).setFrom(request.getPageNo() - 1).setSize(request.getPageSize());
        SearchResponse response = srb.execute().get();
        return response;
    }

    private SearchResponse searchIndexes(QueryRequest request, BoolQueryBuilder boolQuery, List<String> indices)
            throws InterruptedException, ExecutionException {
        SearchRequestBuilder srb = esClient.prepareSearch(indices.toArray(new String[]{}));
        HighlightBuilder highlighter = new HighlightBuilder().field("title").field("title.original").field("abs")
                .field("abstract.original").field("keywords.keyword").field("persons.name.keyword")
                .field("applicants.name.original.keyword").field("inventors.name.original.keyword");
        srb.highlighter(highlighter).setQuery(boolQuery).setFrom(request.getPageNo() - 1)
                .setSize(request.getPageSize());

        AggregationBuilder aggYear = AggregationBuilders.histogram("publication_year")
                .field("earliest_publication_date").interval(10000).minDocCount(1);
        srb.addAggregation(aggYear);

        AggregationBuilder docTypes = AggregationBuilders.terms("document_type").field("_type");
        srb.addAggregation(docTypes);

        String annotationField = getAnnotationFieldName(request);
        if (annotationField != null) {
            AggregationBuilder knowledge = AggregationBuilders.terms("knowledge_class").field(annotationField);
            srb.addAggregation(knowledge);
        }

        System.out.println(srb.toString());
        return srb.execute().get();
    }

    private String getAnnotationFieldName(QueryRequest request) {
        String annotationField = "annotation_1.name";
        if (request.getFilters() != null) {
            for (KVBean<String, List<String>> filter : request.getFilters()) {
                if ("annotation_1.name".equals(filter.getK())) {
                    annotationField = "annotation_2.name";
                    break;
                } else if ("annotation_2.name".equals(filter.getK())) {
                    annotationField = "annotation_3.name";
                    break;
                } else if ("annotation_3.name".equals(filter.getK())) {
                    return null;
                }
            }
        }
        return annotationField;
    }

    private boolean isChinese(String words) {
        Pattern chinesePattern = Pattern.compile("[\\u4E00-\\u9FA5]+");
        Matcher matcherResult = chinesePattern.matcher(words);
        return matcherResult.find();
    }

    @POST
    @Path("/prompt")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "提示", notes = "关键词提示")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<List<PromptBean>> prompt(@ApiParam(value = "提示请求") QueryRequest request) throws Exception {
        // TODO validation check
        log.info(com.hiekn.util.JSONUtils.toJson(request));
        if (StringUtils.isEmpty(request.getKw())) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }
        QueryBuilder titleTerm;
        if (!isChinese(request.getKw())) {
            titleTerm = QueryBuilders.prefixQuery("name.pinyin", request.getKw()).boost(2);
        } else {
            titleTerm = QueryBuilders.termQuery("name", request.getKw()).boost(2);
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        boolQuery.should(titleTerm);
        if (request.getKwType() != null && request.getKwType() > 0) {
            boolQuery.filter(QueryBuilders.termQuery("type", request.getKwType()));
        }

        SearchRequestBuilder srb = esClient.prepareSearch(PROMPT_INDEX);
        srb.setQuery(boolQuery).setFrom(request.getPageNo() - 1).setSize(request.getPageSize());
        log.info(srb.toString());
        SearchResponse response = srb.execute().get();
        List<PromptBean> promptList = new ArrayList<>();
        for (SearchHit hit : response.getHits()) {
            Map<String, Object> source = hit.getSource();
            Object nameObj = source.get("name");
            Object typeObj = source.get("type");
            Object descObj = source.get("description");
            PromptBean bean = new PromptBean();
            if (typeObj != null)
                bean.setType(Integer.valueOf(getString(typeObj)));
            bean.setName(getString(nameObj));
            bean.setDescription(getString(descObj));

            if (bean.getName() != null && promptList.indexOf(bean) < 0) {
                promptList.add(bean);
            }
        }

        return new RestResp<List<PromptBean>>(promptList, request.getTt());
    }

    @POST
    @Path("/schema")
    @ApiOperation(value = "schema")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<SchemaBean> schema(@QueryParam("tt") Long tt) {
        SchemaBean schema = generalSSEService.getAllAtts(kgName);
        schema.setTypes(this.generalSSEService.getAllTypes(kgName));
        return new RestResp<>(schema, tt);
    }


    @POST
    @Path("/kw2")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "搜索", notes = "搜索过滤及排序")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<SearchResultBean> kw2(@ApiParam(value = "检索请求") JSONObject request)
            throws Exception {
        if (request.get("docType") == null) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }

        String requestStr = JSONUtils.toJson(request);
        log.info(requestStr);

        AbstractService service;
        QueryRequest req;
        switch (DocType.valueOf(request.getString("docType"))) {
            case STANDARD:
                service = standardService;
                req = JSONUtils.fromJson(requestStr, StandardQueryRequest.class);
                break;
            case PAPER:
                service = paperService;
                req = JSONUtils.fromJson(requestStr, QueryRequest.class);
                break;
            case PATENT:
                service = patentService;
                req = JSONUtils.fromJson(requestStr, PatentQueryRequest.class);
                break;
            case PICTURE:
                service = pictureService;
                req = JSONUtils.fromJson(requestStr, QueryRequest.class);
                break;
            default:
                throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }

        if (req == null) throw new BaseException(Code.JSON_ERROR.getCode());

        SearchResultBean result = service.doSearch(req);

        Long tt = request.get("tt") == null ? 0l : request.getLong("tt");
        return new RestResp<>(result, tt);
    }


    private void setResultData(SearchResultBean result, SearchResponse response) {
        for (SearchHit hit : response.getHits()) {
            ItemBean item;
            if (hit.getType().equals("patent_data"))
                item = patentService.extractItem(hit);
            else if (hit.getType().equals("paper_data"))
                item = paperService.extractItem(hit);
            else if (hit.getType().equals("standard_data"))
                item = standardService.extractItem(hit);
            else if (hit.getType().equals("picture_data"))
                item = pictureService.extractItem(hit);
            else {
                continue;
            }
            result.getRsData().add(item);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        paperService = new PaperService(esClient);
        patentService = new PatentService(esClient);
        pictureService = new PictureService(esClient);
        standardService = new StandardService(esClient);
    }
}
