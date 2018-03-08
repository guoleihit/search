package com.hiekn.search.rest;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.primitives.Floats;
import com.google.gson.reflect.TypeToken;
import com.hiekn.plantdata.bean.graph.EntityBean;
import com.hiekn.plantdata.bean.graph.SchemaBean;
import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.SimpleTerm;
import com.hiekn.search.bean.prompt.PromptBean;
import com.hiekn.search.bean.request.CompositeQueryRequest;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.request.QueryRequestInternal;
import com.hiekn.search.bean.result.*;
import com.hiekn.search.exception.BaseException;
import com.hiekn.search.exception.JsonException;
import com.hiekn.search.exception.ServiceException;
import com.hiekn.service.*;
import com.hiekn.service.nlp.NLPServiceImpl;
import com.hiekn.util.CommonResource;
import com.hiekn.util.HttpClient;
import com.hiekn.util.JSONUtils;
import com.hiekn.word2vector.Word2VEC;
import com.hiekn.word2vector.WordEntry;
import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import javax.annotation.Resource;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.hiekn.service.Helper.*;
import static com.hiekn.util.CommonResource.*;

@Controller
@Path("/p")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
@Api(tags = {"搜索"})
public class SearchRestApi implements InitializingBean, DisposableBean {

    @Value("${kg_name}")
    private String kgName;
    @Value("${word2vector_model_location}")
    private String modelLocation;
    @Value("${knowledge_book_location}")
    private String bookLocation;

    @Resource
    private IGeneralSSEService generalSSEService;

    private Word2VEC word2vec;

    private static Logger log = LoggerFactory.getLogger(SearchRestApi.class);

    @Resource
    private TransportClient esClient;

    @Resource
    private NLPServiceImpl nlpService;

    private PaperService paperService = null;
    private PatentService patentService = null;
    private PictureService pictureService = null;
    private StandardService standardService = null;
    private ResultsService resultsService = null;
    private BookService bookService = null;
    private BaikeService baikeService = new BaikeService();

    private Map<String, String> synDicts = new ConcurrentHashMap<>();

    @GET
    @Path("/detailByKg")
    @ApiOperation(value = "通过kgId获取详情")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<SearchResultBean> detailByKgId(@QueryParam("kgId") String kgId,
                                             @QueryParam("tt") Long tt) throws Exception {
        log.info("kgId=" + kgId);
        if (StringUtils.isEmpty(kgId)) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }

        if (!Helper.isNumber(kgId)) {
            throw new RuntimeException("invalid kgId:"+ kgId);
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.termQuery("kg_id", Long.valueOf(kgId).longValue()));

        SearchRequestBuilder srb = esClient.prepareSearch(new String[]{PATENT_INDEX,PAPER_INDEX,STANDARD_INDEX,RESULTS_INDEX});
        srb.setQuery(boolQuery).setSize(1);
        SearchResponse docResp = srb.execute().get();

        SearchResultBean result = new SearchResultBean(kgId);
        DocType docType = null;
        AbstractService service = null;
        String docId = null;

        if (docResp.getHits().getHits().length > 0) {
            SearchHit hit = docResp.getHits().getAt(0);
            if ("paper_data".equals(hit.getType())) {
                service = paperService;
            } else if ("patent_data".equals(hit.getType())) {
                service = patentService;
            } else if ("results_data".equals(hit.getType())) {
                service = resultsService;
            } else if ("standard_data".equals(hit.getType())) {
                service = standardService;
            } else if ("book_data".equals(hit.getType())) {
                service = bookService;
            }
            ItemBean item = service.extractDetail(hit);
            result.getRsData().add(item);
            docId = item.getDocId();
        }

        // 详情页推荐信息
        if(result.getRsData().size() > 0 && service != null && docId != null){
            service.searchSimilarData(docId, result);
        }
        return new RestResp<>(result, tt);
    }

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
        String index = getIndex(docType);

        //System.out.println(Helper.getItemFromHbase(docId, DocType.NEWS).toString());

        SearchRequestBuilder srb = esClient.prepareSearch(index);
        srb.setQuery(docQuery).setFrom(0).setSize(1);
        SearchResponse docResp = srb.get();
        if (docResp.getHits().getHits().length > 0) {
            SearchHit hit = docResp.getHits().getAt(0);
            ItemBean item = extractDetail(hit, docType);
            result.getRsData().add(item);
        }

        // 详情页推荐信息
        if(result.getRsData().size() > 0){
            if (DocType.PATENT.equals(docType)) {
                patentService.searchSimilarData(docId, result);
            }else if (DocType.PAPER.equals(docType)) {
                paperService.searchSimilarData(docId, result);
            }else if (DocType.RESULTS.equals(docType)) {
                resultsService.searchSimilarData(docId, result);
            }else if (DocType.BOOK.equals(docType)) {
                bookService.searchSimilarData(docId, result);
            }
        }
        return new RestResp<>(result, tt);
    }

    private String getIndex(DocType docType) {
        String index = PATENT_INDEX;
        if (DocType.PICTURE.equals(docType)) {
            index = PICTURE_INDEX;
        } else if (DocType.PAPER.equals(docType)) {
            index = PAPER_INDEX;
        } else if (DocType.STANDARD.equals(docType)) {
            index = STANDARD_INDEX;
        } else if (DocType.RESULTS.equals(docType)){
            index = RESULTS_INDEX;
        } else if (DocType.BOOK.equals(docType)){
            index = BOOK_INDEX;
        }
        return index;
    }

    private AbstractService getService(DocType docType) {
        AbstractService service = null;
        if (DocType.PAPER.equals(docType)) {
            service = paperService;
        } else if (DocType.STANDARD.equals(docType)) {
            service = standardService;
        } else if (DocType.RESULTS.equals(docType)){
            service = resultsService;
        } else if (DocType.BOOK.equals(docType)){
            service = bookService;
        } else if (DocType.PATENT.equals(docType)) {
            service = patentService;
        }
        return service;
    }

    private ItemBean extractDetail(SearchHit hit, DocType docType) {
        switch (docType) {
            case PICTURE:
                return pictureService.extractDetail(hit);
            case PAPER:
                return paperService.extractDetail(hit);
            case STANDARD:
                return standardService.extractDetail(hit);
            case RESULTS:
                return resultsService.extractDetail(hit);
            case BOOK:
                return bookService.extractDetail(hit);
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
                                            @QueryParam("pageSize") Integer pageSize, @QueryParam("type") Integer type,
                                            @QueryParam("tt") Long tt) throws Exception{
        log.info("search baike item:" + baike);
        if (StringUtils.isEmpty(baike)) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }

        if (pageNo == null) {
            pageNo = 1;
        }
        if (pageSize == null) {
            pageSize = 10;
        }
        SearchResultBean result = new SearchResultBean(baike);

        if (!Integer.valueOf(4).equals(type)) {
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
        }

        if (Integer.valueOf(4).equals(type) || result.getRsData().size() == 0) {
            try {
                String encodeTitle = URLEncoder.encode(baike, "utf-8");
                String response = HttpClient.sendGet(CommonResource.internal_journal_service_url+"findByTitle/" + encodeTitle, null, null);
                BaikeItem item = JSONUtils.fromJson(response, BaikeItem.class);
                if (item.getContents() != null && item.getContents().size() > 0) {
                    result.getRsData().add(item);
                }
            }catch (Exception e){
                log.error("get journal http service error:",e );
            }
        }
        return new RestResp<>(result, tt);
    }

    @POST
    @Path("/kw")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "搜索", notes = "搜索过滤及排序")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<SearchResultBean> kw(@ApiParam(value = "检索请求") QueryRequest request)
            throws InterruptedException, ExecutionException {
        if (StringUtils.isEmpty(request.getKw()) && StringUtils.isEmpty(request.getCustomQuery())) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }
        log.info(com.hiekn.util.JSONUtils.toJson(request));

        if(request.getKw()==null){
            request.setKw("");
        }
        SearchResultBean result = new SearchResultBean(request.getKw());
        String[] kws = StringUtils.split(request.getKw());
        List<String> tokens = Lists.newArrayList();
        for(String kw:kws){
            if(kw.indexOf('+')>=0){
                tokens.addAll(Lists.newArrayList(StringUtils.split(kw,'+')));
            }else if (kw.indexOf('/')>=0) {
                tokens.addAll(Lists.newArrayList(StringUtils.split(kw,'/')));
            }else if (kw.indexOf(',')>=0) {
                tokens.addAll(Lists.newArrayList(StringUtils.split(kw,',')));
            }else if (kw.indexOf('，')>=0) {
                tokens.addAll(Lists.newArrayList(StringUtils.split(kw,'，')));
            }
        }
        if (tokens.isEmpty()) {
            tokens = Lists.newArrayList(kws);
        }
        log.info("query keywords:" + tokens);
        QueryRequestInternal queryInternal = new QueryRequestInternal(request);
        queryInternal.setUserSplitSegList(new ArrayList<>());
        queryInternal.getUserSplitSegList().addAll(tokens);

        //
        intentionRecognition(queryInternal);

        synonymExtension(tokens, queryInternal);

        List<String> indices = new ArrayList<>();
        List<AbstractService> services = new ArrayList<>();
        setSearchResources(request,indices, services);

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        for (AbstractService service: services) {
            boolQuery.should(service.buildQuery(queryInternal));
        }
        SearchResponse response = searchIndexes(request,boolQuery,indices);

        assert response != null;
        setSingleSearchResults(request, result, response);

        return new RestResp<>(result, request.getTt());
    }

    private void setSingleSearchResults(QueryRequest request, SearchResultBean result, SearchResponse response) {
        result.setRsCount(response.getHits().totalHits);
        setResultData(result, response);

        setYearAggFilter(result, response,"publication_year", "发表年份", "earliest_publication_date");

        setTermAggFilter(result, response, "document_type", "资源类型", "_type");

        String annotation = getAnnotationFieldName(request);
        setKnowledgeAggResult(request, response,result,annotation);
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
        } else if (DocType.RESULTS.equals(docType)) {
            services.add(resultsService);
            indices.add(RESULTS_INDEX);
        } else if (DocType.BOOK.equals(docType)) {
            services.add(bookService);
            indices.add(BOOK_INDEX);
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
        srb.setQuery(boolQuery).setFrom((request.getPageNo() - 1) * request.getPageSize()).setSize(request.getPageSize());
        return srb.execute().get();
    }

    private SearchResponse searchIndexes(QueryRequest request, BoolQueryBuilder boolQuery, List<String> indices)
            throws InterruptedException, ExecutionException {
        SearchRequestBuilder srb = esClient.prepareSearch(indices.toArray(new String[]{}));
        HighlightBuilder highlighter = new HighlightBuilder().field("title").field("title.original").field("abstract")
                .field("abstract.original").field("keywords.keyword").field("authors.name").field("authors.organization.name.keyword")
                .field("applicants.name.original.keyword").field("inventors.name.original.keyword");

        AggregationBuilder aggYear = AggregationBuilders.histogram("publication_year")
                .field("earliest_publication_date").interval(10000).minDocCount(1).order(Histogram.Order.KEY_DESC);
        srb.addAggregation(aggYear);

        AggregationBuilder docTypes = AggregationBuilders.terms("document_type").field("_type");
        srb.addAggregation(docTypes);

        if (request.getSort() != null) {
            if(Integer.valueOf(1).equals(request.getSort()))
                srb.addSort(SortBuilders.fieldSort("earliest_publication_date").order(SortOrder.DESC));
            else if (Integer.valueOf(10).equals(request.getSort()))
                srb.addSort(SortBuilders.fieldSort("earliest_publication_date").order(SortOrder.ASC));
        }

        String annotationField = getAnnotationFieldName(request);
        if (annotationField != null) {
            AggregationBuilder knowledge = AggregationBuilders.terms("knowledge_class").field(annotationField);
            srb.addAggregation(knowledge);
        }


        //FunctionScoreQueryBuilder q = QueryBuilders.functionScoreQuery(boolQuery).setMinScore(1);

        srb.highlighter(highlighter).setQuery(boolQuery).setFrom((request.getPageNo() - 1) * request.getPageSize())
                .setSize(request.getPageSize());

        //System.out.println(srb.toString());
        return srb.execute().get();
    }

    private boolean isNumber(String str) {
        try{
            Double.parseDouble(str);
        }catch(Exception e){
            return false;
        }
        return true;
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
            BoolQueryBuilder bool = QueryBuilders.boolQuery();
            bool.should(QueryBuilders.termQuery("name", request.getKw()).boost(2));
            bool.should(QueryBuilders.prefixQuery("name.keyword", request.getKw()));
            titleTerm = bool;
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        boolQuery.should(titleTerm);
        if (request.getKwType() != null && request.getKwType() > 0) {
            boolQuery.filter(QueryBuilders.termQuery("type", request.getKwType()));
        }

        FilterFunctionBuilder[] functions = new FilterFunctionBuilder[]{
                // new FilterFunctionBuilder(QueryBuilders.wildcardQuery("description","*电*"), ScoreFunctionBuilders.weightFactorFunction(1.1f)),
                new FilterFunctionBuilder(ScoreFunctionBuilders.fieldValueFactorFunction("score").modifier(FieldValueFactorFunction.Modifier.LOG1P).factor(10).missing(1)),
                new FilterFunctionBuilder(QueryBuilders.termQuery("description",""), ScoreFunctionBuilders.weightFactorFunction(0.01f))};

        QueryBuilder q = QueryBuilders.functionScoreQuery(boolQuery, functions).scoreMode(FiltersFunctionScoreQuery.ScoreMode.MULTIPLY).boostMode(CombineFunction.MULTIPLY);
        SearchRequestBuilder srb = esClient.prepareSearch(PROMPT_INDEX);
        srb.setQuery(q).setFrom((request.getPageNo() - 1) * request.getPageSize()).setSize(request.getPageSize());
        //log.info(srb.toString());
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
            bean.setName(getString(nameObj).trim());
            bean.setDescription(getString(descObj));
            bean.setGraphId(getString(source.get("graphId")));

            if (request.getKwType()!=null && request.getKwType()==1 && StringUtils.isEmpty(bean.getDescription())) {
                //continue;
                bean.setGraphId(null);
            }
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
    @Path("/schemaAsync")
    @ApiOperation(value = "schemaAsync")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public void schemaAsync(@QueryParam("tt") Long tt, @Suspended final AsyncResponse asyncResponse) {
        asyncResponse.resume(schema(tt));
    }


    @POST
    @Path("/kw2")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "2级搜索", notes = "搜索过滤及排序")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<SearchResultBean> kw2(@ApiParam(value = "检索请求") CompositeQueryRequest request)
            throws Exception {
        if (request.getDocType() == null) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }

        String requestStr = JSONUtils.toJson(request);
        log.info(requestStr);

        AbstractService service = getService(request.getDocType());
        if (service == null) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }

        SearchResultBean result = service.doCompositeSearch(request);

        return new RestResp<>(result, request.getTt());
    }

    @POST
    @Path("/cp")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "高级搜索", notes = "搜索过滤及排序")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<SearchResultBean> kwComposite(@ApiParam(value = "检索请求") CompositeQueryRequest request)
            throws Exception {
        String requestStr = JSONUtils.toJson(request);
        log.info(requestStr);

        List<String> indices = new ArrayList<>();
        List<AbstractService> services = new ArrayList<>();
        setSearchResources(request, indices, services);


        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        for (AbstractService service: services) {
            QueryBuilder builder = service.buildEnhancedQuery(request);
            if(builder != null){
                boolQuery.should(builder);
            }else {
                if (service == standardService) {
                    indices.remove(STANDARD_INDEX);
                }else if (service == paperService) {
                    indices.remove(PAPER_INDEX);
                }else if (service == patentService) {
                    indices.remove(PATENT_INDEX);
                }else if (service == resultsService) {
                    indices.remove(RESULTS_INDEX);
                }else if (service == bookService) {
                    indices.remove(BOOK_INDEX);
                }
            }
        }

        if (indices.isEmpty()) {
            throw new ServiceException(Code.SEARCH_UNKNOWN_FIELD_ERROR.getCode());
        }
        SearchResponse response = searchIndexes(request,boolQuery,indices);

        SearchResultBean result = new SearchResultBean(request.getKw());

        setSingleSearchResults(request, result,response);

        return new RestResp<>(result, request.getTt());
    }

    private void setSearchResources(QueryRequest request, List<String> indices, List<AbstractService> services) {
        if (request.getDocType() == null && (request.getDocTypeList() == null || request.getDocTypeList().isEmpty())) {
            services.addAll(Arrays.asList(patentService, paperService, standardService, resultsService, bookService));
            indices.addAll(Arrays.asList(PAPER_INDEX, PATENT_INDEX, STANDARD_INDEX, RESULTS_INDEX, BOOK_INDEX));
        } else if(request.getDocType()!=null) {
            setSearchResource(request.getDocType(),services,indices);
        }else if (request.getDocTypeList() != null && !request.getDocTypeList().isEmpty()) {
            for (DocType doc: request.getDocTypeList()) {
                setSearchResource(doc,services,indices);
            }
        }
    }

    @POST
    @Path("/list")
    @ApiOperation(value = "列表", notes = "返回请求ids的资源数据列表")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<SearchResultBean> getListByIds(@ApiParam(value = "id列表") @FormParam("docIds") String docIds,
                                                   @FormParam("filter") String filter,
                                                   @QueryParam("tt") Long tt)
            throws InterruptedException, ExecutionException {
        if (StringUtils.isEmpty(docIds)) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }
        List<String> idList;
        try {
            idList = com.hiekn.plantdata.util.JSONUtils.fromJson(docIds, new TypeToken<List<String>>() {
            }.getType());
        }catch (Exception e) {
            log.error("parse to json error", e);
            throw JsonException.newInstance();
        }
        SearchRequestBuilder srb = esClient.prepareSearch(new String[]{PATENT_INDEX,PAPER_INDEX,STANDARD_INDEX,RESULTS_INDEX});
        srb.setQuery(QueryBuilders.idsQuery().addIds(idList.toArray(new String[]{}))).setSize(20);
        SearchResponse response = srb.execute().get();
        SearchResultBean result = new SearchResultBean("");
        setResultData(result, response);

        Iterator<ItemBean> itr = result.getRsData().iterator();
        if (filter != null && filter.length() > 0) {
            while (itr.hasNext()) {
                ItemBean bean = itr.next();
                if (bean.getTitle()==null || !bean.getTitle().contains(filter)) {
                    itr.remove();
                }
            }
        }

        // 按照id入参顺序排序
        List<ItemBean> sorted = new LinkedList<>();
        for (int i = 0; i< idList.size(); i++) {
            String id = idList.get(i);
            for (ItemBean itemBean: result.getRsData()) {
                if (id.equals(itemBean.getDocId())) {
                    sorted.add(itemBean);
                    break;
                }
            }
        }
        result.setRsData(sorted);
        return new RestResp<>(result, tt);
    }

    @POST
    @Path("/segments")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "分词", notes = "句子分词")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<Map<String,List<String>>> segments(@ApiParam(value = "检索请求") String request)
            throws Exception {

        if (StringUtils.isEmpty(request)) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }

        Set<String> words = new HashSet<>();
        List<SimpleTerm> simpleTerms = nlpService.seg("nshort", request,Lists.newArrayList(), true);
        int wordLimits = 30;
        for (SimpleTerm simpleTerm: simpleTerms) {
            if ("n".equals(simpleTerm.getNature()) || "nz".equals(simpleTerm.getNature())) {
                words.add(simpleTerm.getWord());
                wordLimits -- ;
            }
            if (wordLimits == 0) {
                break;
            }
        }
//        List<AnalyzeResponse.AnalyzeToken> esSegments = Helper.esSegment(request,PAPER_INDEX,esClient, "ik_smart");
//
//        for(AnalyzeResponse.AnalyzeToken token: esSegments){
//            String term = token.getTerm();
//            if(term.length() > 1){
//                words.add(term);
//            }
//        }

        //log.info("es seg:" + Lists.newArrayList(words.toArray(new String[]{})));

        List<String> relatedWords = words.stream().flatMap((w)->{
            Set<WordEntry> commonWords = word2vec.distance(w,2);
            return commonWords.stream();
        }).filter((w)->{return w.score>0.75 && !isNumber(w.name);})
                .sorted((w1,w2)-> Floats.compare(w2.score, w1.score))
                .map((w)->{return w.name;})
                .limit(15)
                .collect(Collectors.toList());


        List<EntityBean> rsList = this.generalSSEService.kg_semantic_seg(request, kgName, false, true, false);
        Set<String> graphWords = new HashSet<>();
        for (EntityBean bean: rsList) {
            if(!StringUtils.isEmpty(bean.getName()) && bean.getName().length()>1)
                graphWords.add(bean.getName());
        }
        List<String> recommendWords = graphWords.stream().filter(w->!isNumber(w) && !relatedWords.contains(w))//sorted((w1,w2)->{return w2.length() - w1.length();})
                .limit(15).collect(Collectors.toList());
        Map <String, List<String>> result = new HashMap<>();

        relatedWords.addAll(recommendWords);

        result.put("commonWords", Lists.newArrayList(words.toArray(new String[]{})));
        result.put("recommendWords",relatedWords);

        log.info("common seg:" + words);
        log.info("recommend seg:" + relatedWords);
        return new RestResp<>(result, 0L);
    }

    @POST
    @Path("/cite")
    @ApiOperation(value = "引用", notes = "生成引用数据")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<Map<String,String>> cite(
            @ApiParam(value = "文档ID列表") @FormParam("docIds") String docIds,
            @ApiParam(value = "引用格式，1=参考文献格式;2=查新格式;3=自定义格式") @FormParam("format") Integer format,
            @ApiParam(value = "自定义字段列表, [\"title\",\"abs\"]") @FormParam("customContent") String customContent,
            @QueryParam("tt") Long tt)
            throws Exception {

        RestResp<SearchResultBean> beanListRes = getListByIds(docIds, null, null);
        try {
            if (!beanListRes.getData().getRsData().isEmpty()) {
                List<ItemBean> beanList = beanListRes.getData().getRsData().get(0).getRsData();
                List<Map<String,String>> mapList = new ArrayList<>();
                Map<String,List<String>> customizedFields = new HashMap<>();

                try {
                    Map<String,List<String>> cFields = com.hiekn.plantdata.util.JSONUtils.fromJson(customContent, new TypeToken<Map<String,List<String>>>() {
                    }.getType());
                    if (cFields != null) {
                        customizedFields = cFields;
                    }
                }catch (Exception e) {
                    log.error("parse to json error", e);
                }

                for (ItemBean itemBean: beanList) {
                    if (DocType.PATENT.equals(itemBean.getDocType())) {
                        mapList.add(patentService.formatCite(itemBean, format, customizedFields.get(DocType.PATENT.getName())));
                    } else if (DocType.PAPER.equals(itemBean.getDocType())) {
                        mapList.add(paperService.formatCite(itemBean, format, customizedFields.get(DocType.PAPER.getName())));
                    } else if (DocType.STANDARD.equals(itemBean.getDocType())) {
                        mapList.add(standardService.formatCite(itemBean, format, customizedFields.get(DocType.STANDARD.getName())));
                    } else if (DocType.RESULTS.equals(itemBean.getDocType())) {
                        mapList.add(resultsService.formatCite(itemBean, format, customizedFields.get(DocType.RESULTS.getName())));
                    }
                }
                return new RestResp<>(mapList, tt);
            }

        }catch (Exception e){
            log.error("request cite error", e);
        }

        return new RestResp<>(tt);
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
            else if (hit.getType().equals("book_data"))
                item = bookService.extractItem(hit);
            else if (hit.getType().equals("results_data"))
                item = resultsService.extractItem(hit);
            else {
                continue;
            }
            result.getRsData().add(item);
        }
    }

    private Map<String, String> intentionRecognition(QueryRequestInternal request){
        Map<String, String> result = new HashMap<>();
        // 识别用户输入机构、人物
        if (request.getUserSplitSegList() != null && request.getUserSplitSegList().size() > 0
                /*&& (request.getKwType() ==1||request.getKwType()==2)*/ ) {

            String userInputPersonName = null;
            String userInputOrgName = null;
            List<EntityBean> rsList = generalSSEService.kg_semantic_seg(request.getKw(), kgName, false, true, false);
            Long person = Helper.types.get("人物");
            Long org = Helper.types.get("机构");

            if(person == null || org == null){
                log.warn("no kg person or org info available.");
                return result;
            }
            for(EntityBean bean: rsList){
                if(person.equals(bean.getClassId()) && !StringUtils.isEmpty(bean.getName())){
                    if (request.getUserSplitSegList().contains(bean.getName())) {
                        if (isChinese(bean.getName()) && (bean.getName().length() > 4 || bean.getName().length() < 2)) {
                            continue;
                        }
                        userInputPersonName = bean.getName();
                        request.getUserSplitSegList().remove(bean.getName());
                        result.put("人物", userInputPersonName);
                        log.info("got person:" + userInputPersonName);
                        request.setRecognizedPerson(userInputPersonName);
                        request.setKw(request.getKw().replace(userInputPersonName, ""));
                    }
                }else if(org.equals(bean.getClassId())){
                    if (request.getUserSplitSegList().contains(bean.getName())) {
                        userInputOrgName = bean.getName();
                        request.getUserSplitSegList().remove(bean.getName());
                        result.put("机构", userInputOrgName);
                        log.info("got org:" + userInputOrgName);
                        request.setRecognizedOrg(userInputOrgName);
                        request.setKw(request.getKw().replace(userInputOrgName, ""));
                    }
                }

                if (!StringUtils.isEmpty(userInputPersonName) && !StringUtils.isEmpty(userInputOrgName)) {
                    //request.setKw(request.getKw().replace(userInputPersonName, ""));
                    //request.setKw(request.getKw().replace(userInputOrgName, ""));
                    break;
                }
            }
        }
        return result;
    }

    private void synonymExtension(List<String> tokens, QueryRequestInternal request) {
        for (String token : tokens) {
            String syn = synDicts.get(token);
            if (!StringUtils.isEmpty(syn)) {
                request.getUserSplitSegList().add(syn);
                log.info("extend synonym word: " + syn);
            }
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        paperService = new PaperService(esClient, generalSSEService, kgName);
        patentService = new PatentService(esClient, generalSSEService, kgName);
        pictureService = new PictureService(esClient);
        standardService = new StandardService(esClient, generalSSEService, kgName);
        bookService = new BookService(esClient);
        resultsService = new ResultsService(esClient, generalSSEService, kgName);
        resultsService.setPaperService(paperService);

        word2vec = new Word2VEC();
        word2vec.loadJavaModel(modelLocation);

        try(BufferedReader reader = new BufferedReader(new InputStreamReader(
                this.getClass().getClassLoader().getResourceAsStream("synonym.txt")))) {

            String line;
            while((line=reader.readLine())!=null){
                String[] kws = StringUtils.split(line,'\t');
                if (kws.length == 2) {
                    if (kws[1].contains("\\")) {
                        continue;
                    }
                    synDicts.put(kws[0],kws[1]);
                }
            }
        }catch (Exception e) {
            log.error("init syn dictionary failed.", e);
        }


        File file = new File(bookLocation);
        File[] files = file.listFiles();
        if (file.isDirectory() && (Objects.isNull(files) || files.length == 0)) {
            return;
        }
        Map<String, Map<String,List<String>>> top = new HashMap<>();

        for (File f : files) {
            try(BufferedReader br = new BufferedReader(new FileReader(f))){
                String txt;
                String currentTopword = null;
                String currentMidword = null;

                while((txt=br.readLine())!=null){
                    if (txt.startsWith("\t\t") && !txt.startsWith("\t\t\t")) {
                        // bottom word
                        Map<String,List<String>> mid = top.get(currentTopword);
                        List<String> bottom = mid.get(currentMidword);
                        bottom.add(txt.trim());
                    }else if (txt.startsWith("\t") && !txt.startsWith("\t\t")) {
                        // middle word
                        if (currentTopword!=null) {
                            txt = txt.trim();
                            Map<String,List<String>> mid = top.get(currentTopword);
                            if(mid != null) {
                                if(mid.get(txt) == null){
                                    mid.put(txt,new ArrayList());
                                }
                            }
                            currentMidword = txt;
                        }
                    }else if (!txt.startsWith("\t")) {
                        // top word
                        txt = txt.trim();
                        if (top.get(txt)==null) {
                            Map<String,List<String>> mid = new HashMap<>();
                            top.put(txt, mid);
                        }
                        currentTopword = txt;
                    }
                }
            }catch(Exception e){
                log.error("reading book error.", e);
            }
        }

        System.out.println(top);
        Helper.book = top;
    }

    @Override
    public void destroy() throws Exception {

    }
}
