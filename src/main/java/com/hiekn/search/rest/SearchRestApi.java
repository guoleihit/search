package com.hiekn.search.rest;

import com.google.common.collect.Lists;
import com.google.common.primitives.Floats;
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
import com.hiekn.search.exception.ServiceException;
import com.hiekn.service.*;
import com.hiekn.service.nlp.NLPServiceImpl;
import com.hiekn.util.CommonResource;
import com.hiekn.util.JSONUtils;
import com.hiekn.word2vector.Word2VEC;
import com.hiekn.word2vector.WordEntry;
import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
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
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import javax.annotation.Resource;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.hiekn.service.Helper.*;
import static com.hiekn.util.CommonResource.*;

@Controller
@Path("/p")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
@Api(tags = {"搜索"})
public class SearchRestApi implements InitializingBean {

    @Value("${kg_name}")
    private String kgName;
    @Value("${word2vector_model_location}")
    private String modelLocation;

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
        } else if (DocType.RESULTS.equals(docType)){
            index = RESULTS_INDEX;
        }

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
            }
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
            case RESULTS:
                return resultsService.extractDetail(hit);
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
        if (StringUtils.isEmpty(request.getKw()) && StringUtils.isEmpty(request.getCustomQuery())) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }
        log.info(com.hiekn.util.JSONUtils.toJson(request));

        if(request.getKw()==null){
            request.setKw("");
        }
        SearchResultBean result = new SearchResultBean(request.getKw());
        String[] kws = StringUtils.split(request.getKw());
        List<String> tokens = Lists.newArrayList(kws);
        for(String kw:kws){
            if(kw.indexOf('+')>=0){
                tokens.addAll(Lists.newArrayList(StringUtils.split(kw,'+')));
            }else if (kw.indexOf('/')>=0) {
                tokens.addAll(Lists.newArrayList(StringUtils.split(kw,'/')));
            }
        }
        log.info("query keywords:" + tokens);
        QueryRequestInternal queryInternal = new QueryRequestInternal(request);
        queryInternal.setUserSplitSegList(tokens);

        //
        intentionRecognition(queryInternal);


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
        setKnowledgeAggResult(response,result,annotation);
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
                .field("abstract.original").field("keywords.keyword").field("authors.name.keyword").field("authors.organization.name.keyword")
                .field("applicants.name.original.keyword").field("inventors.name.original.keyword");

        AggregationBuilder aggYear = AggregationBuilders.histogram("publication_year")
                .field("earliest_publication_date").interval(10000).minDocCount(1).order(Histogram.Order.KEY_DESC);
        srb.addAggregation(aggYear);

        AggregationBuilder docTypes = AggregationBuilders.terms("document_type").field("_type");
        srb.addAggregation(docTypes);

        if (request.getSort() != null) {
            if(Integer.valueOf(1).equals(request.getSort()))
                srb.addSort(SortBuilders.fieldSort("earliest_publication_date").order(SortOrder.DESC));
        }

        String annotationField = getAnnotationFieldName(request);
        if (annotationField != null) {
            AggregationBuilder knowledge = AggregationBuilders.terms("knowledge_class").field(annotationField);
            srb.addAggregation(knowledge);
        }


        //FunctionScoreQueryBuilder q = QueryBuilders.functionScoreQuery(boolQuery).setMinScore(1);

        srb.highlighter(highlighter).setQuery(boolQuery).setFrom((request.getPageNo() - 1) * request.getPageSize())
                .setSize(request.getPageSize());

        System.out.println(srb.toString());
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
            titleTerm = QueryBuilders.termQuery("name", request.getKw()).boost(2);
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        boolQuery.should(titleTerm);
        if (request.getKwType() != null && request.getKwType() > 0) {
            boolQuery.filter(QueryBuilders.termQuery("type", request.getKwType()));
        }

        FilterFunctionBuilder[] functions = new FilterFunctionBuilder[]{
                new FilterFunctionBuilder(QueryBuilders.wildcardQuery("description","*电*"), ScoreFunctionBuilders.weightFactorFunction(1.1f)),
                new FilterFunctionBuilder(QueryBuilders.termQuery("description",""), ScoreFunctionBuilders.weightFactorFunction(0.5f))};

        QueryBuilder q = QueryBuilders.functionScoreQuery(boolQuery, functions).scoreMode(FiltersFunctionScoreQuery.ScoreMode.MAX).boostMode(CombineFunction.MULTIPLY);
        SearchRequestBuilder srb = esClient.prepareSearch(PROMPT_INDEX);
        srb.setQuery(q).setFrom((request.getPageNo() - 1) * request.getPageSize()).setSize(request.getPageSize());
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
            bean.setGraphId(getString(source.get("graphId")));

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

        AbstractService service;
        //QueryRequest req;
        switch (request.getDocType()) {
            case STANDARD:
                service = standardService;
                break;
            case PAPER:
                service = paperService;
                break;
            case PATENT:
                service = patentService;
                break;
            case RESULTS:
                service = resultsService;
                break;
            default:
                throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }

       // if (req == null) throw new BaseException(Code.JSON_ERROR.getCode());

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
            services.addAll(Arrays.asList(patentService, paperService, standardService, resultsService));
            indices.addAll(Arrays.asList(PAPER_INDEX, PATENT_INDEX, STANDARD_INDEX, RESULTS_INDEX));
        } else if(request.getDocType()!=null) {
            setSearchResource(request.getDocType(),services,indices);
        }else if (request.getDocTypeList() != null && !request.getDocTypeList().isEmpty()) {
            for (DocType doc: request.getDocTypeList()) {
                setSearchResource(doc,services,indices);
            }
        }
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
            else if (hit.getType().equals("results_data"))
                item = resultsService.extractItem(hit);
            else {
                continue;
            }
            result.getRsData().add(item);
        }
    }

    protected Map<String, String> intentionRecognition(QueryRequestInternal request){
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

    @Override
    public void afterPropertiesSet() throws Exception {
        paperService = new PaperService(esClient, generalSSEService, kgName);
        patentService = new PatentService(esClient, generalSSEService, kgName);
        pictureService = new PictureService(esClient);
        standardService = new StandardService(esClient, generalSSEService, kgName);
        resultsService = new ResultsService(esClient, generalSSEService, kgName);

        word2vec = new Word2VEC();
        word2vec.loadJavaModel(modelLocation);
    }
}
