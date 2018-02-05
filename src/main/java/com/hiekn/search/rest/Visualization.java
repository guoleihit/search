package com.hiekn.search.rest;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.reflect.TypeToken;
import com.hiekn.plantdata.bean.TypeBean;
import com.hiekn.plantdata.bean.graph.EntityBean;
import com.hiekn.plantdata.bean.graph.GraphBean;
import com.hiekn.plantdata.bean.graph.SchemaBean;
import com.hiekn.plantdata.util.JSONUtils;
import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.result.Code;
import com.hiekn.search.bean.result.RestResp;
import com.hiekn.search.exception.BaseException;
import com.hiekn.service.Helper;
import com.hiekn.util.CommonResource;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import javax.annotation.Resource;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Controller
@Path("/map")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
@Api(tags = {"可视化数据"})
public class Visualization implements InitializingBean, DisposableBean {

    private static Logger log = LoggerFactory.getLogger(Visualization.class);

    @Resource
    private KGRestApi kgApi;

    @Value("${mongo_ip}")
    private String mongoIP;

    @Value("${mongo_port}")
    private String mongoPort;

    @Resource
    private TransportClient esClient;

    private MongoClient mongoClient;

    private MongoDatabase mapDB;

    @Value("${kg_name}")
    private String kgName;

    @POST
    @Path("/data")
    @ApiOperation(value = "")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<java.util.Map> data(@QueryParam("id") String id, @FormParam("name") String name,
                                             @QueryParam("tt") Long tt) throws Exception {
        Document d = null;
        if(!StringUtils.isEmpty(id)){
            d = mapDB.getCollection("knowledge_map_data").find(Filters.eq("id", Long.valueOf(id))).first();
        }else if(!StringUtils.isEmpty(name)){
            d = mapDB.getCollection("knowledge_map_data").find(Filters.eq("name",name)).first();
        }

        Map r = new JSONObject();
        if(d!=null) {
            d.remove("_id");
            r = d;
        }

        return  new RestResp<>(r ,tt);
    }

    @POST
    @Path("/tree")
    @ApiOperation(value = "树图")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<Map<String, Object>> kgTree(@FormParam("kw") String kw, @FormParam("id") String id, @FormParam("allowAtts") String allowAtts,
                                  @FormParam("allowTypes") String allowTypes, @FormParam("entitiesLimit") Integer entitiesLimit,
                                  @FormParam("relationsLimit") Integer relationsLimit, @FormParam("conceptsLimit") Integer conceptsLimit,
                                  @FormParam("statsLimit") Integer statsLimit, @QueryParam("pageNo") Integer pageNo,
                                  @QueryParam("pageSize") Integer pageSize, @FormParam("kwType") Integer kwType,
                                  @ApiParam("0表示不继承，1表示继承,默认0") @DefaultValue("0") @FormParam("isInherit") Integer isInherit,
                                                @ApiParam("是否查顶层父概念")@DefaultValue("false")@FormParam("isTop") Boolean isTop,
                                                @ApiParam("不查顶层父概念")@FormParam("excludeClassIds") String excludeClassIds,
                                  @QueryParam("tt") Long tt) throws InterruptedException, ExecutionException {
        RestResp<SchemaBean> rest = kgApi.kgSchema(tt);

        log.info(allowAtts+","+allowTypes);
        List<Long> allowAttList;
        List<Long> allowTypeList;
        try {
            allowAttList = JSONUtils.fromJson(allowAtts, new TypeToken<List<Long>>() {
            }.getType());
            allowTypeList = JSONUtils.fromJson(allowTypes, new TypeToken<List<Long>>() {
            }.getType());
        }catch(Exception e) {
            allowAttList = null;
            allowTypeList = null;
        }

        RestResp<GraphBean> restGraph;
        if (allowAttList!=null || allowTypeList != null) {
            restGraph = kgApi.kg(kw, id, allowAtts, allowTypes, entitiesLimit, relationsLimit, conceptsLimit, statsLimit, pageNo, pageSize, kwType, isInherit, isTop, excludeClassIds, tt);
        }else {
            restGraph = kgApi.kg2(kw, id, allowAtts, allowTypes, pageNo, pageSize, kwType, isInherit, isTop, excludeClassIds, tt);
        }

        GraphBean bean = null;
        if(restGraph.getData() != null && restGraph.getData().getRsData() != null
                && restGraph.getData().getRsData().size() > 0) {
            bean = restGraph.getData().getRsData().get(0);
        }
        if (bean != null) {
            List<EntityBean> entityBeanList = bean.getEntityList();
            if (entityBeanList.size() > 0) {
                Map<Long, List<EntityBean>> treeClassId = new HashMap<>();
                for (int i = 1; i < entityBeanList.size(); i++) {
                    EntityBean entity = entityBeanList.get(i);

                    Long classId = entity.getClassId();
                    // 按照classid对结果分类
                    if (treeClassId.get(classId) == null) {
                        List<EntityBean> children = new ArrayList<>();
                        treeClassId.put(classId, children);
                        children.add(entity);
                    } else {
                        treeClassId.get(classId).add(entity);
                    }
                }
                Map<String, Object> result = new HashMap<>();
                result.put("name", entityBeanList.get(0).getName());
                List<Map> topChildren = new ArrayList<>();
                result.put("children", topChildren);
                for(Map.Entry<Long, List<EntityBean>> entry: treeClassId.entrySet()){
                    Long classId = entry.getKey();
                    List<EntityBean> children = entry.getValue();
                    Map<String, Object> child = new HashMap<>();
                    String name = getNameByClassId(rest,classId);
                    child.put("name",name);
                    child.put("children",children);
                    topChildren.add(child);
                }

                result.put("level1HasNextPage", bean.getLevel1HasNextPage());
                return new RestResp<>(result,tt);
            }
        }
        return new RestResp<>(tt);
    }


    @POST
    @Path("/journal/cite")
    @ApiOperation(value = "")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public com.hiekn.search.bean.result.RestData<Object> journalCiteStats(@FormParam("journalName")String journalName,
                                                                 @ApiParam("1表示机构，2表示作者，3表示主题、知识点") @FormParam("statsType")Integer statsType,
                                                                 @ApiParam("0=echart,1=kv")@DefaultValue("0")  @FormParam("returnType")Integer returnType,
                                                                 @DefaultValue("10") @FormParam("size") Integer size,
                                                                 @QueryParam("tt") Long tt) throws ExecutionException, InterruptedException {

        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        bool.filter(QueryBuilders.termQuery("paperType","JOURNAL"));
        bool.must(QueryBuilders.termQuery("journal.journal_chinese_name",journalName));

        SearchRequestBuilder srb = esClient.prepareSearch(CommonResource.PAPER_INDEX);
        srb.setQuery(bool).setSize(0);

        if (statsType == 1) { // 机构
            AggregationBuilder orgsCiteAggs = AggregationBuilders.terms("parent").field("authors.organization.name")
                    .order(Terms.Order.aggregation("citeCount", false)).size(size);
            AggregationBuilder citeCountAggs = AggregationBuilders.sum("citeCount").field("citeCount");
            orgsCiteAggs.subAggregation(citeCountAggs);
            srb.addAggregation(orgsCiteAggs);
        } else if (statsType == 2) { //作者
            bool.mustNot(QueryBuilders.termQuery("authors.name.keyword","(missing)"));

            AggregationBuilder authorCiteAggs = AggregationBuilders.terms("parent").field("authors.name.keyword")
                    .order(Terms.Order.aggregation("citeCount", false)).size(size);
            AggregationBuilder citeCountAggs = AggregationBuilders.sum("citeCount").field("citeCount");
            authorCiteAggs.subAggregation(citeCountAggs);
            srb.addAggregation(authorCiteAggs);
        } else if (statsType == 3) { //主题、知识点
            AggregationBuilder knowledgeCiteAggs = AggregationBuilders.terms("parent").field("keywords")
                    .order(Terms.Order.aggregation("citeCount", false)).size(size);
            AggregationBuilder citeCountAggs = AggregationBuilders.sum("citeCount").field("citeCount");
            knowledgeCiteAggs.subAggregation(citeCountAggs);
            srb.addAggregation(knowledgeCiteAggs);
        }

        SearchResponse response =  srb.execute().get();

        Terms parents = response.getAggregations().get("parent");
        Map<String, Long> results = new HashMap<>();
        for (Terms.Bucket parent: parents.getBuckets()) {
            String key = parent.getKey().toString();
            Sum sum = parent.getAggregations().get("citeCount");
            Long value = Double.valueOf(sum.getValue()).longValue();
            results.put(key, value);
        }

        results = Helper.sortMapByValue(results);

        Map<String, Object> internal = new HashMap<>();
        if (returnType == 0) {
            List xAxis = Lists.newArrayList();
            List xDataList = Lists.newArrayList();
            Map xData = Maps.newHashMap();
            xData.put("data",xDataList);
            xAxis.add(xData);

            List series = Lists.newArrayList();
            List sDataList = Lists.newArrayList();
            Map sData = Maps.newHashMap();
            sData.put("data",sDataList);
            series.add(sData);

            internal.put("xAxis", xAxis);
            internal.put("series", series);


            for(Map.Entry<String, Long> entry: results.entrySet()) {
                String key = entry.getKey();
                Long value = entry.getValue();
                xDataList.add(key);
                sDataList.add(value);
            }

        } else if (returnType == 1) {
            List xAxis = new ArrayList();
            Map xData = Maps.newHashMap();
            xData.put("data",Lists.newArrayList());
            xAxis.add(xData);

            List series = Lists.newArrayList();
            for(Map.Entry<String, Long> entry: results.entrySet()) {
                JSONObject e = new JSONObject();
                e.put("name", entry.getKey());
                e.put("value", entry.getValue());
                series.add(e);
            }
            internal.put("xAxis", xAxis);
            internal.put("series", series);
        }
        return new com.hiekn.search.bean.result.RestData<>(internal);
    }

    @POST
    @Path("/top/persons")
    @ApiOperation(value = "")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp< List<Map<String, Object>>> getTopInventorsStats(@FormParam("organization")String organization, @FormParam("knowledge")String knowledge, @DefaultValue("10") @FormParam("size") Integer size,
                                         @DefaultValue("PATENT") @FormParam("docType")DocType docType,  @QueryParam("tt") Long tt) throws ExecutionException, InterruptedException {

        System.out.println(organization+","+knowledge+","+docType.getName());
        if (StringUtils.isEmpty(organization) && StringUtils.isEmpty(knowledge)) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }
        if (size !=null && size > 100) {
            throw new RuntimeException("size is too large.");
        }
        if (docType != null && !DocType.PAPER.equals(docType) && !DocType.PATENT.equals(docType)) {
            throw new RuntimeException("doc type not supported. only PAPER and PATENT are acceptable.");
        }

        BoolQueryBuilder bool = QueryBuilders.boolQuery();

        String defaultIndex = CommonResource.PATENT_INDEX;
        if (DocType.PAPER.equals(docType)) {
            defaultIndex = CommonResource.PAPER_INDEX;

            if (organization!=null && !StringUtils.isEmpty(organization.trim())) {
                BoolQueryBuilder b = QueryBuilders.boolQuery().minimumShouldMatch(1);
                b.should(QueryBuilders.termQuery("authors.organization.name", organization.trim()));
                b.should(QueryBuilders.termQuery("parent_associated_tag.name", organization.trim()));
                bool.must(b);
            }

        }else { // 专利
            if (organization!=null && !StringUtils.isEmpty(organization.trim())) {
                BoolQueryBuilder b = QueryBuilders.boolQuery().minimumShouldMatch(1);
                b.should(QueryBuilders.termQuery("applicants.name.original.keyword", organization.trim()));
                b.should(QueryBuilders.termQuery("parent_associated_tag.name", organization.trim()));
                bool.must(b);
            }
        }



        if (knowledge != null && !StringUtils.isEmpty(knowledge.trim())) {
            knowledge = knowledge.trim();
            BoolQueryBuilder knowledgeTerm = QueryBuilders.boolQuery().minimumShouldMatch(1);
            knowledgeTerm.should(QueryBuilders.termQuery("_kg_knowledge_tag.name", knowledge));
            knowledgeTerm.should(QueryBuilders.termQuery("_kg_annotation_3.name", knowledge));
            knowledgeTerm.should(QueryBuilders.termQuery("_kg_annotation_2.name", knowledge));
            knowledgeTerm.should(QueryBuilders.termQuery("_kg_annotation_1.name", knowledge));
            bool.must(knowledgeTerm);
        }
        SearchRequestBuilder srb = esClient.prepareSearch(defaultIndex);
        srb.setQuery(bool);

        int sizeHack = size;
        if (sizeHack * 1.5 <= 50) {
            sizeHack = 50;
        }else {
            sizeHack = 100;
        }
        AggregationBuilder topN = AggregationBuilders.nested("annotation_tag","annotation_tag")
                .subAggregation(AggregationBuilders.filter("person_tag", QueryBuilders.termQuery("annotation_tag.classId", 2))
                        .subAggregation(AggregationBuilders.terms("person_id").field("annotation_tag.id").size(sizeHack).order(Terms.Order.count(false))));
        srb.addAggregation(topN);

        SearchResponse response =  srb.execute().get();

        InternalNested annotationTagAggs = response.getAggregations().get("annotation_tag");
        Map<Long,Map<String, Object>> resultMap = new HashMap<>();
        List<Map<String, Object>> results = new ArrayList<>();
        List<Long> ids = new ArrayList<>();
        InternalFilter personTagAgg = annotationTagAggs.getAggregations().get("person_tag");
        Terms personIdAgg = personTagAgg.getAggregations().get("person_id");

        for (Terms.Bucket pId: personIdAgg.getBuckets()) {
            Long id = Long.valueOf(pId.getKey().toString());
            Map<String, Object> map = new HashMap<>();
            map.put("docCount", pId.getDocCount());
            map.put("id", id);
            resultMap.put(id, map);
            results.add(map);
            ids.add(id);
        }

        String dbName = getDBNameOfKg();
        MongoDatabase kgDB = mongoClient.getDatabase(dbName);
        MongoCollection entities = kgDB.getCollection("basic_info");
        MongoCursor<Document> dbCursor = entities.find(Filters.in("_id", ids)).iterator();
        while (dbCursor.hasNext()) {
            Document doc = dbCursor.next();
            Long id = doc.getLong("_id");
            String name = doc.getString("name");
            if (name == null || name.length()<2 || name.length() > 4) {
                Map<String,Object> v = resultMap.remove(id);
                results.remove(v);
                continue;
            }
            String meaningTags = null;
            if (doc.get("meaning_tag")!=null) {
                meaningTags = doc.getString("meaning_tag");
                if (meaningTags != null) {
                    // 如果用户搜索的是机构 国家电网公司 ，append它到原来的 meaningtag 上面
                    if ("国家电网公司".equals(organization) && !meaningTags.equals(organization)) {
                        meaningTags = meaningTags.concat(",国家电网公司");
                    }
                    // 如果用户搜索机构，去掉非当前机构的人物
                    else if (!StringUtils.isEmpty(organization) && !meaningTags.contains(organization)) {
                        Map<String,Object> v = resultMap.remove(id);
                        results.remove(v);
                        continue;
                    }
                }
            }

            Map<String,Object> info = resultMap.get(id);
            if (info != null) {
                info.put("name", name);
                if (meaningTags != null) {
                    info.put("meaningTags", meaningTags);
                }
            }
        }
        dbCursor.close();

        if (results.size() > size) {
            results = results.subList(0,size);
        }
        return new RestResp(results, tt);
    }

    private String getNameByClassId(RestResp<SchemaBean> rest, Long classId) {
        for(SchemaBean bean :rest.getData().getRsData()){
            for(TypeBean type:bean.getTypes()){
                if(classId.equals(type.getK())){
                    return type.getV();
                }
            }
            break;
        }
        return "";
    }

    @Override
    public void destroy() throws Exception {
        if(mongoClient != null){
            mongoClient.close();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        try {
            mongoClient = new MongoClient(mongoIP, Integer.valueOf(mongoPort));
            mapDB = mongoClient.getDatabase("knowledge_map");
        }catch(Exception e){
        }
    }

    private String getDBNameOfKg() {
        String name = CommonResource.getDBNameOfKg(kgName, mongoClient);
        log.info("got kgDbName:" + name);
        return name;
    }
}
