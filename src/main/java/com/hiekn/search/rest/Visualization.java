package com.hiekn.search.rest;

import com.alibaba.fastjson.JSONObject;
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
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import javax.annotation.Resource;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Controller
@Path("/map")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
@Api(tags = {"可视化数据"})
public class Visualization implements InitializingBean, DisposableBean {

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

    private MongoDatabase kgDB;

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
            String results = d.toJson();
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

        RestResp<GraphBean> restGraph = kgApi.kg(kw,id,allowAtts,allowTypes,entitiesLimit,relationsLimit,conceptsLimit,statsLimit,pageNo,pageSize,kwType,isInherit, isTop, excludeClassIds, tt);
        GraphBean bean = null;
        if(restGraph.getData() != null && restGraph.getData().getRsData() != null) {
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
                List<Map> topChildren = new ArrayList<Map>();
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

        String defaultIndex = CommonResource.PATENT_INDEX;
        if (DocType.PAPER.equals(docType)) {
            defaultIndex = CommonResource.PAPER_INDEX;
        }
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        if (organization!=null && !StringUtils.isEmpty(organization.trim())) {
            bool.must(QueryBuilders.termQuery("applicants.name.original.keyword", organization.trim()));
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

        AggregationBuilder topN = AggregationBuilders.nested("annotation_tag","annotation_tag")
                .subAggregation(AggregationBuilders.filter("person_tag", QueryBuilders.termQuery("annotation_tag.classId", 2))
                        .subAggregation(AggregationBuilders.terms("person_id").field("annotation_tag.id").size(size).order(Terms.Order.count(false))));
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


        MongoCollection entities = kgDB.getCollection("entity_id");
        MongoCursor<Document> dbCursor = entities.find(Filters.in("id", ids)).iterator();
        while (dbCursor.hasNext()) {
            Document doc = dbCursor.next();
            Long id = doc.getLong("id");
            String name = doc.getString("name");
            if (name == null || name.length()<2 || name.length() > 4) {
                Map<String,Object> v = resultMap.remove(id);
                results.remove(v);
                continue;
            }
            String meaningTags = null;
            Object meta = doc.get("meta_info");
            if (meta != null){
                List<String> meaning_tags = new ArrayList<>();
                Object d2r = ((Document) meta).get("d2r");
                if (d2r != null) {
                    Object idsObj = ((Document) d2r).get("id");
                    if (idsObj != null && idsObj instanceof List) {
                        for (Object idObj : (List) idsObj) {
                            Document idDoc = (Document) idObj;
                            if (idDoc.get("org") != null) {
                                meaning_tags.add(idDoc.getString("org"));
                            }
                        }
                    }
                }
                if (!meaning_tags.isEmpty()) {
                    StringBuilder builder = new StringBuilder();
                    int counter = 0;
                    for (String str : meaning_tags) {
                        counter++;
                        builder.append(str);
                        if (meaning_tags.size() > counter) {
                            builder.append(",");
                        }
                    }
                    meaningTags = builder.toString();
                }
            }

            Map<String,Object> info = resultMap.get(id);
            info.put("name",name);

            if (meaningTags != null) {
                info.put("meaningTags", meaningTags);
            }
        }
        dbCursor.close();
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
            kgDB = mongoClient.getDatabase(kgName);
        }catch(Exception e){
        }
    }
}
