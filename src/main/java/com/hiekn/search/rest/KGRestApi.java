package com.hiekn.search.rest;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import com.hiekn.kg.service.bean.GraphHasAttsItem;
import com.hiekn.kg.service.bean.KGResultItem;
import com.hiekn.plantdata.bean.TypeBean;
import com.hiekn.plantdata.bean.graph.*;
import com.hiekn.plantdata.bean.rest.RestReturnCode;
import com.hiekn.plantdata.exception.ServiceException;
import com.hiekn.plantdata.parser.graph.GraphParser;
import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.plantdata.util.HttpClient;
import com.hiekn.plantdata.util.JSONUtils;
import com.hiekn.plantdata.util.SseUtils;
import com.hiekn.search.bean.result.Code;
import com.hiekn.search.bean.result.RestResp;
import com.hiekn.search.exception.BaseException;
import com.hiekn.search.exception.JsonException;
import com.hiekn.service.Helper;
import com.hiekn.util.CommonResource;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import javax.annotation.Resource;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.concurrent.*;

import static com.hiekn.service.Helper.knowledgeIds;
import static com.hiekn.service.Helper.types;

@Controller
@Path("/g")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
@Api(tags = {"搜索"})
public class KGRestApi implements InitializingBean, DisposableBean {

    @Value("${kg_name}")
    private String kgName;
    @Resource
    private IGeneralSSEService generalSSEService;

    @Value("${mongo_ip}")
    private String mongoIP;

    @Value("${mongo_port}")
    private String mongoPort;

    private MongoClient mongoClient;

    private static Logger log = LoggerFactory.getLogger(KGRestApi.class);

    @ApiOperation(value = "实体的下拉提示")
    @POST
    @Path("/prompt")
    public Response prompt(@QueryParam("userId") Integer userId, @FormParam("kgName") String kgName,
                           @FormParam("kw") String kw, @FormParam("pageSize") @DefaultValue("3") Integer pageSize,
                           @FormParam("allowTypes") String allowTypes,
                           @FormParam("isCaseSensitive") @DefaultValue("false") Boolean isCaseSensitive,
                           @FormParam("metaDataOption") @DefaultValue("{'sorts':{'4':-1}}") String metaDataOption,
                           @QueryParam("token") String token, @QueryParam("tt") Long tt) {

        List<Long> allowTypesList;
        List<EntityBean> rsList;

        allowTypesList = JSONUtils.fromJson(allowTypes, new TypeToken<List<Long>>() {
        }.getType());

        rsList = getPrompt(kgName, kw, allowTypesList, isCaseSensitive, pageSize, metaDataOption);
        RestResp<EntityBean> rs = new RestResp<>(rsList, tt);
        return Response.ok().entity(rs).build();
    }

    public List<EntityBean> getPrompt(String kgName, String kw, List<Long> allowTypes, boolean isCaseSensitive,
                                      Integer pageSize, String metaDataOption) {
        String url = CommonResource.kg_public_service_url + "/kg/prompt";

        MultivaluedMap<String, Object> para = new MultivaluedHashMap<String, Object>();
        para.add("kgName", kgName);
        para.add("text", kw);
        para.add("type", 1);
        para.add("size", pageSize);
        para.add("isCaseInsensitive", false);
        para.add("metaDataOption", metaDataOption);

        if (allowTypes != null && allowTypes.size() > 0) {
            para.add("conceptIds", allowTypes);
            para.add("isInherit", false);
        } else {
            // 如果没指定 在全域上搜 指定顶层为0 递归为true
            para.add("conceptIds", Arrays.asList(0));
            para.add("isInherit", true);
        }

        String rs;

        try {
            rs = HttpClient.sendPost(url, null, para);

        } catch (Exception e) {
            throw ServiceException.newInstance(RestReturnCode.REMOTE_INVOKE_ERROR);
        }

        try {
            System.out.println(rs);
            JSONObject result = JSONUtils.fromJson(rs, JSONObject.class);
            List<EntityBean> rsList = new ArrayList<>();
            for (Object e : ((java.util.Map) result.get("data")).values()) {
                if (e instanceof List) {
                    for (Object elem : (List) e) {
                        JSONObject element = (JSONObject) elem;
                        EntityBean bean = new EntityBean();
                        bean.setId(element.getLong("id"));
                        if (element.get("entityName") != null)
                            bean.setName(element.getString("entityName"));
                        if (element.get("meaningTag") != null)
                            bean.setMeaningTag(element.getString("meaningTag"));
                        if (element.get("classId") != null)
                            bean.setClassId(element.getLong("classId"));
                        if (element.get("type") != null)
                            bean.setKgType(element.getInteger("type"));
                        rsList.add(bean);
                    }
                }
            }
            return rsList;
        } catch (Exception e) {
            throw ServiceException.newInstance(RestReturnCode.REMOTE_PARSE_ERROR);
        }
    }

    public RestResp<GraphBean> kg2(@FormParam("kw") String kw, @FormParam("id") String id, @FormParam("allowAtts") String allowAtts,
                                   @FormParam("allowTypes") String allowTypes, @QueryParam("pageNo") Integer pageNo,
                                   @QueryParam("pageSize") Integer pageSize, @FormParam("kwType") Integer kwType,
                                   @ApiParam("0表示不继承，1表示继承,默认0") @DefaultValue("0") @FormParam("isInherit") Integer isInherit,
                                   @ApiParam("是否查顶层父概念") @DefaultValue("false") @FormParam("isTop") Boolean isTop,
                                   @ApiParam("不查顶层父概念") @FormParam("excludeClassIds") String excludeClassIds,
                                   @QueryParam("tt") Long tt) {

        if (StringUtils.isEmpty(kw) && StringUtils.isEmpty(id)) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }
        log.info("kw:" + kw + ",tt:" + tt + ",pageNo:" + pageNo + ",pageSize:" + pageSize);
        List<Long> excludeClassIdList = null;
        if (pageNo == null) {
            pageNo = 1;
        }

        if (pageSize == null || pageSize == 0) {
            pageSize = 20;
        }

        if (kw == null) {
            kw = "";
        }

        String[] kws = kw.trim().split(" ");
        if (kws.length > 0) {
            kw = kws[0];
        }

        try {
            excludeClassIdList = JSONUtils.fromJson(excludeClassIds, new TypeToken<List<Long>>() {
            }.getType());
        } catch (Exception e) {
            log.error("parse to json error", e);
            throw JsonException.newInstance();
        }


        List<EntityBean> rsList = null;
        if (StringUtils.isEmpty(id)) {
            rsList = this.generalSSEService.kg_semantic_seg(kw, kgName, false, true, false);
        }

        Long entityId = getEntityId(kw, kwType, kws, rsList);
        if (!StringUtils.isEmpty(id)) {
            try {
                entityId = Long.valueOf(id);
            } catch (Exception e) {
            }
        }
        entityId = getEntityIdByTrick(kw, id, kwType, entityId);


        GraphBean graphBean = null;
        if (entityId != null) {
            GraphHasAttsItem graphHasAttsItem = doQueryKgInternal(kgName, entityId, 1, 0, allowAtts, allowTypes,
                    pageNo, pageSize, isInherit);
            graphBean = GraphParser.GraphItem2GraphBean(graphHasAttsItem, true);
            if (isTop) {
                List<EntityBean> entityList = graphBean.getEntityList();
                List<Long> ids = Lists.newArrayList();
                for (EntityBean entityBean : entityList) {
                    if (Objects.nonNull(excludeClassIdList) && !excludeClassIdList.isEmpty() && excludeClassIdList.contains(entityBean.getClassId())) {
                        continue;
                    }
                    ids.add(entityBean.getId());

                }
                Map<Long, List<Long>> parentConcept = generalSSEService.getParentConcept(kgName, ids, 1);
                if (Objects.nonNull(parentConcept) && !parentConcept.isEmpty()) {
                    for (EntityBean entityBean : entityList) {
                        Long eId = entityBean.getId();
                        if (parentConcept.containsKey(eId)) {
                            entityBean.setClassId(parentConcept.get(eId).get(0));
                        }
                    }
                }
            }
        }
        return new RestResp<>(graphBean, 0L);
    }

    /**
     * ugly trick
     * @param kw
     * @param id
     * @param kwType
     * @param entityId
     * @return
     */
    private Long getEntityIdByTrick(String kw,String id, Integer kwType, Long entityId) {
        // 实体识别没有考虑概率值，直接读库吧。。。
        if (StringUtils.isEmpty(id) && kwType!=null && kwType == 1 && Helper.types.get("人物") != null) {
            String kgDb = CommonResource.getDBNameOfKg(kgName, mongoClient);
            MongoDatabase db = mongoClient.getDatabase(kgDb);
            MongoCollection<Document> collection = db.getCollection("entity_id");
            Document doc = collection.find(Filters.and(Filters.eq("name",kw),
                    Filters.exists("meta_info.d2r.id.org"),
                    Filters.ne("meta_info.d2r.id.org",""),
                    Filters.eq("concept_id", Long.valueOf(Helper.types.get("人物")))))
                    .sort(new Document().append("meta_data.meta_data_4", -1)).first();
            if (doc != null && doc.get("id")!=null) {
                entityId = doc.getLong("id");
                log.info("for kwType=2, got an entity with highest score from kg db :" + entityId);
            }
        }
        // 期刊图谱，命名实体识别失败，直接读库吧。。。
        else if (entityId == null && kwType!=null && kwType == 4 && Helper.types.get("期刊") != null) {
            String kgDb = CommonResource.getDBNameOfKg(kgName, mongoClient);
            MongoDatabase db = mongoClient.getDatabase(kgDb);
            MongoCollection<Document> collection = db.getCollection("entity_id");
            Document doc = collection.find(Filters.and(Filters.regex("name","^"+kw),
                    Filters.eq("concept_id", Long.valueOf(Helper.types.get("期刊"))))).first();
            if (doc != null && doc.get("id")!=null) {
                entityId = doc.getLong("id");
                log.info("for kwType=4, got entity from kg db :" + entityId);
            }
        } else if (StringUtils.isEmpty(id) && kwType == null) {
            String kgDb = CommonResource.getDBNameOfKg(kgName, mongoClient);
            MongoDatabase db = mongoClient.getDatabase(kgDb);
            MongoCollection<Document> collection = db.getCollection("entity_id");
            Document doc = collection.find(Filters.and(Filters.regex("name","^"+kw)))
                    .sort(new Document().append("meta_data.meta_data_4", -1)).first();
            if (doc != null && doc.get("id")!=null) {
                entityId = doc.getLong("id");
                log.info("for kwType=null, got entity from kg db :" + entityId);
            }

        }
        return entityId;
    }

    private Long getEntityId(@FormParam("kw") String kw, @FormParam("kwType") Integer kwType, String[] kws, List<EntityBean> rsList) {
        Long entityId = null;
        if (rsList != null && rsList.size() > 0) {
            if (kwType != null && kwType > 0) {
                if (kwType == 2) { // 机构
                    for (EntityBean entity : rsList) {
                        if (entity.getClassId() != null && entity.getClassId().equals(types.get("机构")) && kw.equals(entity.getName())) {
                            entityId = entity.getId();
                            break;
                        }
                    }
                } else if (kwType == 1) { // 人物
                    for (EntityBean entity : rsList) {
                        if (entity.getClassId() != null && entity.getClassId().equals(types.get("人物"))) {
                            if (kws.length > 1 && !StringUtils.isEmpty(entity.getMeaningTag())) {
                                if (kws[1].indexOf(entity.getMeaningTag()) >= 0 || (entity.getMeaningTag() != null
                                        || entity.getMeaningTag().indexOf(kws[1]) >= 0)) {
                                    entityId = entity.getId();
                                    break;
                                }
                            }

                            if (!StringUtils.isEmpty(entity.getMeaningTag()) && entity.getName().length() >= 2
                                    && entity.getName().length() <= 4) {
                                entityId = entity.getId();
                                break;
                            }

                        }
                    }
                } else if (kwType == 3) { // 知识点
                    for (EntityBean entity : rsList) {
                        if (entity.getClassId() != null && knowledgeIds.contains(entity.getClassId()) && kw.equals(entity.getName())) {
                            entityId = entity.getId();
                            log.info("found knowledge:" + entityId + ",classId:" + entity.getClassId());
                            break;
                        }
                    }
                } else if (kwType == 4) { // 期刊
                    for (EntityBean entity : rsList) {
                        if (entity.getClassId() != null && entity.getClassId().equals(types.get("期刊")) && kw.equals(entity.getName())) {
                            entityId = entity.getId();
                            break;
                        }
                    }
                }
            }else {
                for (EntityBean entity : rsList) {
                    if (kw.equals(entity.getName())) {
                        entityId = entity.getId();
                        log.info("found entity:" + entityId + ",classId:" + entity.getClassId());
                        break;
                    }
                }
            }
        }
        return entityId;
    }

    private GraphHasAttsItem doQueryKgInternal(String kgName, Long entityId,
                                               Integer distance,
                                               Integer direction, String allowAtts,
                                               String allowTypes, Integer pageNo, Integer pageSize, Integer isInherit) {
        String url = com.hiekn.plantdata.util.CommonResource.KG_SERVICE_PUBLIC_PATH + "/kg/graph/full/hasatts";
        MultivaluedMap<String, Object> para = new MultivaluedHashMap<String, Object>();
        para.add("kgName", kgName);
        para.add("entityId", entityId);
        para.add("distance", distance);
        para.add("direction", direction);
        if (distance == 1) {
            para.add("level1PageNo", pageNo);
            para.add("level1PageSize", pageSize);
        }
        para.add("typeInherit", isInherit);

        List<Long> keyList = new ArrayList<>(distance);
        for (int d = 1; d <= distance; d++) {
            keyList.add(Long.valueOf(d));
        }

        if (allowAtts != null) {
            para.add("allowAtts", allowAtts);
        }
        if (allowTypes != null) {
            para.add("allowTypes", allowTypes);
        }

        return SseUtils.sendPost(url, null, para, new TypeToken<KGResultItem<GraphHasAttsItem>>() {
        }.getType());
    }


    @POST
    @Path("/graph")
    @ApiOperation(value = "图谱")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<GraphBean> kg(@FormParam("kw") String kw, @FormParam("id") String id, @FormParam("allowAtts") String allowAtts,
                                  @FormParam("allowTypes") String allowTypes, @FormParam("entitiesLimit") Integer entitiesLimit,
                                  @FormParam("relationsLimit") Integer relationsLimit, @FormParam("conceptsLimit") Integer conceptsLimit,
                                  @FormParam("statsLimit") Integer statsLimit, @QueryParam("pageNo") Integer pageNo,
                                  @QueryParam("pageSize") Integer pageSize, @FormParam("kwType") Integer kwType,
                                  @ApiParam("0表示不继承，1表示继承,默认0") @DefaultValue("0") @FormParam("isInherit") Integer isInherit,
                                  @ApiParam("是否查顶层父概念") @DefaultValue("false") @FormParam("isTop") Boolean isTop,
                                  @ApiParam("不查顶层父概念") @FormParam("excludeClassIds") String excludeClassIds,
                                  @QueryParam("tt") Long tt) throws InterruptedException, ExecutionException {

        if (StringUtils.isEmpty(kw) && StringUtils.isEmpty(id)) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }
        log.info("kw:" + kw + ",tt:" + tt + ",pageNo:" + pageNo + ",pageSize:" + pageSize);
        List<Long> allowAttList = null;
        List<Long> allowTypeList = null;
        List<Long> excludeClassIdList = null;
        if (pageNo == null) {
            pageNo = 1;
        }

        if (pageSize == null || pageSize == 0) {
            pageSize = 20;
        }

        if (kw == null) {
            kw = "";
        }

        try {
            allowAttList = JSONUtils.fromJson(allowAtts, new TypeToken<List<Long>>() {
            }.getType());
            allowTypeList = JSONUtils.fromJson(allowTypes, new TypeToken<List<Long>>() {
            }.getType());
            excludeClassIdList = JSONUtils.fromJson(excludeClassIds, new TypeToken<List<Long>>() {
            }.getType());
        } catch (Exception e) {
            log.error("parse to json error", e);
            throw JsonException.newInstance();
        }
        String[] kws = kw.trim().split(" ");
        if (kws.length > 0) {
            kw = kws[0];
        }
        List<EntityBean> rsList = null;
        if (StringUtils.isEmpty(id) || !Helper.isNumber(id)) {
            rsList = this.generalSSEService.kg_semantic_seg(kw, kgName, false, true, false);
        }

        GraphBean graphBean = null;

        Long entityId = getEntityId(kw, kwType, kws, rsList);
        if (!StringUtils.isEmpty(id)) {
            try {
                entityId = Long.valueOf(id);
            } catch (Exception e) {
            }
        }
        entityId = getEntityIdByTrick(kw,id,kwType,entityId);
        if (entityId != null) {
            graphBean = generalSSEService.kg_graph_full_hasatts(kgName, entityId, 1, 0, allowAttList, allowTypeList,
                    true, pageNo, pageSize, isInherit, isTop, excludeClassIdList);

            if (entitiesLimit != null && entitiesLimit > 0) {
                List<EntityBean> entities = graphBean.getEntityList();
                if (entities != null && entities.size() > entitiesLimit) {
                    graphBean.setEntityList(entities.subList(0, entitiesLimit));
                }
            }

            if (conceptsLimit != null && conceptsLimit > 0) {
                List<PathAGBean> concepts = graphBean.getConnects();
                if (concepts != null && concepts.size() > conceptsLimit) {
                    graphBean.setConnects(concepts.subList(0, conceptsLimit));
                }
            }

            if (relationsLimit != null && relationsLimit > 0) {
                List<RelationBean> relations = graphBean.getRelationList();
                if (relations != null && relations.size() > relationsLimit) {
                    graphBean.setRelationList(relations.subList(0, relationsLimit));
                }
            }

            if (statsLimit != null && statsLimit > 0) {
                List<GraphStatBean> stats = graphBean.getStats();
                if (stats != null && stats.size() > statsLimit) {
                    graphBean.setStats(stats.subList(0, statsLimit));
                }
            }
        }


        return new RestResp<>(graphBean, 0L);
    }

    @POST
    @Path("/schema")
    @ApiOperation(value = "图schema")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<SchemaBean> kgSchema(@QueryParam("tt") Long tt) {
        SchemaBean schema = generalSSEService.getAllAtts(kgName);
        schema.setTypes(this.generalSSEService.getAllTypes(kgName));
        return new RestResp<SchemaBean>(schema, tt);
    }

    @POST
    @Path("/invalidate")
    @ApiOperation(value = "重建缓存")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<SchemaBean> cacheInvalidate(@QueryParam("tt") Long tt) {
        asyncGetKnowledge();
        log.info("begin to reconstruct kg cache...");
        return new RestResp<>(tt);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        try {
            asyncGetKnowledge();

            mongoClient = new MongoClient(mongoIP, Integer.valueOf(mongoPort));
        } catch (Exception e) {
            log.error("initializing error:", e);
        }
    }

    private void asyncGetKnowledge() {
        FutureTask f = new FutureTask(new Callable() {
            @Override
            public Object call() throws Exception {
                getGraphKnowledge();
                return null;
            }
        });

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(f, 60, TimeUnit.SECONDS);
        executor.shutdown();
    }

    private void getGraphKnowledge() {
        Map<String, Long> types = new HashMap<>();
        Set<Long> knowledgeIds = new HashSet<>();
        RestResp<SchemaBean> rest = kgSchema(0L);
        for (SchemaBean bean : rest.getData().getRsData()) {
            log.info("begin to get knowledge...");
            for (TypeBean type : bean.getTypes()) {
                if ("人物".equals(type.getV())) {
                    types.put("人物", type.getK());
                } else if ("机构".equals(type.getV())) {
                    types.put("机构", type.getK());
                } else if ("知识点".equals(type.getV())) {
                    types.put("知识点", type.getK());
                    knowledgeIds.add(type.getK());
                } else if ("期刊".equals(type.getV())) {
                    types.put("期刊", type.getK());
                }

                if (knowledgeIds.contains(type.getParentId())) {
                    knowledgeIds.add(type.getK());
                }
            }
            log.info("get knowledge end." + knowledgeIds.size());
            break;
        }
        Helper.knowledgeIds = knowledgeIds;
        Helper.types = types;
    }

    @Override
    public void destroy() throws Exception {
        if(mongoClient != null){
            mongoClient.close();
        }
    }
}
