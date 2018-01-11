package com.hiekn.search.rest;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.reflect.TypeToken;
import com.hiekn.plantdata.bean.TypeBean;
import com.hiekn.plantdata.bean.graph.*;
import com.hiekn.plantdata.bean.rest.RestReturnCode;
import com.hiekn.plantdata.exception.ServiceException;
import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.plantdata.util.HttpClient;
import com.hiekn.plantdata.util.JSONUtils;
import com.hiekn.search.bean.graph.MyGraphBean;
import com.hiekn.search.bean.result.Code;
import com.hiekn.search.bean.result.RestResp;
import com.hiekn.search.exception.BaseException;
import com.hiekn.search.exception.JsonException;
import com.hiekn.service.Helper;
import com.hiekn.util.CommonResource;
import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class KGRestApi implements InitializingBean{

    @Value("${kg_name}")
    private String kgName;
    @Resource
    private IGeneralSSEService generalSSEService;

    private static Logger log = LoggerFactory.getLogger(KGRestApi.class);

    @ApiOperation(value = "实体的下拉提示")
    @POST
    @Path("/prompt")
    public Response prompt(@QueryParam("userId") Integer userId, @FormParam("kgName") String kgName,
                           @FormParam("kw") String kw, @FormParam("pageSize") @DefaultValue("3") Integer pageSize,
                           @FormParam("allowTypes") String allowTypes,
                           @FormParam("isCaseSensitive") @DefaultValue("false") Boolean isCaseSensitive,
                           @QueryParam("token") String token, @QueryParam("tt") Long tt) {

        List<Long> allowTypesList;
        List<EntityBean> rsList;

        allowTypesList = JSONUtils.fromJson(allowTypes, new TypeToken<List<Long>>() {
        }.getType());

        rsList = getPrompt(kgName, kw, allowTypesList, isCaseSensitive, pageSize);
        RestResp<EntityBean> rs = new RestResp<>(rsList, tt);
        return Response.ok().entity(rs).build();
    }

    public List<EntityBean> getPrompt(String kgName, String kw, List<Long> allowTypes, boolean isCaseSensitive,
                                      Integer pageSize) {
        String url = CommonResource.kg_public_service_url + "/kg/prompt";

        MultivaluedMap<String, Object> para = new MultivaluedHashMap<String, Object>();
        para.add("kgName", kgName);
        para.add("text", kw);
        para.add("type", 1);
        para.add("size", pageSize);
        para.add("isCaseInsensitive", false);

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
                if(e instanceof List) {
                    for (Object elem: (List)e) {
                        JSONObject element = (JSONObject) elem;
                        EntityBean bean = new EntityBean();
                        bean.setId(element.getLong("id"));
                        if (element.get("entityName") != null)
                            bean.setName(element.getString("entityName"));
                        if (element.get("meaningTag") != null)
                            bean.setMeaningTag(element.getString("meaningTag"));
                        if (element.get("classId")!=null)
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

    private MyGraphBean new_kg_graph_full_hasatts(String kgName, Long entityId,
                                                Integer distance, Integer direction, String allowAtts,
                                                String allowTypes, boolean isMergeRelation,
                                                Integer pageNo,Integer pageSize,Integer isInherit){

        MultivaluedMap<String, Object> form = new MultivaluedHashMap<>();
        form.add("kgName", kgName);
        form.add("id", entityId);
        form.add("distance", distance);
        form.add("direction", direction);
        form.add("isInherit", isInherit);
        form.add("allowAtts", allowAtts);
        form.add("allowTypes", allowTypes);
        form.add("isMergeRelation", isMergeRelation);

        MultivaluedHashMap<String,Object> query = new MultivaluedHashMap<>();
        query.add("pageNo", pageNo);
        query.add("pageSize", pageSize);

        String rs;
        try{
            String url = com.hiekn.util.CommonResource.new_plantdata_service_url+"sdk/specialGraph";
            log.info("send request to " + url);
            rs = HttpClient.sendPost(url, query, form);
        }catch(Exception e){
            log.error("invoke remote service error.", e);
            throw ServiceException.newInstance(RestReturnCode.REMOTE_INVOKE_ERROR);
        }

        RestResp<MyGraphBean> result = com.hiekn.util.JSONUtils.fromJson(rs,
                new com.google.gson.reflect.TypeToken<RestResp<MyGraphBean>>(){}.getType());
        return result.getData().getRsData().get(0);
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
                                  @ApiParam("是否查顶层父概念")@DefaultValue("false")@FormParam("isTop") Boolean isTop,
                                  @ApiParam("不查顶层父概念")@FormParam("excludeClassIds") String excludeClassIds,
                                  @QueryParam("tt") Long tt) throws InterruptedException, ExecutionException {

        if (StringUtils.isEmpty(kw) && StringUtils.isEmpty(id)) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }
        log.info("kw:"+kw+",tt:"+tt+",pageNo:"+pageNo+",pageSize:"+pageSize);
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
        if(StringUtils.isEmpty(id)) {
            rsList = this.generalSSEService.kg_semantic_seg(kw, kgName, false, true, false);
        }

        GraphBean graphBean = null;

        Long entityId = null;
        if (rsList != null && rsList.size() > 0) {
            entityId = rsList.get(0).getId();
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

                        }
                    }
                } else if (kwType == 3) { // 知识点
                    for (EntityBean entity : rsList) {
                        if (entity.getClassId() != null && knowledgeIds.contains(entity.getClassId()) && kw.equals(entity.getName())) {
                            entityId = entity.getId();
                            log.info("found knowledge:"+entityId + ",classId:"+entity.getClassId());
                            break;
                        }
                    }
                }
            }
        }
        if (!StringUtils.isEmpty(id)) {
            try {
                entityId = Long.valueOf(id);
            } catch (Exception e) {
            }
        }


        if (entityId != null) {
           graphBean = generalSSEService.kg_graph_full_hasatts(kgName, entityId, 1, 0, allowAttList, allowTypeList,
                    true, pageNo, pageSize, isInherit, isTop, excludeClassIdList);

//            graphBean = this.new_kg_graph_full_hasatts(kgName, entityId, 1, 0, allowAtts, allowTypes,
//                    true, pageNo, pageSize, isInherit);
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


    @Override
    public void afterPropertiesSet() throws Exception {
        try{
            FutureTask f = new java.util.concurrent.FutureTask(new Callable() {
                @Override
                public Object call() throws Exception {
                     getGraphKnowledge();
                     return null;
                }
            });

            ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
            executor.schedule(f, 60, TimeUnit.SECONDS);
            executor.shutdown();
        }catch(Exception e){
            log.error("initializing error:", e);
        }
    }

    private void getGraphKnowledge() {
        Map<String,Long> types = new HashMap<>();
        Set<Long> knowledgeIds = new HashSet<>();
        RestResp<SchemaBean> rest = kgSchema(0L);
        for(SchemaBean bean :rest.getData().getRsData()){
            log.info("begin to get knowledge...");
            for(TypeBean type:bean.getTypes()){
                if("人物".equals(type.getV())){
                    types.put("人物", type.getK());
                }else if ("机构".equals(type.getV())) {
                    types.put("机构", type.getK());
                }else if ("知识点".equals(type.getV())) {
                    types.put("知识点",type.getK());
                    knowledgeIds.add(type.getK());
                }

                if (knowledgeIds.contains(type.getParentId())) {
                    knowledgeIds.add(type.getK());
                }
            }
            log.info("get knowledge end."+knowledgeIds.size());
            break;
        }
        Helper.knowledgeIds = knowledgeIds;
        Helper.types = types;
    }
}
