package com.hiekn.search.rest;

import cn.edu.ecust.sse.bean.KGResultItem;
import cn.edu.ecust.sse.bean.PromptItem;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.reflect.TypeToken;
import com.hiekn.plantdata.bean.TypeBean;
import com.hiekn.plantdata.bean.graph.*;
import com.hiekn.plantdata.bean.rest.RestReturnCode;
import com.hiekn.plantdata.exception.ServiceException;
import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.plantdata.util.HttpClient;
import com.hiekn.plantdata.util.JSONUtils;
import com.hiekn.plantdata.util.SSEResource;
import com.hiekn.search.bean.result.Code;
import com.hiekn.search.bean.result.RestResp;
import com.hiekn.search.exception.BaseException;
import com.hiekn.search.exception.JsonException;
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
import java.util.concurrent.ExecutionException;

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
        String url = SSEResource.sse_service_location + "/kg/entity/prompt";

        MultivaluedMap<String, Object> para = new MultivaluedHashMap<String, Object>();
        para.add("kgName", kgName);
        para.add("prefix", kw);
        para.add("type", 1);
        para.add("size", pageSize);
        para.add("isCaseInsensitive", !isCaseSensitive);

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

        KGResultItem<Map<String, List<PromptItem>>> kgResultItem = null;
        try {
            System.out.println(rs);
            JSONObject result = JSONUtils.fromJson(rs, JSONObject.class);
            List<EntityBean> rsList = new ArrayList<>();
            for (Object e : (List) result.get("data")) {
                JSONObject element = (JSONObject) e;
                EntityBean bean = new EntityBean();
                bean.setId(element.getLong("_id"));
                if (element.get("entity_name") != null)
                    bean.setName(element.getString("entity_name"));
                if (element.get("meaning_tag") != null)
                    bean.setMeaningTag(element.getString("meaning_tag"));
                rsList.add(bean);
            }
            return rsList;
        } catch (Exception e) {
            throw ServiceException.newInstance(RestReturnCode.REMOTE_PARSE_ERROR);
        }
    }

    private GraphBean new_kg_graph_full_hasatts(String kgName, Long entityId,
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

        RestResp<GraphBean> result = com.hiekn.util.JSONUtils.fromJson(rs,
                new com.google.gson.reflect.TypeToken<RestResp<GraphBean>>(){}.getType());
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
                                  @FormParam("statsLimit") Integer statsLimit, @FormParam("pageNo") Integer pageNo,
                                  @FormParam("pageSize") Integer pageSize, @FormParam("kwType") Integer kwType,
                                  @ApiParam("0表示不继承，1表示继承,默认0") @DefaultValue("0") @FormParam("isInherit") Integer isInherit,
                                  @QueryParam("tt") Long tt) throws InterruptedException, ExecutionException {

        if (StringUtils.isEmpty(kw)) {
            throw new BaseException(Code.PARAM_QUERY_EMPTY_ERROR.getCode());
        }
        log.info("kw+"+kw+",tt:"+tt);
        List<Long> allowAttList = null;
        List<Long> allowTypeList = null;

        if (pageNo == null) {
            pageNo = 0;
        }

        if (pageSize == null) {
            pageSize = 10;
        }
        try {
            allowAttList = JSONUtils.fromJson(allowAtts, new TypeToken<List<Long>>() {
            }.getType());
            allowTypeList = JSONUtils.fromJson(allowTypes, new TypeToken<List<Long>>() {
            }.getType());
        } catch (Exception e) {
            log.error("parse to json error", e);
            throw JsonException.newInstance();
        }
        String[] kws = kw.trim().split(" ");
        if (kws.length > 0) {
            kw = kws[0];
        }
        List<EntityBean> rsList = this.generalSSEService.kg_semantic_seg(kw, kgName, false, true, false);
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
//            graphBean = generalSSEService.kg_graph_full_hasatts(kgName, entityId, 1, 0, allowAttList, allowTypeList,
//                    true, pageNo, pageSize, isInherit);

            graphBean = this.new_kg_graph_full_hasatts(kgName, entityId, 1, 0, allowAtts, allowTypes,
                    true, pageNo, pageSize, isInherit);
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

    Map<String,Long> types = new HashMap<>();
    Set<Long> knowledgeIds = new HashSet<>();
    @Override
    public void afterPropertiesSet() throws Exception {
        try{
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
        }catch(Exception e){
            log.error("initializing error:", e);
        }
    }
}
