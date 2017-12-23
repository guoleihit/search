package com.hiekn.search.rest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hiekn.plantdata.bean.graph.EntityBean;
import com.hiekn.plantdata.bean.graph.GraphBean;
import com.hiekn.search.bean.result.RestResp;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
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

    private MongoClient mongoClient;

    private MongoDatabase mongoDB;

    @POST
    @Path("/data")
    @ApiOperation(value = "")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<java.util.Map> data(@QueryParam("id") String id, @FormParam("name") String name,
                                             @QueryParam("tt") Long tt) throws Exception {
        Document d = null;
        if(!StringUtils.isEmpty(id)){
            d = mongoDB.getCollection("knowledge_map_data").find(Filters.eq("id", Long.valueOf(id))).first();
        }else if(!StringUtils.isEmpty(name)){
            d = mongoDB.getCollection("knowledge_map_data").find(Filters.eq("name",name)).first();
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
    @Path("/graph/tree")
    @ApiOperation(value = "树图")
    @ApiResponses(value = {@ApiResponse(code = 200, message = "成功", response = RestResp.class),
            @ApiResponse(code = 500, message = "失败")})
    public RestResp<Map<String, Object>> kgTree(@FormParam("kw") String kw, @FormParam("id") String id, @FormParam("allowAtts") String allowAtts,
                                  @FormParam("allowTypes") String allowTypes, @FormParam("entitiesLimit") Integer entitiesLimit,
                                  @FormParam("relationsLimit") Integer relationsLimit, @FormParam("conceptsLimit") Integer conceptsLimit,
                                  @FormParam("statsLimit") Integer statsLimit, @QueryParam("pageNo") Integer pageNo,
                                  @QueryParam("pageSize") Integer pageSize, @FormParam("kwType") Integer kwType,
                                  @ApiParam("0表示不继承，1表示继承,默认0") @DefaultValue("0") @FormParam("isInherit") Integer isInherit,
                                  @QueryParam("tt") Long tt) throws InterruptedException, ExecutionException {
        RestResp<GraphBean> restGraph = kgApi.kg(kw,id,allowAtts,allowTypes,entitiesLimit,relationsLimit,conceptsLimit,statsLimit,pageNo,pageSize,kwType,isInherit,tt);
        GraphBean bean = restGraph.getData().getRsData().get(0);
        if (bean != null) {
            List<EntityBean> entityBeanList = bean.getEntityList();
            if (entityBeanList.size() > 0) {
                Map<Long, List<EntityBean>> treeClassId = new HashMap<>();
                for (int i = 1; i < entityBeanList.size(); i++) {
                    EntityBean entity = entityBeanList.get(i);

                    Long classId = entity.getClassId();
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
                    child.put("classId",classId);
                    child.put("children",children);
                    topChildren.add(child);
                }

            }
        }
        return new RestResp<>();
    }
    @Override
    public void destroy() throws Exception {
        if(mongoClient != null){
            mongoClient.close();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        mongoClient = new MongoClient(mongoIP, Integer.valueOf(mongoPort));
        mongoDB = mongoClient.getDatabase("knowledge_map");
    }
}
