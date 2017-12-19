package com.hiekn.search.rest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hiekn.search.bean.result.RestResp;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.Map;

@Controller
@Path("/map")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
@Api(tags = {"可视化数据"})
public class Visualization implements InitializingBean, DisposableBean {

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
