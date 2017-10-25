package com.hiekn.search.rest;

import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.annotation.Resource;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

import com.hiekn.plantdata.bean.graph.EntityBean;
import com.hiekn.plantdata.bean.graph.GraphBean;
import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.RestResp;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Controller
@Path("/g")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
@Api(tags = { "搜索" })
public class KGRestApi {

	private String kgName = "u89_graph_ea33277a";
	@Resource
	private IGeneralSSEService generalSSEService;
	
	private static Logger log = LoggerFactory.getLogger(SearchRestApi.class);

	@POST
	@Path("/kg")
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "搜索", notes = "搜索过滤及排序")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "成功", response = RestResp.class),
			@ApiResponse(code = 500, message = "失败") })
	public RestResp<GraphBean> kg(@ApiParam(value = "检索请求") String kw)
			throws InterruptedException, ExecutionException{
		// TODO validation check
		
		log.info(kw);
		
		List<EntityBean> rsList = this.generalSSEService.kg_semantic_seg(kw, kgName, false, true, false);
		
		GraphBean graphBean = null;

		
		if(rsList!=null && rsList.size()>0){
			
			Long entityId = rsList.get(0).getId();
			
			graphBean = generalSSEService.kg_graph_full_hasatts(kgName, entityId, 1, 0, null, null, true);
		}
		
		return new RestResp<GraphBean>(graphBean, 0L);
	}
}
