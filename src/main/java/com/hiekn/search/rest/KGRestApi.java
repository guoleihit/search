package com.hiekn.search.rest;

import java.util.concurrent.ExecutionException;

import javax.annotation.Resource;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

import com.hiekn.plantdata.service.SDKService;
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

	private static final String PATENT_INDEX = "gw_patent";
	private static final String BAIKE_INDEX = "gw_baike";

	@Resource
	private SDKService sdkService;
	
	private static Logger log = LoggerFactory.getLogger(SearchRestApi.class);

	@POST
	@Path("/kg")
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "搜索", notes = "搜索过滤及排序")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "成功", response = RestResp.class),
			@ApiResponse(code = 500, message = "失败") })
	public RestResp<ItemBean> kg(@ApiParam(value = "检索请求") QueryRequest request)
			throws InterruptedException, ExecutionException {
		// TODO validation check
		log.info(com.hiekn.util.JSONUtils.toJson(request));

		return new RestResp<ItemBean>(null);
	}
}
