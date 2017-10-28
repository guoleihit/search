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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import com.google.gson.reflect.TypeToken;
import com.hiekn.plantdata.bean.graph.EntityBean;
import com.hiekn.plantdata.bean.graph.GraphBean;
import com.hiekn.plantdata.bean.graph.GraphStatBean;
import com.hiekn.plantdata.bean.graph.PathAGBean;
import com.hiekn.plantdata.bean.graph.RelationBean;
import com.hiekn.plantdata.bean.graph.SchemaBean;
import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.plantdata.util.JSONUtils;
import com.hiekn.search.bean.result.RestResp;
import com.hiekn.search.exception.JsonException;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Controller
@Path("/g")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
@Api(tags = { "搜索" })
public class KGRestApi {

	@Value("${kg_name}")
	private String kgName;
	@Resource
	private IGeneralSSEService generalSSEService;

	private static Logger log = LoggerFactory.getLogger(KGRestApi.class);

	@POST
	@Path("/graph")
	@ApiOperation(value = "图谱")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "成功", response = RestResp.class),
			@ApiResponse(code = 500, message = "失败") })
	public RestResp<GraphBean> kg(@FormParam("kw") String kw, 
			@FormParam("allowAtts") String allowAtts,
			@FormParam("allowTypes") String allowTypes, 
			@FormParam("entitiesLimit") Integer entitiesLimit,
			@FormParam("relationsLimit") Integer relationsLimit, 
			@FormParam("conceptsLimit") Integer conceptsLimit,
			@FormParam("statsLimit") Integer statsLimit, 
			@QueryParam("tt") Long tt)
			throws InterruptedException, ExecutionException {

		List<Long> allowAttList = null;
		List<Long> allowTypeList = null;

		try {
			allowAttList = JSONUtils.fromJson(allowAtts, new TypeToken<List<Long>>() {
			}.getType());
			allowTypeList = JSONUtils.fromJson(allowTypes, new TypeToken<List<Long>>() {
			}.getType());
		} catch (Exception e) {
			log.error("parse to json error", e);
			throw JsonException.newInstance();
		}
		List<EntityBean> rsList = this.generalSSEService.kg_semantic_seg(kw, kgName, false, true, false);
		GraphBean graphBean = null;

		if (rsList != null && rsList.size() > 0) {
			Long entityId = rsList.get(0).getId();
			graphBean = generalSSEService.kg_graph_full_hasatts(kgName, entityId, 1, 0, allowAttList, allowTypeList,
					true);
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

		return new RestResp<GraphBean>(graphBean, 0L);
	}

	@POST
	@Path("/schema")
	@ApiOperation(value = "图schema")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "成功", response = RestResp.class),
			@ApiResponse(code = 500, message = "失败") })
	public RestResp<SchemaBean> kgSchema(@QueryParam("tt") Long tt) {
		SchemaBean schema = generalSSEService.getAllAtts(kgName);
		schema.setTypes(this.generalSSEService.getAllTypes(kgName));
		return new RestResp<SchemaBean>(schema, tt);
	}
}
