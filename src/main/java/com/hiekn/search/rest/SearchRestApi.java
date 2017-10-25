package com.hiekn.search.rest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.Resource;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.prompt.PromptBean;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.BaikeItem;
import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.RestResp;
import com.hiekn.search.bean.result.SearchResultBean;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Controller
@Path("/p")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
@Api(tags = { "搜索" })
public class SearchRestApi {

	private static final String PATENT_INDEX = "gw_patent";
	private static final String BAIKE_INDEX = "gw_baike";

	private static Logger log = LoggerFactory.getLogger(SearchRestApi.class);

	@Resource
	private TransportClient esClient;

	@POST
	@Path("/kw")
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "搜索", notes = "搜索过滤及排序")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "成功", response = RestResp.class),
			@ApiResponse(code = 500, message = "失败") })
	public RestResp<ItemBean> kw(@ApiParam(value = "检索请求") QueryRequest request)
			throws InterruptedException, ExecutionException {
		// TODO validation check
		log.info(com.hiekn.util.JSONUtils.toJson(request));

		SearchResultBean result = new SearchResultBean(request.getKw());

		BoolQueryBuilder baikeQuery = buildQueryBaike(request);
		SearchResponse baikeResp = searchBaikeIndex(request, baikeQuery);
		if (baikeResp.getHits().getHits().length > 0) {
			SearchHit hit = baikeResp.getHits().getAt(0);
			BaikeItem item = extractBaikeItem(hit);
			result.getRsData().add(item);
		}

		BoolQueryBuilder boolQuery = buildQueryPatent(request);
		SearchResponse response = searchPatentIndex(request, boolQuery);

		result.setRsCount(response.getHits().totalHits);
		for (SearchHit hit : response.getHits()) {
			ItemBean item = extractPatentItem(hit);
			result.getRsData().add(item);
		}
		return new RestResp<ItemBean>(result.getRsData(), result.getRsCount(), request.getTt());
	}

	private String toString(Object obj) {
		return obj == null ? "" : obj.toString();
	}

	@SuppressWarnings("unchecked")
	private BaikeItem extractBaikeItem(SearchHit hit) {
		Map<String, Object> source = hit.getSource();
		BaikeItem item = new BaikeItem();
		item.setTitle(toString(source.get("title")));
		item.seteTitle(toString(source.get("etitle")));
		item.setPyTitle(toString(source.get("pinyinTitle")));
		Object contentsObj = source.get("content");
		if (contentsObj instanceof List) {
			for (Object content : (List<Object>) contentsObj) {
				item.getContents().add(toString(content));
			}
		}
		return item;
	}

	private SearchResponse searchBaikeIndex(QueryRequest request, BoolQueryBuilder boolQuery)
			throws InterruptedException, ExecutionException {
		SearchRequestBuilder srb = esClient.prepareSearch(BAIKE_INDEX);
		srb.setQuery(boolQuery).setFrom(request.getPageNo()).setSize(request.getPageSize());

		SearchResponse response = srb.execute().get();
		return response;
	}

	private SearchResponse searchPatentIndex(QueryRequest request, BoolQueryBuilder boolQuery)
			throws InterruptedException, ExecutionException {
		SearchRequestBuilder srb = esClient.prepareSearch(PATENT_INDEX);
		srb.setQuery(boolQuery).setFrom(request.getPageNo()).setSize(request.getPageSize());

		SearchResponse response = srb.execute().get();
		return response;
	}

	private BoolQueryBuilder buildQueryBaike(QueryRequest request) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
		TermQueryBuilder titleTerm = QueryBuilders.termQuery("title", request.getKw());
		TermQueryBuilder etitleTerm = QueryBuilders.termQuery("etitle", request.getKw());
		TermQueryBuilder pytitleTerm = QueryBuilders.termQuery("pinyintitle", request.getKw());
		boolQuery.should(titleTerm);
		boolQuery.should(etitleTerm);
		boolQuery.should(pytitleTerm);
		return boolQuery;
	}

	private BoolQueryBuilder buildQueryPatent(QueryRequest request) {
		TermQueryBuilder titleTerm = QueryBuilders.termQuery("title.original", request.getKw()).boost(2);
		TermQueryBuilder abstractTerm = QueryBuilders.termQuery("abstract.original", request.getKw());
		TermQueryBuilder inventorTerm = QueryBuilders.termQuery("inventors.name.original", request.getKw()).boost(1.5f);
		TermQueryBuilder agenciesTerm = QueryBuilders.termQuery("agencies_standerd.agency", request.getKw())
				.boost(1.5f);

		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
		boolQuery.should(titleTerm);
		boolQuery.should(abstractTerm);
		boolQuery.should(inventorTerm);
		boolQuery.should(agenciesTerm);
		return boolQuery;
	}

	private ItemBean extractPatentItem(SearchHit hit) {
		ItemBean item = new ItemBean();
		item.setDocType(DocType.PATENT);
		Map<String, Object> source = hit.getSource();
		item.setDocId(hit.getId().toString());

		Object titleObj = source.get("title");
		if (titleObj != null && titleObj instanceof Map) {
			item.setTitle(((Map) titleObj).get("original") != null ? ((Map) titleObj).get("original").toString() : "");
		}
		Object absObj = source.get("abstract");
		if (absObj != null && absObj instanceof Map) {
			item.setAbs(((Map) absObj).get("original") != null ? ((Map) absObj).get("original").toString() : "");
		}
		Object agenciesObj = source.get("agencies");
		if (agenciesObj != null && agenciesObj instanceof List) {
			for (Object agency : (List) agenciesObj) {
				if (agency != null) {
					item.getAgencies().add(agency.toString());
				}
			}
		}

		Object inventorsObj = source.get("inventors");
		if (inventorsObj != null && inventorsObj instanceof List) {
			for (Object inventor : (List) inventorsObj) {
				if (inventor != null && ((Map) inventor).get("name") != null) {
					Object nameObj = ((Map) inventor).get("name");
					if (nameObj instanceof Map) {
						item.getAuthors().add(((Map) nameObj).get("original").toString());
					} else {
						item.getAuthors().add(nameObj.toString());
					}
				}
			}
		}
		return item;
	}

	@POST
	@Path("/prompt")
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "提示", notes = "关键词提示")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "成功", response = RestResp.class),
			@ApiResponse(code = 500, message = "失败") })
	public RestResp<List<PromptBean>> prompt(@ApiParam(value = "提示请求") QueryRequest request) throws Exception {
		// TODO validation check
		log.info(com.hiekn.util.JSONUtils.toJson(request));
		TermQueryBuilder titleTerm = QueryBuilders.termQuery("title.original", request.getKw()).boost(2);
		TermQueryBuilder inventorTerm = QueryBuilders.termQuery("inventors.name.original", request.getKw()).boost(1.5f);
		TermQueryBuilder agenciesTerm = QueryBuilders.termQuery("agencies", request.getKw()).boost(1.5f);

		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
		boolQuery.should(titleTerm);
		boolQuery.should(inventorTerm);
		boolQuery.should(agenciesTerm);

		SearchRequestBuilder srb = esClient.prepareSearch("gw_prompt");
		srb.setQuery(boolQuery).setFrom(request.getPageNo()).setSize(request.getPageSize());

		SearchResponse response = srb.execute().get();
		List<PromptBean> promptList = new ArrayList<>();
		for (SearchHit hit : response.getHits()) {
			Object agenciesObj = hit.getSource().get("agencies");
			Object inventorsObj = hit.getSource().get("inventors");
			Object titleObj = hit.getSource().get("title");
			PromptBean bean = new PromptBean();
			bean.setType(2);
			if (agenciesObj != null && agenciesObj instanceof List) {
				for (Object agency : (List) agenciesObj) {
					if (agency != null && agency.toString().indexOf(request.getKw()) >= 0) {
						bean.setName(agency.toString());
					}
				}
			} else if (inventorsObj != null && inventorsObj instanceof List) {
				for (Object inventor : (List) inventorsObj) {
					if (inventor != null && ((Map) inventor).get("name") != null) {
						Object nameObj = ((Map) inventor).get("name");
						if (nameObj instanceof Map && ((Map) nameObj).get("original") != null) {
							String name = ((Map) nameObj).get("original").toString();
							if (name.indexOf(request.getKw()) < 0)
								continue;

							bean.setType(1);
							bean.setName(name);
						}
					}
				}
			} else if (titleObj != null && titleObj instanceof Map) {
				String title = ((Map) titleObj).get("original") != null ? ((Map) titleObj).get("original").toString()
						: "";
				if (title.indexOf(request.getKw()) >= 0) {
					bean.setType(3);
					bean.setName(title);
				}
			}

			if (bean.getName() != null && promptList.indexOf(bean) < 0) {
				promptList.add(bean);
			}
		}

		return new RestResp<List<PromptBean>>(promptList, request.getTt());
	}
}
