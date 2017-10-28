package com.hiekn.search.rest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.Resource;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.prompt.PromptBean;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.BaikeItem;
import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.PaperItem;
import com.hiekn.search.bean.result.PatentItem;
import com.hiekn.search.bean.result.RestResp;
import com.hiekn.search.bean.result.SearchResultBean;
import com.hiekn.search.bean.result.StandardItem;

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

	private static final String PROMPT_INDEX = "gw_prompt";
	private static final String STANDARD_INDEX = "gw_standard";
	private static final String PATENT_INDEX = "gw_patent";
	private static final String PAPER_INDEX = "gw_paper";
	private static final String BAIKE_INDEX = "gw_baike";

	private static Logger log = LoggerFactory.getLogger(SearchRestApi.class);

	@Resource
	private TransportClient esClient;

	@GET
	@Path("/baike")
	@ApiOperation(value = "搜索百科")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "成功", response = RestResp.class),
			@ApiResponse(code = 500, message = "失败") })
	public RestResp<SearchResultBean> baike(@QueryParam("baike") String baike, @QueryParam("pageNo") Integer pageNo,
			@QueryParam("pageSize") Integer pageSize, @QueryParam("tt") Long tt) throws Exception {
		log.info("search baike item:" + baike);
		SearchResultBean result = new SearchResultBean(baike);

		BoolQueryBuilder baikeQuery = buildQueryBaike(baike);
		QueryRequest request = new QueryRequest();
		request.setKw(baike);
		request.setPageNo(pageNo);
		request.setPageSize(pageSize);
		SearchResponse baikeResp = searchBaikeIndex(request, baikeQuery);
		if (baikeResp.getHits().getHits().length > 0) {
			SearchHit hit = baikeResp.getHits().getAt(0);
			BaikeItem item = extractBaikeItem(hit);
			result.getRsData().add(item);
		}
		return new RestResp<SearchResultBean>(result, request.getTt());
	}

	@POST
	@Path("/kw")
	@Consumes(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "搜索", notes = "搜索过滤及排序")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "成功", response = RestResp.class),
			@ApiResponse(code = 500, message = "失败") })
	public RestResp<SearchResultBean> kw(@ApiParam(value = "检索请求") QueryRequest request)
			throws InterruptedException, ExecutionException {
		log.info(com.hiekn.util.JSONUtils.toJson(request));

		SearchResultBean result = new SearchResultBean(request.getKw());
		SearchResponse response = null;
		if (request.getDocType() == null) {
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
			BoolQueryBuilder pantentQuery = buildQueryPatent(request);
			BoolQueryBuilder paperQuery = buildQueryPaper(request);
			BoolQueryBuilder standardQuery = buildQueryStandard(request);
			boolQuery.should(paperQuery);
			boolQuery.should(pantentQuery);
			boolQuery.should(standardQuery);
			response = searchIndexes(request, boolQuery, Arrays.asList(new String[] { PAPER_INDEX, PATENT_INDEX, STANDARD_INDEX }));
		} else if (DocType.PATENT.equals(request.getDocType())) {
			BoolQueryBuilder boolQuery = buildQueryPatent(request);
			response = searchIndexes(request, boolQuery, Arrays.asList(new String[] { PATENT_INDEX }));
		} else if (DocType.PAPER.equals(request.getDocType())) {
			BoolQueryBuilder boolQuery = buildQueryPaper(request);
			response = searchIndexes(request, boolQuery, Arrays.asList(new String[] { PAPER_INDEX }));
		} else if (DocType.STANDARD.equals(request.getDocType())) {
			BoolQueryBuilder boolQuery = buildQueryStandard(request);
			response = searchIndexes(request, boolQuery, Arrays.asList(new String[] { STANDARD_INDEX }));
		}

		result.setRsCount(response.getHits().totalHits);
		for (SearchHit hit : response.getHits()) {
			ItemBean item = null;
			if (hit.getType().equals("patent_data"))
				item = extractPatentItem(hit);
			else if (hit.getType().equals("paper_data"))
				item = extracePaperItem(hit);
			else if (hit.getType().equals("standard_data"))
				item = extraceStandardItem(hit);
			else {
				continue;
			}
			result.getRsData().add(item);
		}

		Histogram yearAgg = response.getAggregations().get("publication_year");
		KVBean<String, Map<String, ? extends Object>> yearFilter = new KVBean<>();
		yearFilter.setD("发表年份");
		yearFilter.setK("earliest_publication_date");
		Map<String, Long> yearMap = new HashMap<>();
		for (Histogram.Bucket bucket : yearAgg.getBuckets()) {
			if (bucket.getKey() instanceof Number) {
				Double year = Double.valueOf(bucket.getKeyAsString());
				year = year / 10000;
				yearMap.put(String.valueOf(year.intValue()), bucket.getDocCount());
			}
		}
		yearFilter.setV(yearMap);
		result.getFilters().add(yearFilter);

		Terms docTypes = response.getAggregations().get("document_type");
		KVBean<String, Map<String, ? extends Object>> docTypeFilter = new KVBean<>();
		docTypeFilter.setD("资源类型");
		docTypeFilter.setK("_type");
		Map<String, Long> docMap = new HashMap<>();
		for (Terms.Bucket bucket : docTypes.getBuckets()) {
			docMap.put(bucket.getKeyAsString(), bucket.getDocCount());
		}
		docTypeFilter.setV(docMap);
		result.getFilters().add(docTypeFilter);

		Terms knowledgeClasses = response.getAggregations().get("knowledge_class");
		KVBean<String, Map<String, ? extends Object>> knowledgeClassFilter = new KVBean<>();
		knowledgeClassFilter.setD("知识体系");
		knowledgeClassFilter.setK(getAnnotationFieldName(request));
		Map<String, Long> knowledgeMap = new HashMap<>();
		for (Terms.Bucket bucket : knowledgeClasses.getBuckets()) {
			knowledgeMap.put(bucket.getKeyAsString(), bucket.getDocCount());
		}
		knowledgeClassFilter.setV(knowledgeMap);
		result.getFilters().add(knowledgeClassFilter);

		return new RestResp<SearchResultBean>(result, request.getTt());
	}

	private ItemBean extraceStandardItem(SearchHit hit) {
		StandardItem item = new StandardItem();
		Map<String, Object> source = hit.getSource();
		item.setDocId(hit.getId().toString());

		Object titleObj = source.get("title");
		if (titleObj != null) {
			item.setTitle(titleObj.toString());
		}
		Object absObj = source.get("abs");
		if (absObj != null) {
			item.setAbs(absObj.toString());
		}

		Object inventorsObj = source.get("persons");
		List<String> inventors = toStringList(inventorsObj);
		if (!inventors.isEmpty()) {
			item.setAuthors(inventors);
		}
		return item;
	}

	private List<String> toStringList(Object keywords) {
		List<String> kws = new ArrayList<>();
		if (keywords != null && keywords instanceof List) {
			for (Object kw : (List) keywords) {
				if (kw != null) {
					kws.add(kw.toString());
				}
			}
		}
		return kws;
	}

	private BoolQueryBuilder buildQueryStandard(QueryRequest request) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

		makeFilters(request, boolQuery);

		TermQueryBuilder titleTerm = QueryBuilders.termQuery("title", request.getKw()).boost(2);
		TermQueryBuilder abstractTerm = QueryBuilders.termQuery("abs", request.getKw());
		TermQueryBuilder authorTerm = QueryBuilders.termQuery("persons.keyword", request.getKw()).boost(1.5f);
		TermQueryBuilder kwsTerm = QueryBuilders.termQuery("keywords", request.getKw()).boost(1.5f);

		BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
		termQuery.should(titleTerm);
		termQuery.should(abstractTerm);
		termQuery.should(authorTerm);
		termQuery.should(kwsTerm);

		boolQuery.must(termQuery);
		boolQuery.must(QueryBuilders.termQuery("_type", "standard_data"));
		return boolQuery;
	}

	@SuppressWarnings("rawtypes")
	private PaperItem extracePaperItem(SearchHit hit) {
		PaperItem item = new PaperItem();
		Map<String, Object> source = hit.getSource();
		item.setDocId(hit.getId().toString());

		Object titleObj = source.get("title");
		if (titleObj != null) {
			item.setTitle(titleObj.toString());
		}
		Object absObj = source.get("abs");
		if (absObj != null) {
			item.setAbs(absObj.toString());
		}
		Object keywords = source.get("keywords");
		List<String> kws = toStringList(keywords);
		if (!kws.isEmpty()) {
			item.setKeywords(kws);
		}

		Object inventorsObj = source.get("persons");
		List<String> inventors = new ArrayList<>();
		if (inventorsObj != null && inventorsObj instanceof List) {
			for (Object inventor : (List) inventorsObj) {
				if (inventor != null && ((Map) inventor).get("name") != null) {
					inventors.add(((Map) inventor).get("name").toString());
				}
			}
		}
		if (!inventors.isEmpty()) {
			item.setAuthors(inventors);
		}
		return item;
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
		srb.setQuery(boolQuery).setFrom(request.getPageNo() - 1).setSize(request.getPageSize());

		SearchResponse response = srb.execute().get();
		return response;
	}

	private SearchResponse searchIndexes(QueryRequest request, BoolQueryBuilder boolQuery, List<String> indices)
			throws InterruptedException, ExecutionException {
		SearchRequestBuilder srb = esClient.prepareSearch(indices.toArray(new String[] {}));
		srb.setQuery(boolQuery).setFrom(request.getPageNo() - 1).setSize(request.getPageSize());

		AggregationBuilder aggYear = AggregationBuilders.histogram("publication_year")
				.field("earliest_publication_date").interval(10000);
		srb.addAggregation(aggYear);

		AggregationBuilder docTypes = AggregationBuilders.terms("document_type").field("_type");
		srb.addAggregation(docTypes);

		String annotationField = getAnnotationFieldName(request);
		AggregationBuilder knowledge = AggregationBuilders.terms("knowledge_class").field(annotationField);
		srb.addAggregation(knowledge);

		System.out.println(srb.toString());
		SearchResponse response = srb.execute().get();
		return response;
	}

	private String getAnnotationFieldName(QueryRequest request) {
		String annotationField = "annotation_1.name";
		if (request.getFilters() != null) {
			for (KVBean<String, List<String>> filter : request.getFilters()) {
				if ("annotation_1.name".equals(filter.getK())) {
					annotationField = "annotation_2.name";
					break;
				} else if ("annotation_2.name".equals(filter.getK())) {
					annotationField = "annotation_3.name";
					break;
				}
			}
		}
		return annotationField;
	}

	private BoolQueryBuilder buildQueryBaike(String baike) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
		TermQueryBuilder titleTerm = QueryBuilders.termQuery("title", baike);
		TermQueryBuilder etitleTerm = QueryBuilders.termQuery("etitle", baike);
		TermQueryBuilder pytitleTerm = QueryBuilders.termQuery("pinyintitle", baike);
		boolQuery.should(titleTerm);
		boolQuery.should(etitleTerm);
		boolQuery.should(pytitleTerm);
		return boolQuery;
	}

	private BoolQueryBuilder buildQueryPatent(QueryRequest request) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

		makeFilters(request, boolQuery);

		TermQueryBuilder titleTerm = QueryBuilders.termQuery("title.original", request.getKw()).boost(1.5f);
		TermQueryBuilder abstractTerm = QueryBuilders.termQuery("abstract.original", request.getKw());
		TermQueryBuilder inventorTerm = QueryBuilders.termQuery("inventors.name.original.keyword", request.getKw()).boost(2);
		TermQueryBuilder applicantTerm = QueryBuilders.termQuery("applicants.name.original", request.getKw())
				.boost(1.5f);
		TermQueryBuilder agenciesTerm = QueryBuilders.termQuery("agencies_standerd.agency", request.getKw())
				.boost(1.5f);

		BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
		termQuery.should(titleTerm);
		termQuery.should(abstractTerm);
		termQuery.should(inventorTerm);
		termQuery.should(agenciesTerm);
		termQuery.should(applicantTerm);

		boolQuery.must(termQuery);
		boolQuery.must(QueryBuilders.termQuery("_type", "patent_data"));
		return boolQuery;
	}

	private void makeFilters(QueryRequest request, BoolQueryBuilder boolQuery) {
		if (request.getFilters() != null) {
			System.out.println(request.getFilters());
			List<KVBean<String, List<String>>> filters = request.getFilters();
			for (KVBean<String, List<String>> filter : filters) {
				if ("earliest_publication_date".equals(filter.getK())) {
					BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
					for (String v : filter.getV()) {
						filterQuery.should(QueryBuilders.rangeQuery(filter.getK()).gt(Long.valueOf(v + "0000"))
								.lt(Long.valueOf(v + "9999")));
					}
					boolQuery.must(filterQuery);
				} else if ("_type".equals(filter.getK()) || filter.getK().startsWith("annotation_")) {
					BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
					for (String v : filter.getV()) {
						filterQuery.should(QueryBuilders.termQuery(filter.getK(), v));
					}
					boolQuery.must(filterQuery);
				}
			}
		}
	}

	private BoolQueryBuilder buildQueryPaper(QueryRequest request) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

		makeFilters(request, boolQuery);

		TermQueryBuilder titleTerm = QueryBuilders.termQuery("title", request.getKw()).boost(2);
		TermQueryBuilder abstractTerm = QueryBuilders.termQuery("abs", request.getKw());
		TermQueryBuilder authorTerm = QueryBuilders.termQuery("persons.name.keyword", request.getKw()).boost(1.5f);
		TermQueryBuilder kwsTerm = QueryBuilders.termQuery("keywords", request.getKw()).boost(1.5f);

		BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
		termQuery.should(titleTerm);
		termQuery.should(abstractTerm);
		termQuery.should(authorTerm);
		termQuery.should(kwsTerm);

		boolQuery.must(termQuery);
		boolQuery.must(QueryBuilders.termQuery("_type", "paper_data"));
		return boolQuery;
	}

	@SuppressWarnings("rawtypes")
	private ItemBean extractPatentItem(SearchHit hit) {
		PatentItem item = new PatentItem();
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
		List<String> agencies = toStringList(agenciesObj);
		if (!agencies.isEmpty()) {
			item.setAgencies(agencies);
		}

		Object applicantsObj = source.get("applicants");
		List<String> applicants = getStringListFromNameOrgObject(applicantsObj);
		if (!applicants.isEmpty()) {
			item.setApplicants(applicants);
		}

		Object inventorsObj = source.get("inventors");
		List<String> inventors = getStringListFromNameOrgObject(inventorsObj);
		if (!inventors.isEmpty()) {
			item.setAuthors(inventors);
		}
		return item;
	}

	private List<String> getStringListFromNameOrgObject(Object inventorsObj) {
		List<String> inventors = new ArrayList<>();
		if (inventorsObj != null && inventorsObj instanceof List) {
			for (Object inventor : (List) inventorsObj) {
				if (inventor != null && ((Map) inventor).get("name") != null) {
					Object nameObj = ((Map) inventor).get("name");
					if (nameObj instanceof Map) {
						inventors.add(((Map) nameObj).get("original").toString());
					} else {
						inventors.add(nameObj.toString());
					}
				}
			}
		}
		return inventors;
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
		TermQueryBuilder titleTerm = QueryBuilders.termQuery("name", request.getKw()).boost(2);

		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
		boolQuery.should(titleTerm);

		SearchRequestBuilder srb = esClient.prepareSearch(PROMPT_INDEX);
		srb.setQuery(boolQuery).setFrom(request.getPageNo() - 1).setSize(request.getPageSize());

		SearchResponse response = srb.execute().get();
		List<PromptBean> promptList = new ArrayList<>();
		for (SearchHit hit : response.getHits()) {
			Map<String, Object> source = hit.getSource();
			Object nameObj = source.get("name");
			Object typeObj = source.get("type");
			Object descObj = source.get("description");
			PromptBean bean = new PromptBean();
			if (typeObj != null)
				bean.setType(Integer.valueOf(toString(typeObj)));
			bean.setName(toString(nameObj));
			bean.setDescription(toString(descObj));

			if (bean.getName() != null && promptList.indexOf(bean) < 0) {
				promptList.add(bean);
			}
		}

		return new RestResp<List<PromptBean>>(promptList, request.getTt());
	}
}
