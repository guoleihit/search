package com.hiekn.service;

import com.alibaba.fastjson.JSONObject;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.PaperDetail;
import com.hiekn.search.bean.result.PaperItem;
import com.hiekn.search.bean.result.SearchResultBean;
import com.hiekn.util.CommonResource;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.hiekn.service.Helper.*;

public class PaperService extends AbstractService{

	public PaperService (TransportClient client) {
		esClient = client;
	}

	public ItemBean extractDetail(SearchHit hit) {
		PaperDetail item = new PaperDetail();
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

		Object categories = source.get("categories");
		List<String> cts = toStringList(categories);
		if (!cts.isEmpty()) {
			item.setCategories(cts);
		}

		Object inventorsObj = source.get("persons");
		List<String> inventors = toStringListByKey(inventorsObj, "name");
		if (!inventors.isEmpty()) {
			item.setAuthors(inventors);
		}
		if (source.get("earliest_publication_date") != null) {
			item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
		}

		item.setJournal(getString(source.get("journal")));
		item.setCiteCount(getString(source.get("citeCount")));
		return item;
	
	}

	@SuppressWarnings("rawtypes")
	public PaperItem extractItem(SearchHit hit) {
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
		if (source.get("earliest_publication_date") != null) {
			item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
		}


		//highlight
		if (hit.getHighlightFields() != null) {
			for (Map.Entry<String, HighlightField> entry : hit.getHighlightFields().entrySet()) {
				Text[] frags = entry.getValue().getFragments();
				switch (entry.getKey()) {
					case "title":
						if (frags != null && frags.length > 0) {
							item.setTitle(frags[0].string());
						}
						break;
					case "abs":
						if (frags != null && frags.length > 0) {
							item.setAbs(frags[0].string());
						}
						break;
					case "keywords.keyword":
						if (frags != null && frags.length > 0) {
							ListIterator<String> itr = item.getKeywords().listIterator();
							setHighlightElements(frags, itr);
						}
						break;
					case "persons.name.keyword":
						if (frags != null && frags.length > 0) {
							ListIterator<String> itr = item.getAuthors().listIterator();
							setHighlightElements(frags, itr);
						}
						break;
				}
			}
		}
		return item;
	}


    public BoolQueryBuilder buildQuery(QueryRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);

        TermQueryBuilder titleTerm = QueryBuilders.termQuery("title", request.getKw()).boost(2);
        TermQueryBuilder abstractTerm = QueryBuilders.termQuery("abs", request.getKw());
        TermQueryBuilder authorTerm = QueryBuilders.termQuery("persons.name.keyword", request.getKw()).boost(1.5f);
        TermQueryBuilder orgsTerm = QueryBuilders.termQuery("persons.orgs.name.keyword", request.getKw()).boost(1.5f);
        TermQueryBuilder kwsTerm = QueryBuilders.termQuery("keywords.keyword", request.getKw()).boost(1.5f);
        TermQueryBuilder annotationTagTerm = QueryBuilders.termQuery("annotation_tag.name", request.getKw())
                .boost(1.5f);

        BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        termQuery.should(titleTerm);
        termQuery.should(abstractTerm);
        termQuery.should(authorTerm);
        termQuery.should(kwsTerm);
        termQuery.should(orgsTerm);
        termQuery.should(annotationTagTerm);

        boolQuery.must(termQuery);
        boolQuery.filter(QueryBuilders.termQuery("_type", "paper_data"));
        return boolQuery;
    }

	public SearchResultBean doSearch(QueryRequest request) throws ExecutionException, InterruptedException {
		BoolQueryBuilder boolQuery = buildQuery(request);
		SearchRequestBuilder srb = esClient.prepareSearch(CommonResource.PAPER_INDEX);
		HighlightBuilder highlighter = new HighlightBuilder().field("title");
		srb.highlighter(highlighter).setQuery(boolQuery).setFrom(request.getPageNo() - 1)
				.setSize(request.getPageSize());
		SearchResponse response =  srb.execute().get();
		SearchResultBean result = new SearchResultBean(request.getKw());
		result.setRsCount(response.getHits().totalHits);
		for (SearchHit hit : response.getHits()) {
			ItemBean item = extractItem(hit);
			result.getRsData().add(item);
		}
		return result;
	}
}
