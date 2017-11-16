package com.hiekn.service;

import static com.hiekn.service.Helper.toDateString;
import static com.hiekn.service.Helper.toStringList;
import static com.hiekn.service.Helper.getString;
import static com.hiekn.service.Helper.makeFilters;

import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.PictureDetail;
import com.hiekn.search.bean.result.PictureItem;

public class PictureService {
	public ItemBean extractPictureDetail(SearchHit hit) {
		PictureDetail item = new PictureDetail();

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

		Object keywords = source.get("keywords");
		List<String> keywordList = toStringList(keywords);
		if (!keywordList.isEmpty()) {
			item.setKeywords(keywordList);
		}

		if (source.get("earliest_publication_date") != null) {
			item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
		}

		Object categories = source.get("categories");
		List<String> categoryList = toStringList(categories);
		if (!categoryList.isEmpty()) {
			item.setCategories(categoryList);
		}

		if (source.get("allowPub") != null) {
			item.setAllowedPublication(Integer.valueOf(1).equals(source.get("allowPub")));
		}
		item.setPhotoPlace(getString(source.get("place")));
		item.setStoreSize(getString(source.get("storeSize")));
		item.setSize(getString(source.get("size")));
		return item;
	}

	public ItemBean extractPictureItem(SearchHit hit) {
		PictureItem item = new PictureItem();

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

		Object keywords = source.get("keywords");
		List<String> keywordList = toStringList(keywords);
		if (!keywordList.isEmpty()) {
			item.setKeywords(keywordList);
		}

		if (source.get("earliest_publication_date") != null) {
			item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
		}
		return item;
	}
	
	public BoolQueryBuilder buildQueryPicture(QueryRequest request) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

		makeFilters(request, boolQuery);

		TermQueryBuilder titleTerm = QueryBuilders.termQuery("title", request.getKw()).boost(2);
		TermQueryBuilder authorTerm = QueryBuilders.termQuery("persons", request.getKw()).boost(1.5f);
		TermQueryBuilder kwsTerm = QueryBuilders.termQuery("keywords", request.getKw()).boost(1.5f);
		TermQueryBuilder classificationTerm = QueryBuilders.termQuery("categories", request.getKw()).boost(1.5f);

		BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
		termQuery.should(titleTerm);
		termQuery.should(authorTerm);
		termQuery.should(kwsTerm);
		termQuery.should(classificationTerm);

		boolQuery.must(termQuery);
		boolQuery.filter(QueryBuilders.termQuery("_type", "picture_data"));
		return boolQuery;
	}
}
