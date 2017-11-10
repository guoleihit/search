package com.hiekn.service;

import static com.hiekn.service.Helper.getString;
import static com.hiekn.service.Helper.toDateString;
import static com.hiekn.service.Helper.toStringList;
import static com.hiekn.service.Helper.toStringListByKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.SearchHit;

import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.PaperDetail;
import com.hiekn.search.bean.result.PaperItem;

public class PaperService {

	public static ItemBean extractPaperDetail(SearchHit hit) {
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
	public static PaperItem extractPaperItem(SearchHit hit) {
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
		return item;
	}
}
