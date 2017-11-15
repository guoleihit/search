package com.hiekn.service;

import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.PaperDetail;
import com.hiekn.search.bean.result.PaperItem;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static com.hiekn.service.Helper.*;

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
}
