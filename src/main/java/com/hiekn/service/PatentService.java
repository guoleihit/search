package com.hiekn.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.SearchHit;

import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.PatentDetail;

public class PatentService extends Helper {

	@SuppressWarnings("rawtypes")
	public static ItemBean extractPatentDetail(SearchHit hit) {
		PatentDetail item = new PatentDetail();

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

		if (source.get("application_number") != null) {
			item.setApplicationNumber(source.get("application_number").toString());
		}
		if (source.get("publication_number") != null) {
			item.setPublicationNumber(source.get("publication_number").toString());
		}
		if (source.get("application_date") != null) {
			item.setApplicationNumber(toDateString(source.get("application_date").toString(), "-"));
		}
		if (source.get("earliest_publication_date") != null) {
			item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
		}
		Object applicantsObj = source.get("applicants");

		List<Map<String, Object>> applicants = new ArrayList<>();
		if (applicantsObj != null && applicantsObj instanceof List) {
			for (Object applicant : (List) applicantsObj) {
				Map<String, Object> app = new HashMap<>();
				if (applicant == null) {
					continue;
				}
				if (((Map) applicant).get("name") != null) {
					Object nameObj = ((Map) applicant).get("name");
					if (nameObj instanceof Map) {
						app.put("name", ((Map) nameObj).get("original"));
					} else {
						app.put("name", nameObj);
					}
				}
				if (((Map) applicant).get("address") != null) {
					Object addressObj = ((Map) applicant).get("address");
					if (addressObj instanceof Map) {
						app.put("address", ((Map) addressObj).get("original"));
					} else {
						app.put("address", addressObj);
					}
				}
				if (((Map) applicant).get("type") != null) {
					app.put("type", ((Map) applicant).get("type"));
				}
				applicants.add(app);
			}
		}

		if (!applicants.isEmpty()) {
			item.setApplicants(applicants);
		}

		Object inventorsObj = source.get("inventors");
		List<String> inventors = getStringListFromNameOrgObject(inventorsObj);
		if (!inventors.isEmpty()) {
			item.setAuthors(inventors);
		}

		Object mainIpcObj = source.get("main_ipc");
		if (mainIpcObj != null && mainIpcObj instanceof Map) {
			item.setMainIPC(String.valueOf(((Map) mainIpcObj).get("ipc")));
		}

		Object ipcsObj = source.get("ipcs");
		List<String> ipcs = toStringListByKey(ipcsObj, "ipc");
		if (!ipcs.isEmpty()) {
			item.setIpces(ipcs);
		}
		try {
			if (source.get("fulltext_pages") != null) {
				item.setPages(Integer.valueOf(source.get("fulltext_pages").toString()));
			}
		} catch (Exception e) {
			item.setPages(0);
		}
		return item;
	}
}
