package com.hiekn.service;

import java.util.*;

import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.SearchResultBean;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

public class Helper {

	public static String getString(Object obj) {
		return obj == null ? "" : obj.toString();
	}

	public static List<String> toStringListByKey(Object keywords, String key) {
		List<String> kws = new ArrayList<>();
		if (keywords != null && keywords instanceof List) {
			for (Object kw : (List) keywords) {
				if (kw != null && kw instanceof Map) {
					kws.add(String.valueOf(((Map) kw).get(key)));
				}
			}
		}
		return kws;
	}

	public static List<String> toStringList(Object keywords) {
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

	public static List<String> getStringListFromNameOrgObject(Object inventorsObj) {
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

	public static String getAnnotationFieldName(QueryRequest request) {
		String annotationField = "annotation_1.name";
		if (request.getFilters() != null) {
			for (KVBean<String, List<String>> filter : request.getFilters()) {
				if ("annotation_1.name".equals(filter.getK())) {
					annotationField = "annotation_2.name";
					break;
				} else if ("annotation_2.name".equals(filter.getK())) {
					annotationField = "annotation_3.name";
					break;
				} else if ("annotation_3.name".equals(filter.getK())) {
					return null;
				}
			}
		}
		return annotationField;
	}

    public static void setKnowledgeAggResult(SearchResponse response, SearchResultBean result, String annotation) {
        if (annotation != null) {
            Terms knowledgeClasses = response.getAggregations().get("knowledge_class");
            KVBean<String, Map<String, ? extends Object>> knowledgeClassFilter = new KVBean<>();
            knowledgeClassFilter.setD("知识体系");
            knowledgeClassFilter.setK(annotation);
            Map<String, Long> knowledgeMap = new HashMap<>();
            for (Terms.Bucket bucket : knowledgeClasses.getBuckets()) {
                knowledgeMap.put(bucket.getKeyAsString(), bucket.getDocCount());
            }
            knowledgeClassFilter.setV(knowledgeMap);
            result.getFilters().add(knowledgeClassFilter);
        }
    }
	public static void setHighlightElements(Text[] highlightFrags, ListIterator<String> itr) {
		for (Text txt : highlightFrags) {
			while (itr.hasNext()) {
				String applicant = itr.next().toString();
				if (txt.string().indexOf(applicant) > 0) {
					itr.set(txt.string());
				}
			}
		}
	}

	/**
	 * yyyyMMdd long to format string
	 * 
	 * @return
	 */
	public static String toDateString(Long date, String splitter) {
		Long year = date / 10000;
		Long dd = date - year * 10000;
		Long mm = dd / 100;
		dd = dd - mm * 100;
		return new StringBuilder().append(year).append(splitter).append(mm).append(splitter).append(dd).toString();
	}

	/**
	 * yyyyMMdd string to format string
	 * 
	 * @return
	 */
	public static String toDateString(String date, String splitter) {
		try {
			return toDateString(Long.valueOf(date), splitter);

		} catch (Exception e) {
			return "";
		}
	}
}
