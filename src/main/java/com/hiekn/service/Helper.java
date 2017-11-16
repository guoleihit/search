package com.hiekn.service;

import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.QueryRequest;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

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


    public static void makeFilters(QueryRequest request, BoolQueryBuilder boolQuery) {
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
}
