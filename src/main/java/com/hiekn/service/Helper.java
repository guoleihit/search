package com.hiekn.service;

import com.alibaba.fastjson.JSONObject;
import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.SearchResultBean;
import com.hiekn.util.HttpClient;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Helper {

	static public volatile Map<String,Long> types = new HashMap<>();
	static public volatile Set<Long> knowledgeIds = new HashSet<>();
	static public volatile Map<String, Map<String, List<String>>> book = new HashMap<>();

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

    public static String toStringFromList(Iterable<String> keywords, String splitter) {
        StringBuilder builder = new StringBuilder();

        if (keywords!=null) {
            Iterator<String> itr = keywords.iterator();
            Boolean hasNext = itr.hasNext();
            while(hasNext){
                String kw = itr.next();
                builder.append(kw);
                hasNext = itr.hasNext();
                if (hasNext) {
                    builder.append(splitter);
                }
            }
        }

        return builder.toString();
    }

    /**
     * check if given word is contained in element of list.
     * @param list
     * @param word
     * @return
     */
    public static Boolean contains(List<String> list, String word) {
        for (String s: list) {
            if (s.contains(word)) {
                return Boolean.TRUE;
            }
        }

        return Boolean.FALSE;
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
		String annotationField = "_kg_annotation_1.name";
		int level = 1;
		if (request.getFilters() != null) {
			for (KVBean<String, List<String>> filter : request.getFilters()) {
				if ("_kg_annotation_1.name".equals(filter.getK())) {
					if(level<2){
						annotationField = "_kg_annotation_2.name";
						level = 2;
					}
				} else if ("_kg_annotation_2.name".equals(filter.getK())) {
					if(level<3) {
						annotationField = "_kg_annotation_3.name";
						level = 3;
					}
				} else if ("_kg_annotation_3.name".equals(filter.getK())) {
					return null;
				}
			}
		}
		return annotationField;
	}

    public static void setKnowledgeAggResult(QueryRequest request, SearchResponse response, SearchResultBean result, String annotation) {
        if (annotation != null) {
            Terms knowledgeClasses = response.getAggregations().get("knowledge_class");
            KVBean<String, Map<String, ? extends Object>> knowledgeClassFilter = new KVBean<>();
            knowledgeClassFilter.setD("知识体系");
            knowledgeClassFilter.setK(annotation);

            String value = null;
            String field1=null, field2=null;
            if (request.getFilters() != null) {
                for (KVBean<String, List<String>> filter : request.getFilters()) {
                    if (annotation.equals(filter.getK())) {
                        value = filter.getV().get(0);
                    }
                    if ("_kg_annotation_1.name".equals(filter.getK())) {
                        field1 = filter.getV().get(0);
                    } else if ("_kg_annotation_2.name".equals(filter.getK())) {
                        field2 = filter.getV().get(0);
                    }
                }
            }

            List<String> validNames = new ArrayList<>();
            if (field1 != null) {
                Map<String,List<String>> mids = Helper.book.get(field1);
                if (mids != null && field2 != null) {
                    validNames = mids.get(field2);
                }else if (field2 == null && mids != null) {
                    for (String key: mids.keySet()) {
                        validNames.add(key);
                    }
                }
            }else {
                for (String key: Helper.book.keySet()) {
                    validNames.add(key);
                }
            }
            Map<String, Long> knowledgeMap = new HashMap<>();
            for (Terms.Bucket bucket : knowledgeClasses.getBuckets()) {
                if (validNames.contains(bucket.getKeyAsString())) {
                    knowledgeMap.put(bucket.getKeyAsString(), bucket.getDocCount());
                }
            }
            if (annotation.indexOf("3") > 0) {
                knowledgeMap.put("_end", -1l);
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

    public static List<Map<String, Integer>> hightedwords(String highlighted) {
        //int targeted = 0;
        int countEm = 0;
        boolean emStart = false;
        boolean emed = false;
        boolean emEnd = false;
        char[] chars = highlighted.toCharArray();
        List<Character> word = new ArrayList<>();
        List<Map<String, Integer>> results = new ArrayList<>();
        int position = 0;
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            if (!emed && ((c == '<' && !emStart) || (c == 'e' && emStart) || (c == 'm' && emStart)
                    || (c == '>' && emStart))) {
                emStart = true;
                countEm++;
                if (c == '>') { // found an em started tag
                    emed = true;
                    emStart = false;
                    countEm = 0;
                    word.clear();
                }
            } else if (emStart && !emed && c != 'e' && c != 'm' && c != '>') {
                emStart = false;
            } else if (emed && ((c == '<' && !emEnd) || (c == '/' && emEnd) || (c == 'e' && emEnd)
                    || (c == 'm' && emEnd) || (c == '>' && emEnd))) {
                emEnd = true;
                countEm++;
                if (c == '>') { // found an em end tag
                    emed = false;
                    emEnd = false;
                    countEm = 0;

                    String w = characterListToString(word);
                    Map<String, Integer> map = new HashMap<>();
                    map.put(w, position++);
                    results.add(map);
                }
            } else if (emEnd && !emed && c != 'e' && c != 'm' && c != '>' && c != '/') {
                emEnd = false;
            } else if (emed) {
                if (countEm != 0) {
                    //targeted += countEm;
                    countEm = 0;
                }
                //targeted++;
                word.add(c);
                if (emEnd) {
                    emEnd = false;
                }
                if (emStart) {
                    emStart = false;
                }
            } else {
                position++;
            }
        }

        return results;
    }

    private static String characterListToString(List<Character> word) {
        StringBuilder sb = new StringBuilder(word.size());
        for (Character ch : word)
            sb.append(ch.charValue());
        String result = sb.toString();
        return result;
    }

	public static void setTermAggFilter(SearchResultBean result, SearchResponse response, String aggName, String filterD, String filterK) {
		Terms docTypes = response.getAggregations().get(aggName);
		KVBean<String, Map<String, ?>> docTypeFilter = new KVBean<>();
		docTypeFilter.setD(filterD);
		docTypeFilter.setK(filterK);
		Map<String, Long> docMap = new HashMap<>();
		for (Terms.Bucket bucket : docTypes.getBuckets()) {
			String key = bucket.getKeyAsString();
			if (key == null || key.length()==0) {
				key = "其他";
			}
            addToMap(docMap, bucket, key);
			//docMap.put(key, bucket.getDocCount());
		}
		docMap.put("_end",-1l);
		docTypeFilter.setV(docMap);
		result.getFilters().add(docTypeFilter);
	}

    /**
     * 按照规则排序
     * @param result
     * @param response
     * @param aggName
     * @param filterD
     * @param filterK
     * @param sortRule
     */
    public static void setTermAggFilter(SearchResultBean result, SearchResponse response, String aggName, String filterD, String filterK, List<String> sortRule) {
        Terms docTypes = response.getAggregations().get(aggName);
        KVBean<String, Map<String, ?>> docTypeFilter = new KVBean<>();
        docTypeFilter.setD(filterD);
        docTypeFilter.setK(filterK);
        Map<String, Long> docMap = new LinkedHashMap<>();

        for(String s : sortRule) {
            for (Terms.Bucket bucket : docTypes.getBuckets()) {
                String key = bucket.getKeyAsString();
                if (key == null || key.length()==0) {
                    key = "其他";
                }

                if (key.equals(s)) {
                    addToMap(docMap, bucket, key);
                }
            }
        }

        if (docMap.size() < docTypes.getBuckets().size()) {
            for (Terms.Bucket bucket : docTypes.getBuckets()) {
                String key = bucket.getKeyAsString();
                if (key == null || key.length()==0) {
                    key = "其他";
                }

                if (!docMap.containsKey(key)) {
                    docMap.put(key, bucket.getDocCount());
                } else {
                    addToMap(docMap, bucket, key);
                }
            }
        }

        docMap.put("_end",-1l);
        docTypeFilter.setV(docMap);
        result.getFilters().add(docTypeFilter);
    }

    public static void addToMap(Map<String, Long> docMap, Terms.Bucket bucket, String key) {
        Long count = bucket.getDocCount();
        if (docMap.get(key)!=null) {
            count += docMap.get(key);
        }
        docMap.put(key, count);
    }

    public static void setYearAggFilter(SearchResultBean result, SearchResponse response, String aggName, String filterD, String filterK) {
		Histogram yearAgg = response.getAggregations().get(aggName);
		KVBean<String, Map<String, ?>> yearFilter = new KVBean<>();
		yearFilter.setD(filterD);
		yearFilter.setK(filterK);
		Map<String, Long> yearMap = new TreeMap<>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return o2.compareTo(o1);
			}
		});
		for (Histogram.Bucket bucket : yearAgg.getBuckets()) {
			if (bucket.getKey() instanceof Number) {
				Double year = Double.valueOf(bucket.getKeyAsString());
				year = year / 10000;
				yearMap.put(String.valueOf(year.intValue()), bucket.getDocCount());
			}
		}
		yearMap.put("_end",-1l);
		yearFilter.setV(yearMap);
		result.getFilters().add(yearFilter);
	}
	public static List<AnalyzeResponse.AnalyzeToken> esSegment(String input, String index, TransportClient esClient){
		return esSegment(input, index, esClient, "ik_max_word");
	}

	public static List<AnalyzeResponse.AnalyzeToken> esSegment(String input, String index, TransportClient esClient, String analyzer){
		IndicesAdminClient indicesClient = esClient.admin().indices();
		AnalyzeRequestBuilder arb = indicesClient.prepareAnalyze(input);
		arb.setIndex(index);
		arb.setAnalyzer(analyzer);
		AnalyzeResponse response = arb.execute().actionGet();
		List<AnalyzeResponse.AnalyzeToken> segList = response.getTokens();
		return segList;
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


    /**
     * yyyy-MM-dd string to long
     *
     * @return
     */
    public static Long fromDateString(String dataStr) {
        return fromDateString(dataStr, "-");
    }

    public static Long fromDateString(String dateStr, String splitter) {
        if(dateStr.indexOf(splitter)<0){
            return Long.valueOf(dateStr) * 10000;
        }
        String year = dateStr.substring(0, dateStr.indexOf(splitter));
        dateStr = dateStr.substring(dateStr.indexOf(splitter) + 1);
        if (dateStr.indexOf(splitter) < 0) {
            return Long.valueOf(year) * 10000 + Long.valueOf(dateStr) * 100;
        }

        String month = dateStr.substring(0, dateStr.indexOf(splitter));
        String dateS = dateStr.substring(dateStr.indexOf(splitter) + 1);

        return Long.valueOf(year) * 10000 + Long.valueOf(month) * 100 + Long.valueOf(dateS);
    }

	public static boolean isChinese(String words) {
		Pattern chinesePattern = Pattern.compile("[\\u4E00-\\u9FA5]+");
		Matcher matcherResult = chinesePattern.matcher(words);
		return matcherResult.find();
	}

	public static boolean isNumber(String str) {
		try{
			Double.parseDouble(str);
		}catch(Exception e){
			return false;
		}
		return true;
	}

    public static Map<String, Long> sortMapByValue(Map<String, Long> oriMap) {
        if (oriMap == null || oriMap.isEmpty()) {
            return null;
        }
        Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
        List<Map.Entry<String, Long>> entryList = new ArrayList<Map.Entry<String, Long>>(
                oriMap.entrySet());
        Collections.sort(entryList, new MapValueComparator());

        Iterator<Map.Entry<String, Long>> iter = entryList.iterator();
        Map.Entry<String, Long> tmpEntry;
        while (iter.hasNext()) {
            tmpEntry = iter.next();
            sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
        }
        return sortedMap;
    }

    public static void addSortByPubDate(QueryRequest request, SearchRequestBuilder srb) {
        if (Integer.valueOf(1).equals(request.getSort())) {
            srb.addSort(SortBuilders.fieldSort("earliest_publication_date").order(SortOrder.DESC));
        }else if (Integer.valueOf(1).equals(request.getSort())) {
            srb.addSort(SortBuilders.fieldSort("earliest_publication_date").order(SortOrder.ASC));
        }
    }

    public static JSONObject getItemFromHbase(String rowKey, DocType docType){
        String table = getHBaseTableName(docType);
        String response = HttpClient.sendGet("http://10.10.20.8:20550/"+table+"/"+rowKey,null,null);
        System.out.println(response);
        try {
            SAXParser parser = SAXParserFactory.newInstance().newSAXParser();
            HBaseResultHandler handler = new HBaseResultHandler();
            parser.parse(new java.io.ByteArrayInputStream(response.getBytes()), handler);

            if(handler.columns.get("f_big:col")!=null){
                return com.alibaba.fastjson.JSON.parseObject(handler.columns.get("f_big:col"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static String getHBaseTableName(DocType docType){
        if(DocType.NEWS.equals(docType)){
            return "json_beijixing_data_news";
        }
        return "";
    }

    public static Comparator<Object> getMapComparator(final String key) {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy年mm月",Locale.CHINESE);
        return (o1, o2)->{
            if (o1 instanceof Map && o2 instanceof Map) {
                String str1 = (String)((Map) o1).get(key);
                String str2 = (String)((Map) o2).get(key);
                return doCompareString(sdf, str1, str2);
            }
            return 0;
        };
    }

    public static Comparator<Object> getItemBeanComparatorForPubDate() {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return (o1, o2)->{
            if (o1 instanceof ItemBean && o2 instanceof ItemBean) {
                String str1 = ((ItemBean)o1).getPubDate();
                String str2 = ((ItemBean)o2).getPubDate();

                return doCompareString(sdf, str1, str2);
            }
            return 0;
        };
    }

    private static int doCompareString(SimpleDateFormat sdf, String str1, String str2) {
        if(str1 !=null && str2 != null){
                try {
                    Date d1 = sdf.parse(str1);
                    Date d2 = sdf.parse(str2);
                    return d2.compareTo(d1);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            return str2.compareTo(str1);
        }else if(str1 == null && str2 == null){
            return 0;
        }else if (str1 == null) {
            return 1;
        }else {
            return -1;
        }
    }

    static class HBaseResultHandler extends DefaultHandler{
        Map<String, String> columns = new HashMap<>();
        String columnName;
        String columnValue;
        boolean isCellStart = false;
        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if ("Cell".equalsIgnoreCase(qName)) {
                isCellStart = true;

                for (int i = 0; i < attributes.getLength(); ++i) {
                    String attrName = attributes.getQName(i);
                    if ("column".equalsIgnoreCase(attrName)){
                        String attrValue = attributes.getValue(i);
                        columnName =  new String(Base64.getDecoder().decode(attrValue));
                    }
                }
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            if ("Cell".equalsIgnoreCase(qName)) {
                isCellStart = false;
                if(!StringUtils.isEmpty(columnName) && !StringUtils.isEmpty(columnValue)){
                    columns.put(columnName,columnValue);
                }
            }
        }


        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            if (isCellStart) {
                columnValue = new String(Base64.getDecoder().decode(new String(ch,start,length)));
            }
        }
    }

    static class MapValueComparator implements Comparator<Map.Entry<String, Long>> {
        @Override
        public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
            return o2.getValue().compareTo(o1.getValue());
        }
    }
}
