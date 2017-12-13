package com.hiekn.service;

import com.alibaba.fastjson.JSONObject;
import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.SearchResultBean;
import com.hiekn.util.HttpClient;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.util.*;

public class Helper {

	static public volatile Map<String,Long> types = new HashMap<>();
	static public volatile Set<Long> knowledgeIds = new HashSet<>();

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
		String annotationField = "_kg_annotation_1.name";
		if (request.getFilters() != null) {
			for (KVBean<String, List<String>> filter : request.getFilters()) {
				if ("_kg_annotation_1.name".equals(filter.getK())) {
					annotationField = "_kg_annotation_2.name";
				} else if ("_kg_annotation_2.name".equals(filter.getK())) {
					annotationField = "_kg_annotation_3.name";
				} else if ("_kg_annotation_3.name".equals(filter.getK())) {
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

	public static void setTermAggFilter(SearchResultBean result, SearchResponse response, String aggName, String filterD, String filterK) {
		Terms docTypes = response.getAggregations().get(aggName);
		KVBean<String, Map<String, ?>> docTypeFilter = new KVBean<>();
		docTypeFilter.setD(filterD);
		docTypeFilter.setK(filterK);
		Map<String, Long> docMap = new HashMap<>();
		for (Terms.Bucket bucket : docTypes.getBuckets()) {
			docMap.put(bucket.getKeyAsString(), bucket.getDocCount());
		}
		docMap.put("_end",-1l);
		docTypeFilter.setV(docMap);
		result.getFilters().add(docTypeFilter);
	}

	public static void setYearAggFilter(SearchResultBean result, SearchResponse response, String aggName, String filterD, String filterK) {
		Histogram yearAgg = response.getAggregations().get(aggName);
		KVBean<String, Map<String, ?>> yearFilter = new KVBean<>();
		yearFilter.setD(filterD);
		yearFilter.setK(filterK);
		Map<String, Long> yearMap = new HashMap<>();
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

}
