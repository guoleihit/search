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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.util.*;

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
				} else if ("annotation_2.name".equals(filter.getK())) {
					annotationField = "annotation_3.name";
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

	public static List<AnalyzeResponse.AnalyzeToken> esSegment(String input, String index, TransportClient esClient){
		IndicesAdminClient indicesClient = esClient.admin().indices();
		AnalyzeRequestBuilder arb = indicesClient.prepareAnalyze(input);
		arb.setIndex(index);
		arb.setAnalyzer("ik_max_word");
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
