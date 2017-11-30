package com.hiekn.service;

import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.CompositeQueryRequest;
import com.hiekn.search.bean.request.CompositeRequestItem;
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
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.hiekn.service.Helper.*;

public class PaperService extends AbstractService{

	public PaperService (TransportClient client) {
		esClient = client;
	}

	public ItemBean extractDetail(SearchHit hit) {
		PaperDetail item = new PaperDetail();
		Map<String, Object> source = hit.getSource();
		item.setDocId(hit.getId());

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

        item.setJournal(getString(source.get("journal")));

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

	@Override
	public void searchSimilarData(String docId, SearchResultBean result) throws Exception {

	}

	public BoolQueryBuilder buildEnhancedQuery(CompositeQueryRequest request) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);

		if (request.getConditions()!=null && !request.getConditions().isEmpty()) {
			for (CompositeRequestItem reqItem: request.getConditions()) {
				String key = null;
				if(reqItem.getKv()!=null) {
					key = reqItem.getKv().getK();
				}

				String dateKey = null;
				if(reqItem.getKvDate()!=null) {
					dateKey = reqItem.getKvDate().getK();
				}

				if ("title".equals(key)) {
					buildQueryCondition(boolQuery, reqItem, "title", false, false);
				}else if ("abs".equals(key)) {
					buildQueryCondition(boolQuery, reqItem, "abs", false, false);
				} else if ("theme".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "annotation_1.name", false,false);
                    buildQueryCondition(boolQuery, reqItem, "annotation_2.name", false,false);
                    buildQueryCondition(boolQuery, reqItem, "annotation_3.name", false,false);
					buildQueryCondition(boolQuery, reqItem, "annotation_tag.name", false,false);
				}else if ("keyword".equals(key)) {
					buildQueryCondition(boolQuery, reqItem, "keywords.keyword", false,false);
				}else if ("author".equals(key)) {
					buildQueryCondition(boolQuery, reqItem, "persons.name.keyword", false,false);
				}else if ("pubDate".equals(dateKey)) {
					doBuildDateCondition(boolQuery, reqItem, "earliest_publication_date");
				}else if ("all".equals(key)) {
					buildQueryCondition(boolQuery, reqItem, "title", false,false);
					buildQueryCondition(boolQuery, reqItem, "abs", false,false);
                    buildQueryCondition(boolQuery, reqItem, "annotation_1.name", false,false);
                    buildQueryCondition(boolQuery, reqItem, "annotation_2.name", false,false);
                    buildQueryCondition(boolQuery, reqItem, "annotation_3.name", false,false);
					buildQueryCondition(boolQuery, reqItem, "annotation_tag.name", false,true);
					buildQueryCondition(boolQuery, reqItem, "keywords.keyword", false,false);
					buildQueryCondition(boolQuery, reqItem, "persons.name.keyword", false,false);
				}
			}
		}

		return boolQuery;

	}

	public SearchResultBean doCompositeSearch(CompositeQueryRequest request) throws ExecutionException, InterruptedException {
		BoolQueryBuilder boolQuery = buildEnhancedQuery(request);
		SearchRequestBuilder srb = esClient.prepareSearch(CommonResource.PAPER_INDEX);
        HighlightBuilder highlighter = new HighlightBuilder().field("title").field("abs")
                .field("keywords.keyword").field("persons.name.keyword");

        srb.highlighter(highlighter).setQuery(boolQuery).setFrom(request.getPageNo() - 1)
				.setSize(request.getPageSize());

        if (Integer.valueOf(1).equals(request.getSort())) {
            srb.addSort(SortBuilders.fieldSort("earliest_publication_date").order(SortOrder.DESC));
        }

        String annotationField = getAnnotationFieldName(request);
        if (annotationField != null) {
            AggregationBuilder knowledge = AggregationBuilders.terms("knowledge_class").field(annotationField);
            srb.addAggregation(knowledge);
        }
        System.out.println(srb.toString());
		SearchResponse response =  srb.execute().get();
		SearchResultBean result = new SearchResultBean(request.getKw());
		result.setRsCount(response.getHits().totalHits);
		for (SearchHit hit : response.getHits()) {
			ItemBean item = extractItem(hit);
			result.getRsData().add(item);
		}

        String annotation = getAnnotationFieldName(request);
        setKnowledgeAggResult(response, result, annotation);
        return result;
	}
}
