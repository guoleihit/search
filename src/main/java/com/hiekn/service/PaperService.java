package com.hiekn.service;

import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.CompositeQueryRequest;
import com.hiekn.search.bean.request.CompositeRequestItem;
import com.hiekn.search.bean.request.Operator;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.*;
import com.hiekn.util.CommonResource;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
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
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.hiekn.service.Helper.*;
import static com.hiekn.util.CommonResource.PAPER_INDEX;

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
        List<String> inventors = new ArrayList<>();
        List<String> orgList = new ArrayList<>();
        extractAuthorData(inventorsObj, inventors, orgList);

        item.setOrgs(orgList);
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

    private void extractAuthorData(Object inventorsObj, List<String> inventors, List<String> orgList) {
        if (inventorsObj != null && inventorsObj instanceof List) {
            for (Object inventor : (List) inventorsObj) {
                if (inventor != null && ((Map) inventor).get("name") != null) {
                    inventors.add(((Map) inventor).get("name").toString());
                }

                if (((Map) inventor).get("orgs") != null){
                    List orgs = (List)((Map) inventor).get("orgs");
                    for (Object org: orgs) {
                        if(((Map)org).get("name")!=null){
                            orgList.add(((Map)org).get("name").toString());
                        }
                    }
                }
            }
        }
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
        List<String> orgList = new ArrayList<>();
        extractAuthorData(inventorsObj, inventors, orgList);

        item.setOrgs(orgList);
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
						if (frags != null && frags.length > 0 && frags[0].string().length()>1) {
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

		QueryBuilder titleTerm = QueryBuilders.termsQuery("title", request.getUserSplitSegList()).boost(4);

        BoolQueryBuilder boolTitleQuery = null;
		if (request.getKwType()!= 1 && request.getKwType() != 2) {
            List<AnalyzeResponse.AnalyzeToken> tokens = esSegment(request, PAPER_INDEX);
            boolTitleQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
            List<String> oneWordList = new ArrayList<>();
            for (AnalyzeResponse.AnalyzeToken token : tokens) {
                String t = token.getTerm();
                if (t.equals(request.getKw())) {
                    continue;
                }

                if (t.length() == 1) {
                    oneWordList.add(t);
                } else {
                    boolTitleQuery.should(QueryBuilders.termQuery("title", t));
                }
            }
            if (oneWordList.size() == tokens.size()) {
                //boolTitleQuery.should(QueryBuilders.termsQuery("title", oneWordList));
            }
        }

        QueryBuilder abstractTerm = QueryBuilders.termsQuery("abs", request.getUserSplitSegList());
        QueryBuilder authorTerm = QueryBuilders.termsQuery("persons.name.keyword", request.getUserSplitSegList()).boost(3f);
        QueryBuilder orgsTerm = QueryBuilders.termsQuery("persons.orgs.name.keyword", request.getUserSplitSegList()).boost(3f);
        QueryBuilder kwsTerm = QueryBuilders.termsQuery("keywords.keyword", request.getUserSplitSegList()).boost(3f);
        QueryBuilder annotationTagTerm = QueryBuilders.termsQuery("annotation_tag.name", request.getUserSplitSegList())
                .boost(3f);

        BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        if (request.getKwType() == null || request.getKwType() == 0) {
            termQuery.should(titleTerm);
            if(boolTitleQuery !=null) {
                termQuery.should(boolTitleQuery);
            }
            termQuery.should(abstractTerm);
            termQuery.should(authorTerm);
            termQuery.should(kwsTerm);
            termQuery.should(orgsTerm);
            termQuery.should(annotationTagTerm);
        } else if(request.getKwType() == 1){
            if (!StringUtils.isEmpty(request.getDescription())) {
                BoolQueryBuilder bool = QueryBuilders.boolQuery()
                        .must(authorTerm)
                        .must(QueryBuilders.termsQuery("persons.orgs.name.keyword", request.getDescription().split(",")));
                termQuery.should(bool);
            }else {
                termQuery.should(authorTerm);
            }
        } else if (request.getKwType() == 2) {
            termQuery.should(orgsTerm);
        } else if (request.getKwType() == 3) {
            termQuery.should(titleTerm);
            if(boolTitleQuery !=null) {
                termQuery.should(boolTitleQuery);
            }
            termQuery.should(abstractTerm);
            termQuery.should(kwsTerm);
            termQuery.should(annotationTagTerm);
        }


        boolQuery.must(termQuery);
        boolQuery.filter(QueryBuilders.termQuery("_type", "paper_data")).boost(3f);
        return boolQuery;
    }

	@Override
	public void searchSimilarData(String docId, SearchResultBean result) throws Exception {
            String title = result.getRsData().get(0).getTitle();
            QueryBuilder similarPapersQuery = QueryBuilders.matchQuery("title",title).analyzer("ik_max_word");
            SearchRequestBuilder spq = esClient.prepareSearch(PAPER_INDEX);
            HighlightBuilder titleHighlighter = new HighlightBuilder().field("title");


            spq.highlighter(titleHighlighter).setQuery(similarPapersQuery).setFrom(0).setSize(6);

            AggregationBuilder relatedPersons = AggregationBuilders.terms("related_persons").field("persons.name.keyword");
            spq.addAggregation(relatedPersons);

            AggregationBuilder relatedOrgs = AggregationBuilders.terms("related_orgs").field("persons.orgs.name.keyword");
            spq.addAggregation(relatedOrgs);

            Future<SearchResponse> similarPaperFuture = spq.execute();
            SearchResponse similarPaperResp;
            if((similarPaperResp = similarPaperFuture.get())!=null){

                KVBean<String, List<Object>> similarPapers = new KVBean<>();
                similarPapers.setD("相似论文");
                similarPapers.setK("similarPaper");
                similarPapers.setV(new ArrayList<>());
                result.getSimilarData().add(similarPapers);
                for (SearchHit hit : similarPaperResp.getHits()) {
                    ItemBean item = extractItem(hit);
                    if(docId.equals(item.getDocId())){
                        continue;
                    }
                    similarPapers.getV().add(item);
                }

                Terms relatedPersonAgg = similarPaperResp.getAggregations().get("related_persons");
                KVBean<String, List<Object>> relatedPersonFilter = new KVBean<>();
                result.getSimilarData().add(relatedPersonFilter);
                relatedPersonFilter.setD("相关人员");
                relatedPersonFilter.setK("related_persons");
                List<Object> personList = new ArrayList<>();
                for (Terms.Bucket bucket : relatedPersonAgg.getBuckets()) {
                    personList.add(bucket.getKeyAsString());
                }
                relatedPersonFilter.setV(personList);


                Terms relatedOrgsAgg = similarPaperResp.getAggregations().get("related_orgs");
                KVBean<String, List<Object>> relatedOrgsFilter = new KVBean<>();
                result.getSimilarData().add(relatedOrgsFilter);
                relatedOrgsFilter.setD("相关机构");
                relatedOrgsFilter.setK("related_orgs");
                List<Object> orgList = new ArrayList<>();
                for (Terms.Bucket bucket : relatedOrgsAgg.getBuckets()) {
                    orgList.add(bucket.getKeyAsString());
                }
                relatedOrgsFilter.setV(orgList);
            }
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
					BoolQueryBuilder themeQuery = QueryBuilders.boolQuery();
                    buildQueryCondition(themeQuery, reqItem, "annotation_1.name", false,false, Operator.OR);
                    buildQueryCondition(themeQuery, reqItem, "annotation_2.name", false,false, Operator.OR);
                    buildQueryCondition(themeQuery, reqItem, "annotation_3.name", false,false, Operator.OR);
					buildQueryCondition(themeQuery, reqItem, "annotation_tag.name", false,false, Operator.OR);
                    buildQueryCondition(themeQuery, reqItem, "keywords.keyword", false,false, Operator.OR);
					setOperator(boolQuery, reqItem, themeQuery);
				}else if ("keyword".equals(key)) {
					buildQueryCondition(boolQuery, reqItem, "keywords.keyword", false,false);
				}else if ("author".equals(key)) {
					buildQueryCondition(boolQuery, reqItem, "persons.name.keyword", false,false);
				}else if ("pubDate".equals(dateKey)) {
					doBuildDateCondition(boolQuery, reqItem, "earliest_publication_date");
				}else if ("all".equals(key)) {
                    BoolQueryBuilder allQueryBuilder = QueryBuilders.boolQuery();

					buildQueryCondition(allQueryBuilder, reqItem, "title", false,false, Operator.OR);
					buildQueryCondition(allQueryBuilder, reqItem, "abs", false,false, Operator.OR);
                    buildQueryCondition(allQueryBuilder, reqItem, "annotation_1.name", false,false, Operator.OR);
                    buildQueryCondition(allQueryBuilder, reqItem, "annotation_2.name", false,false, Operator.OR);
                    buildQueryCondition(allQueryBuilder, reqItem, "annotation_3.name", false,false, Operator.OR);
					buildQueryCondition(allQueryBuilder, reqItem, "annotation_tag.name", false,true, Operator.OR);
					buildQueryCondition(allQueryBuilder, reqItem, "keywords.keyword", false,false, Operator.OR);
					buildQueryCondition(allQueryBuilder, reqItem, "persons.name.keyword", false,false, Operator.OR);

                    setOperator(boolQuery, reqItem, allQueryBuilder);
                }
			}
		}

		return boolQuery;

	}

    public SearchResultBean doCompositeSearch(CompositeQueryRequest request) throws ExecutionException, InterruptedException {
		BoolQueryBuilder boolQuery = buildEnhancedQuery(request);
		SearchRequestBuilder srb = esClient.prepareSearch(PAPER_INDEX);
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
