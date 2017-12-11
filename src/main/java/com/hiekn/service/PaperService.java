package com.hiekn.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.CompositeQueryRequest;
import com.hiekn.search.bean.request.CompositeRequestItem;
import com.hiekn.search.bean.request.Operator;
import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.PaperDetail;
import com.hiekn.search.bean.result.PaperItem;
import com.hiekn.search.bean.result.SearchResultBean;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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

import static com.hiekn.service.Helper.*;
import static com.hiekn.util.CommonResource.PAPER_INDEX;

public class PaperService extends AbstractService{

	public PaperService (TransportClient client, IGeneralSSEService sse, String kgName) {
		esClient = client;
        generalSSEService = sse;
        this.kgName = kgName;
	}

	public ItemBean extractDetail(SearchHit hit) {
		PaperDetail item = new PaperDetail();
		Map<String, Object> source = hit.getSource();
		item.setDocId(hit.getId());

		Object titleObj = source.get("title");
		if (titleObj != null) {
			item.setTitle(titleObj.toString());
		}
		Object absObj = source.get("abstract");
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

		item.setGraphId(getString(source.get("kg_id")));

        Object inventorsObj = source.get("authors");
        Set<String> inventors = new HashSet<>();
        Set<String> orgList = new HashSet<>();
        extractAuthorData(inventorsObj, inventors, orgList);

        item.setOrgs(orgList);
        if (!inventors.isEmpty()) {
            item.setAuthors(Arrays.asList(inventors.toArray(new String[]{})));
        }

		if (source.get("earliest_publication_date") != null) {
			item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
		}

		item.setJournal(getString(source.get("journal")));
		item.setCiteCount(getString(source.get("citeCount")));
		return item;
	
	}

    private void extractAuthorData(Object inventorsObj, Set<String> inventors, Set<String> orgList) {
        if (inventorsObj != null && inventorsObj instanceof List) {
            for (Object inventor : (List) inventorsObj) {
                if (inventor != null && ((Map) inventor).get("name") != null) {
                    inventors.add(((Map) inventor).get("name").toString());
                }

                if (((Map) inventor).get("organization") != null){
                    List orgs = (List)((Map) inventor).get("organization");
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
		Object absObj = source.get("abstract");
		if (absObj != null) {
			item.setAbs(absObj.toString());
		}
		Object keywords = source.get("keywords");
		List<String> kws = toStringList(keywords);
		if (!kws.isEmpty()) {
			item.setKeywords(kws);
		}

		Object inventorsObj = source.get("authors");
		Set<String> inventors = new HashSet<>();
        Set<String> orgList = new HashSet<>();
        extractAuthorData(inventorsObj, inventors, orgList);

        item.setOrgs(orgList);
		if (!inventors.isEmpty()) {
			item.setAuthors(Arrays.asList(inventors.toArray(new String[]{})));
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
					case "abstract":
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
					case "authors.name.keyword":
						if (frags != null && frags.length > 0) {
							ListIterator<String> itr = item.getAuthors().listIterator();
							setHighlightElements(frags, itr);
						}
						break;
                    case "authors.organization.name.keyword":
                        if (frags != null && frags.length > 0) {
                            List<String> values = Lists.newArrayList(item.getOrgs());
                            ListIterator<String> itr = values.listIterator();
                            setHighlightElements(frags, itr);
                            item.setOrgs(Sets.newHashSet(values));
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

        if (!StringUtils.isEmpty(request.getId())) {
            boolQuery.must(QueryBuilders.termQuery("annotation_tag.id", Long.valueOf(request.getId())).boost(8f));
            boolQuery.filter(QueryBuilders.termQuery("_type", "paper_data")).boost(3f);
            return boolQuery;
        }

        Map<String, String> result = intentionRecognition(request);
        String userInputPersonName = result.get("人物");
        String userInputOrgName = result.get("机构");

        BoolQueryBuilder boolTitleQuery = null;
        // 已经识别出人和机构，或者用户输入的不是人也不是机构
        if (request.getKwType() != 1 && request.getKwType() != 2 || userInputOrgName != null || userInputPersonName!=null) {
            boolTitleQuery = createSegmentsTermQuery(request, PAPER_INDEX, "title");
        }

        QueryBuilder titleTerm = createTermsQuery("title", request.getUserSplitSegList(), 1f);
        QueryBuilder abstractTerm = createTermsQuery("abstract", request.getUserSplitSegList(), 1f);
        QueryBuilder authorTerm = createTermsQuery("authors.name.keyword", request.getUserSplitSegList(), 3f);
        QueryBuilder orgsTerm = createTermsQuery("authors.organization.name.keyword", request.getUserSplitSegList(), 3f);
        QueryBuilder kwsTerm = createTermsQuery("keywords.keyword", request.getUserSplitSegList(), 3f);
        QueryBuilder annotationTagTerm = createTermsQuery("annotation_tag.name", request.getUserSplitSegList(), 3f);

        BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        if (request.getKwType() == null || request.getKwType() == 0) {
            should(termQuery, titleTerm);
            should(termQuery,boolTitleQuery);
            should(termQuery,abstractTerm);
            should(termQuery,kwsTerm);
            should(termQuery,annotationTagTerm);
            if(userInputOrgName != null){
                should(termQuery, QueryBuilders.termQuery("authors.name.keyword", userInputPersonName).boost(5f));
            }else{
                should(termQuery,authorTerm);
            }

            if(userInputPersonName != null){
                should(termQuery, QueryBuilders.termQuery("authors.organization.name.keyword", userInputOrgName).boost(5f));
            }else{
                should(termQuery,orgsTerm);
            }
        } else if(request.getKwType() == 1){
            if (userInputPersonName != null) {
                BoolQueryBuilder personQuery = QueryBuilders.boolQuery();
                personQuery.must(QueryBuilders.termQuery("authors.name.keyword", userInputPersonName).boost(6f));
                if (userInputOrgName != null) {
                    personQuery.should(QueryBuilders.termQuery("authors.organization.name.keyword", userInputOrgName));
                }
                should(personQuery, annotationTagTerm);
                should(personQuery, titleTerm);
                should(personQuery, boolTitleQuery);
                should(personQuery, abstractTerm);
                should(personQuery, kwsTerm);
                should(termQuery, personQuery);
            }else {
                should(termQuery, authorTerm);
            }
        } else if (request.getKwType() == 2) {
            if (userInputOrgName != null) {
                BoolQueryBuilder orgQuery = QueryBuilders.boolQuery();
                orgQuery.must(QueryBuilders.termQuery("authors.organization.name.keyword", userInputOrgName).boost(6f));
                if (userInputPersonName != null) {
                    orgQuery.should(QueryBuilders.termQuery("authors.name.keyword", userInputPersonName));
                }
                should(orgQuery, annotationTagTerm);
                should(orgQuery, titleTerm);
                should(orgQuery, boolTitleQuery);
                should(orgQuery, abstractTerm);
                should(orgQuery, kwsTerm);
                should(termQuery, orgQuery);
            }else {
                should(termQuery, orgsTerm);
            }
        } else if (request.getKwType() == 3) {
            should(termQuery, titleTerm);
            should(termQuery, boolTitleQuery);
            should(termQuery, abstractTerm);
            should(termQuery, kwsTerm);
            should(termQuery, annotationTagTerm);
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

            AggregationBuilder relatedPersons = AggregationBuilders.terms("related_persons").field("authors.name.keyword");
            spq.addAggregation(relatedPersons);

            AggregationBuilder relatedOrgs = AggregationBuilders.terms("related_orgs").field("authors.organization.name.keyword");
            spq.addAggregation(relatedOrgs);

            AggregationBuilder relatedKeywords = AggregationBuilders.terms("related_keywords").field("keywords.keyword");
            spq.addAggregation(relatedKeywords);

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


                Terms relatedKeywordsAgg = similarPaperResp.getAggregations().get("related_keywords");
                KVBean<String, List<Object>> relatedKeywordsFilter = new KVBean<>();
                result.getSimilarData().add(relatedKeywordsFilter);
                relatedKeywordsFilter.setD("相关关键词");
                relatedKeywordsFilter.setK("related_keywords");
                List<Object> keywordList = new ArrayList<>();
                for (Terms.Bucket bucket : relatedKeywordsAgg.getBuckets()) {
                    keywordList.add(bucket.getKeyAsString());
                }
                relatedKeywordsFilter.setV(keywordList);
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
					buildLongTextQueryCondition(boolQuery, reqItem, PAPER_INDEX, "title", false, false, null);
				}else if ("abs".equals(key)) {
                    buildLongTextQueryCondition(boolQuery, reqItem, PAPER_INDEX,"abstract", false, false, null);
				} else if ("theme".equals(key)) {
					BoolQueryBuilder themeQuery = QueryBuilders.boolQuery();
                    buildQueryCondition(themeQuery, reqItem, "_kg_annotation_1.name", false,false, Operator.OR);
                    buildQueryCondition(themeQuery, reqItem, "_kg_annotation_2.name", false,false, Operator.OR);
                    buildQueryCondition(themeQuery, reqItem, "_kg_annotation_3.name", false,false, Operator.OR);
					buildQueryCondition(themeQuery, reqItem, "annotation_tag.name", false,false, Operator.OR);
                    buildQueryCondition(themeQuery, reqItem, "keywords.keyword", false,false, Operator.OR);
					setOperator(boolQuery, reqItem, themeQuery);
				}else if ("keyword".equals(key)) {
					buildQueryCondition(boolQuery, reqItem, "keywords.keyword", false,false);
				}else if ("author".equals(key)) {
					buildQueryCondition(boolQuery, reqItem, "authors.name.keyword", false,false);
				}else if ("pubDate".equals(dateKey)) {
					doBuildDateCondition(boolQuery, reqItem, "earliest_publication_date");
				}else if ("all".equals(key)) {
                    BoolQueryBuilder allQueryBuilder = QueryBuilders.boolQuery();

                    buildLongTextQueryCondition(allQueryBuilder, reqItem, PAPER_INDEX,"title", false,false, Operator.OR);
                    buildLongTextQueryCondition(allQueryBuilder, reqItem, PAPER_INDEX,"abstract", false,false, Operator.OR);
                    buildQueryCondition(allQueryBuilder, reqItem, "_kg_annotation_1.name", false,false, Operator.OR);
                    buildQueryCondition(allQueryBuilder, reqItem, "_kg_annotation_2.name", false,false, Operator.OR);
                    buildQueryCondition(allQueryBuilder, reqItem, "_kg_annotation_3.name", false,false, Operator.OR);
					buildQueryCondition(allQueryBuilder, reqItem, "annotation_tag.name", false,true, Operator.OR);
					buildQueryCondition(allQueryBuilder, reqItem, "keywords.keyword", false,false, Operator.OR);
					buildQueryCondition(allQueryBuilder, reqItem, "authors.name.keyword", false,false, Operator.OR);

                    setOperator(boolQuery, reqItem, allQueryBuilder);
                }
			}
		}

		return boolQuery;

	}

    public SearchResultBean doCompositeSearch(CompositeQueryRequest request) throws ExecutionException, InterruptedException {
		BoolQueryBuilder boolQuery = buildEnhancedQuery(request);
		SearchRequestBuilder srb = esClient.prepareSearch(PAPER_INDEX);
        HighlightBuilder highlighter = new HighlightBuilder().field("title").field("abstract")
                .field("keywords.keyword").field("authors.name.keyword");

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
