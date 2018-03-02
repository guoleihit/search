package com.hiekn.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.*;
import com.hiekn.search.bean.result.*;
import com.hiekn.search.bean.result.paper.Conference;
import com.hiekn.search.bean.result.paper.Degree;
import com.hiekn.search.bean.result.paper.Journal;
import com.hiekn.search.bean.result.paper.PaperType;
import com.hiekn.search.exception.ServiceException;
import com.hiekn.util.CommonResource;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hiekn.service.Helper.*;
import static com.hiekn.util.CommonResource.PAPER_INDEX;

public class PaperService extends AbstractService{

    private Map<String, String> paperTypeNameMap;
    private Map<String, String> paperNameTypeMap;
	public PaperService (TransportClient client, IGeneralSSEService sse, String kgName) {
		esClient = client;
        generalSSEService = sse;
        this.kgName = kgName;

        paperTypeNameMap = new ConcurrentHashMap<>();
        paperTypeNameMap.put("CONFERENCE", "会议论文");
        paperTypeNameMap.put("JOURNAL", "期刊论文");
        paperTypeNameMap.put("DEGREE", "学位论文");

        paperNameTypeMap = new ConcurrentHashMap<>();
        paperNameTypeMap.put("会议论文", "CONFERENCE");
        paperNameTypeMap.put("期刊论文", "JOURNAL");
        paperNameTypeMap.put("学位论文", "DEGREE");
	}

	public ItemBean extractDetail(SearchHit hit) {
		Map<String, Object> source = hit.getSource();
        PaperDetail item = (PaperDetail)getPaper(source, new PaperDetail());

		item.setDocId(hit.getId());

		Object titleObj = source.get("title");
		if (titleObj != null) {
			item.setTitle(titleObj.toString());
		}
		Object absObj = source.get("abstract");
		if (absObj != null) {
			item.setAbs(absObj.toString().replaceAll(",","，"));
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

        item.setFirstAuthor(getString(source.get("first_author")));
        item.setFirstAuthorOrg(toStringList(source.get("first_author_org")));

		if (source.get("earliest_publication_date") != null) {
			item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
		}

		//item.setJournal(getString(source.get("journal")));
		item.setCiteCount(getString(source.get("citeCount")));

        if (source.get("refer")!=null) {
            List<Object> references = new ArrayList<>();
            if(source.get("refer") instanceof List){
                references.addAll((List)source.get("refer"));
            }
            item.setReferences(references);
        }

        if (source.get("cite")!=null) {
            List<Object> cites = new ArrayList<>();
            if(source.get("cite") instanceof List){
                cites.addAll((List)source.get("cite"));
            }
            item.setCites(cites);
        }
		return item;
	
	}

    private void extractAuthorData(Object inventorsObj, Set<String> inventors, Set<String> orgList) {
        if (inventorsObj != null && inventorsObj instanceof List) {
            for (Object inventor : (List) inventorsObj) {
                if (inventor != null && ((Map) inventor).get("name") != null) {
                    inventors.add(((Map) inventor).get("name").toString());
                }

                if (((Map) inventor).get("organization") != null){
                    if (((Map) inventor).get("organization") instanceof List) {
                        List orgs = (List) ((Map) inventor).get("organization");
                        for (Object org : orgs) {
                            if (((Map) org).get("name") != null) {
                                orgList.add(((Map) org).get("name").toString());
                            }
                        }
                    }else {
                        orgList.add(getString(((Map) inventor).get("organization")));
                    }
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
	public PaperItem extractItem(SearchHit hit) {
        Map<String, Object> source = hit.getSource();
        PaperItem item = getPaper(source, new PaperItem());

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

		item.setDoi(getString(source.get("doi")));

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

        //item.setJournal(getString(source.get("journal")));

		//highlight
		if (hit.getHighlightFields() != null) {
			for (Map.Entry<String, HighlightField> entry : hit.getHighlightFields().entrySet()) {
				Text[] frags = entry.getValue().getFragments();
				switch (entry.getKey()) {
					case "title":
                    case "title.english":
                    case "title.keyword":
                    case "title.smart":
						if (frags != null && frags.length > 0 && frags[0].string().length()>1) {
							item.setTitle(frags[0].string());
						}
						break;
					case "abstract":
                    case "abstract.english":
                    case "abstract.keyword":
                    case "abstract.smart":
						if (frags != null && frags.length > 0 && !StringUtils.isEmpty(item.getAbs())) {
							for (Text frag: frags) {
							    String fragStr = frag.string();
							    String noEmStr = fragStr.replaceAll("<em>", "");
                                noEmStr = noEmStr.replaceAll("</em>", "");
                                String abs = item.getAbs();
                                abs = abs.replace(noEmStr, fragStr);
                                item.setAbs(abs);
                            }
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
						//TODO
                    case "authors.organization.name":
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




		item.setAbs(item.getAbs().replaceAll(",","，"));

		return item;
	}

    private PaperItem getPaper(Map<String, Object> source, PaperItem paperItem) {
        PaperItem item = paperItem;
        if (source.get("paperType")!=null) {
            if(source.get("paperType").equals(PaperType.CONFERENCE.getName())){
                Conference c = new Conference();
                if(source.get("conference")!=null && source.get("conference") instanceof Map) {
                    Map<String, Object> confObj = (Map<String, Object>) source.get("conference");
                    c.setConference(getString(confObj.get("conference_name")));
                    c.setConferenceDate(getString(confObj.get("conference_time")));
                    c.setConferencePlace(getString(confObj.get("conference_place")));
                    c.setConferenceOrganizer(getString(confObj.get("conference_organizer")));
                }
                item = c;
            }else if(source.get("paperType").equals(PaperType.DEGREE.getName())){
                Degree d = new Degree();
                d.setDegree(getString(source.get("degree")));
                d.setDegreeYear(getString(source.get("degree_year")));
                d.setMajor(getString(source.get("profession")));
                d.setUniversity(getString(source.get("degree_awarded_organization")));
                d.setMentor(toStringListByKey(source.get("mentor"),"tutor_name"));
                item = d;
            }else if(source.get("paperType").equals(PaperType.JOURNAL.getName())){
                Journal j = new Journal();
                if(source.get("journal")!=null && source.get("journal") instanceof Map) {
                    Map<String, Object> journalObj = (Map<String, Object>) source.get("journal");
                    j.seteJournal(getString(journalObj.get("journal_english_name")));
                    j.setJournal(getString(journalObj.get("journal_chinese_name")));
                    j.setJournalYear(getString(journalObj.get("year")));
                    j.setPeriod(getString(journalObj.get("period")));
                }
                item = j;
            }
        }
        // TODO 多个来源和多个url，目前艾迪特数据没确定
        String url = getString(source.get("url"));
        item.setUrl(url);
        item.setOrigin(getString(source.get("origin")));
        if (item.getOrigin() != null) {
            Map<String, String> urls = new HashMap<>();
            urls.put(item.getOrigin(), url);
            item.setUrls(urls);
        }
        return item;
    }


    public QueryBuilder buildQuery(QueryRequestInternal request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);

        if (!StringUtils.isEmpty(request.getId())) {
            boolQuery.must(QueryBuilders.nestedQuery("annotation_tag", QueryBuilders.termQuery("annotation_tag.id", Long.valueOf(request.getId())).boost(8f), ScoreMode.Max));
            boolQuery.filter(QueryBuilders.termQuery("_type", "paper_data")).boost(3f);
            return adjustPaperTypeBoost(boolQuery);
        }

        if (!StringUtils.isEmpty(request.getCustomQuery())) {
            BoolQueryBuilder query = buildCustomQuery(request);
            query.filter(QueryBuilders.termQuery("_type", "paper_data")).boost(3f);
            boolQuery.should(query);
            if(StringUtils.isEmpty(request.getKw())){
                return adjustPaperTypeBoost(boolQuery);
            }
        }

        String userInputPersonName = request.getRecognizedPerson();
        String userInputOrgName = request.getRecognizedOrg();

        BoolQueryBuilder boolTitleQuery = null;
        //
        if (request.getUserSplitSegList()!=null && !request.getUserSplitSegList().isEmpty()) {
            boolTitleQuery = createSegmentsTermQuery(request, PAPER_INDEX, "title");
        }

        Set<String> enWords = Sets.newHashSet();
        Set<String> cnWords = Sets.newHashSet();
        for (String seg: request.getUserSplitSegList()) {
            if (!isChinese(seg)) {
                enWords.add(seg);
            }else{
                cnWords.add(seg);
            }
        }

        List<String> cnWordList = Lists.newArrayList();
        cnWordList.addAll(cnWords);
        List<String> enWordList = Lists.newArrayList();
        enWordList.addAll(enWords);

        QueryBuilder titleTerm = createTermsQuery("title", cnWordList, CommonResource.search_user_input_title_weight);
        QueryBuilder titleExactQuery = createMatchPhraseQuery("title.smart", cnWordList, CommonResource.search_user_input_title_weight);
        QueryBuilder abstractTerm = createTermsQuery("abstract", cnWordList, 1f);
        QueryBuilder journalTerm = createTermsQuery("journal.journal_chinese_name", cnWordList, CommonResource.search_user_input_title_weight);

        QueryBuilder entitleTerm = createTermsQuery("title.english", enWordList, CommonResource.search_user_input_title_weight);
        QueryBuilder enabstractTerm = createTermsQuery("abstract.english", enWordList, 1f);

        QueryBuilder authorTerm = createTermsQuery("authors.name.keyword", request.getUserSplitSegList(), CommonResource.search_person_weight);
        QueryBuilder orgsTerm = createTermsQuery("authors.organization.name", request.getUserSplitSegList(), CommonResource.search_org_weight);
        QueryBuilder kwsTerm = createTermsQuery("keywords.keyword", request.getUserSplitSegList(), 1f);
        QueryBuilder annotationTagTerm = createNestedQuery("annotation_tag","annotation_tag.name", request.getUserSplitSegList(), 1f);

        BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        if (request.getKwType() == null || request.getKwType() == 0) {
            should(termQuery, titleTerm);
            should(termQuery, journalTerm);
            should(termQuery, titleExactQuery);
            should(termQuery, boolTitleQuery);
            should(termQuery, entitleTerm);
            should(termQuery, enabstractTerm);
            should(termQuery, abstractTerm);
            should(termQuery, kwsTerm);
            should(termQuery, annotationTagTerm);
            if(userInputPersonName != null){
                should(termQuery, QueryBuilders.termQuery("authors.name.keyword", userInputPersonName).boost(CommonResource.search_recognized_person_weight));
                should(termQuery, QueryBuilders.termQuery("first_author", userInputPersonName).boost(CommonResource.search_person_weight));
            }else{
                should(termQuery,authorTerm);
            }

            if(userInputOrgName != null){
                should(termQuery, QueryBuilders.termQuery("authors.organization.name", userInputOrgName).boost(CommonResource.search_recognized_org_weight));
                should(termQuery, QueryBuilders.termQuery("first_author_org", userInputOrgName).boost(CommonResource.search_org_weight));
            }else{
                should(termQuery,orgsTerm);
            }

            if (userInputPersonName != null && userInputOrgName != null) {
                BoolQueryBuilder bool = QueryBuilders.boolQuery().boost(CommonResource.search_recognized_org_weight * CommonResource.search_recognized_person_weight);
                bool.should(QueryBuilders.termQuery("authors.organization.name", userInputOrgName));
                bool.should(QueryBuilders.termQuery("authors.name.keyword", userInputPersonName));
                should(termQuery,bool);
            }
        } else if(request.getKwType() == 1){
            if (userInputPersonName != null) {
                BoolQueryBuilder personQuery = QueryBuilders.boolQuery();
                personQuery.must(QueryBuilders.termQuery("authors.name.keyword", userInputPersonName).boost(CommonResource.search_recognized_person_weight));
                should(personQuery, QueryBuilders.termQuery("first_author", userInputPersonName)).boost(CommonResource.search_recognized_person_weight);
                if (userInputOrgName != null) {
                    personQuery.should(QueryBuilders.termQuery("authors.organization.name", userInputOrgName).boost(CommonResource.search_recognized_org_weight));
                    should(personQuery, QueryBuilders.termQuery("first_author_org", userInputOrgName).boost(CommonResource.search_recognized_org_weight));
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
                orgQuery.must(QueryBuilders.termQuery("authors.organization.name", userInputOrgName).boost(CommonResource.search_recognized_org_weight));
                should(orgQuery, QueryBuilders.termQuery("first_author_org", userInputOrgName).boost(CommonResource.search_recognized_org_weight));
                if (userInputPersonName != null) {
                    orgQuery.should(QueryBuilders.termQuery("authors.name.keyword", userInputPersonName).boost(CommonResource.search_recognized_person_weight));
                    should(orgQuery, QueryBuilders.termQuery("first_author", userInputPersonName).boost(CommonResource.search_recognized_person_weight));
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
            should(termQuery, entitleTerm);
            should(termQuery, enabstractTerm);
            should(termQuery, titleTerm);
            should(termQuery, boolTitleQuery);
            should(termQuery, abstractTerm);
            should(termQuery, kwsTerm);
            should(termQuery, annotationTagTerm);
        }


        boolQuery.must(termQuery);
        boolQuery.filter(QueryBuilders.termQuery("_type", "paper_data"));

        FunctionScoreQueryBuilder q = adjustPaperTypeBoost(boolQuery);
        return q;
    }

    private FunctionScoreQueryBuilder adjustPaperTypeBoost(BoolQueryBuilder boolQuery) {
        FunctionScoreQueryBuilder.FilterFunctionBuilder[] functions = new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("paperType","JOURNAL"), ScoreFunctionBuilders.weightFactorFunction(CommonResource.search_journal_paper_weight)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("journal.journal_chinese_name","中国电机工程学报"), ScoreFunctionBuilders.weightFactorFunction(0.09f)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("journal.journal_chinese_name","电力系统自动化"), ScoreFunctionBuilders.weightFactorFunction(0.08f)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("journal.journal_chinese_name","电网技术"), ScoreFunctionBuilders.weightFactorFunction(0.07f)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("journal.journal_chinese_name","电工技术学报"), ScoreFunctionBuilders.weightFactorFunction(0.06f)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("journal.journal_chinese_name","电力系统维护与控制"), ScoreFunctionBuilders.weightFactorFunction(0.05f)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("journal.journal_chinese_name","高电压技术"), ScoreFunctionBuilders.weightFactorFunction(0.04f)),

                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("paperType","DEGREE"), ScoreFunctionBuilders.weightFactorFunction(CommonResource.search_degree_paper_weight)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("degree_awarded_organization","清华大学"), ScoreFunctionBuilders.weightFactorFunction(0.02f)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("degree_awarded_organization","北京大学"), ScoreFunctionBuilders.weightFactorFunction(0.02f)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("degree_awarded_organization","哈尔滨工业大学"), ScoreFunctionBuilders.weightFactorFunction(0.02f)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("degree_awarded_organization","上海交通大学"), ScoreFunctionBuilders.weightFactorFunction(0.02f)),

                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.termQuery("paperType","CONFERENCE"), ScoreFunctionBuilders.weightFactorFunction(CommonResource.search_conference_paper_weight)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.wildcardQuery("conference.conference_name","*IEEE*"), ScoreFunctionBuilders.weightFactorFunction(0.03f)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.wildcardQuery("conference.conference_name","*ACM*"), ScoreFunctionBuilders.weightFactorFunction(0.03f)),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.wildcardQuery("conference.conference_name","*中国电机工程学会*"), ScoreFunctionBuilders.weightFactorFunction(0.09f))
        };
        return QueryBuilders.functionScoreQuery(boolQuery, functions).scoreMode(FiltersFunctionScoreQuery.ScoreMode.SUM);
    }

    @Override
	public void searchSimilarData(String docId, SearchResultBean result) throws Exception {
	        PaperDetail paperDetail = (PaperDetail) result.getRsData().get(0);
            String title = paperDetail.getTitle();
            QueryBuilder similarPapersQuery = QueryBuilders.matchQuery("title",title).analyzer("ik_max_word");
            SearchRequestBuilder spq = esClient.prepareSearch(PAPER_INDEX);
            HighlightBuilder titleHighlighter = new HighlightBuilder().field("title");


            spq.highlighter(titleHighlighter).setQuery(similarPapersQuery).setFrom(0).setSize(6);

            AggregationBuilder relatedPersons = AggregationBuilders.terms("related_persons").field("authors.name.keyword");
            spq.addAggregation(relatedPersons);

            AggregationBuilder relatedOrgs = AggregationBuilders.terms("related_orgs").field("authors.organization.name");
            spq.addAggregation(relatedOrgs);

            AggregationBuilder relatedKeywords = AggregationBuilders.terms("related_keywords").field("keywords");
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
                similarPapers.getV().sort(getItemBeanComparatorForPubDate());

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

                KVBean<String, List<Object>> references = new KVBean<>();
                references.setD("参考文献");
                references.setK("references");
                references.setV(paperDetail.getReferences());
                result.getSimilarData().add(references);

                KVBean<String, List<Object>> cites = new KVBean<>();
                cites.setD("引证文献");
                cites.setK("cites");
                cites.setV(paperDetail.getCites());
                result.getSimilarData().add(cites);
            }
	}

	public QueryBuilder buildEnhancedQuery(CompositeQueryRequest request) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);
        makePaperFilter(request, boolQuery);

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
					String value = reqItem.getKv().getV().get(0);
					if(!reqItem.getKv().getV().isEmpty() && !isChinese(value)){
					    reqItem.getKv().getV().addAll(Arrays.asList(value.toLowerCase().split(" ")));
                        buildQueryCondition(boolQuery, reqItem, "title.english", false, true);
                    }
				}else if ("abs".equals(key)) {
                    buildLongTextQueryCondition(boolQuery, reqItem, PAPER_INDEX,"abstract", false, false, null);
                    String value = reqItem.getKv().getV().get(0);
                    if(!reqItem.getKv().getV().isEmpty() && !isChinese(reqItem.getKv().getV().get(0))){
                        reqItem.getKv().getV().addAll(Arrays.asList(value.toLowerCase().split(" ")));
                        buildQueryCondition(boolQuery, reqItem, "abstract.english", false, true);
                    }
				} else if ("theme".equals(key)) {
					BoolQueryBuilder themeQuery = QueryBuilders.boolQuery();
                    buildQueryCondition(themeQuery, reqItem, "_kg_annotation_1.name", false,false, Operator.OR);
                    buildQueryCondition(themeQuery, reqItem, "_kg_annotation_2.name", false,false, Operator.OR);
                    buildQueryCondition(themeQuery, reqItem, "_kg_annotation_3.name", false,false, Operator.OR);
					//buildQueryCondition(themeQuery, reqItem, "annotation_tag.name", false,false, Operator.OR);
                    if (reqItem.getKv() != null && reqItem.getKv().getV() != null && reqItem.getKv().getV().size() > 0) {
                        QueryBuilder annotation = createNestedQuery("annotation_tag", "annotation_tag.name", reqItem.getKv().getV(), 1f);
                        themeQuery.should(annotation);
                    }
                    buildQueryCondition(themeQuery, reqItem, "keywords.keyword", false,false, Operator.OR);
					setOperator(boolQuery, reqItem, themeQuery);
				}else if ("keyword".equals(key)) {
					buildQueryCondition(boolQuery, reqItem, "keywords.keyword", false,false);
				}else if ("firstAuthor".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "first_author", false,false);
                }else if ("firstAuthorOrg".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "first_author_org", false,false);
                }else if ("author".equals(key)) {
					buildQueryCondition(boolQuery, reqItem, "authors.name.keyword", false,false);
				}else if ("authorOrg".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "authors.organization.name", false,false);
                }else if ("doi".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "doi", false,false);
                }else if ("references".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "refer.name.keyword", false,false);
                }else if ("pubDate".equals(dateKey)) {
					doBuildDateCondition(boolQuery, reqItem, "earliest_publication_date");
				}else if ("all".equals(key)) {
                    BoolQueryBuilder allQueryBuilder = makeFiledAllQueryBuilder(reqItem, Operator.OR);

                    setOperator(boolQuery, reqItem, allQueryBuilder);
                }else if (!StringUtils.isEmpty(key) || !StringUtils.isEmpty(dateKey)) {
                    // 搜索未知域，期望搜索本资源失败
                    if(Operator.AND.equals(reqItem.getOp()))
                        return null;
                }
			}
		}

        boolQuery.filter(QueryBuilders.termQuery("_type", "paper_data"));
		return adjustPaperTypeBoost(boolQuery);

	}

	@Override
    BoolQueryBuilder makeFiledAllQueryBuilder(CompositeRequestItem reqItem, Operator op) {
        BoolQueryBuilder allQueryBuilder = QueryBuilders.boolQuery();
        buildLongTextQueryCondition(allQueryBuilder, reqItem, PAPER_INDEX,"title", false,false, op);
        buildLongTextQueryCondition(allQueryBuilder, reqItem, PAPER_INDEX,"abstract", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "_kg_annotation_1.name", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "_kg_annotation_2.name", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "_kg_annotation_3.name", false,false, op);

        //buildQueryCondition(allQueryBuilder, reqItem, "annotation_tag.name", false,true, op);
        if (reqItem.getKv() != null && reqItem.getKv().getV() != null && reqItem.getKv().getV().size() > 0) {
            QueryBuilder annotation = createNestedQuery("annotation_tag", "annotation_tag.name", reqItem.getKv().getV(), 1f);
            setOperator(allQueryBuilder, op, annotation);
        }

        buildQueryCondition(allQueryBuilder, reqItem, "keywords.keyword", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "authors.name.keyword", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "authors.organization.name", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "refer.name.keyword", false,false, op);
        return allQueryBuilder;
    }

    public SearchResultBean doCompositeSearch(CompositeQueryRequest request) throws ExecutionException, InterruptedException {
		QueryBuilder boolQuery = buildEnhancedQuery(request);
        if (boolQuery==null) {
            throw new ServiceException(Code.SEARCH_UNKNOWN_FIELD_ERROR.getCode());
        }
		SearchRequestBuilder srb = esClient.prepareSearch(PAPER_INDEX);
        HighlightBuilder highlighter = new HighlightBuilder().field("title").field("abstract")
                .field("title.english").field("abstract.english")
                .field("title.smart").field("abstract.smart")
                .field("keywords.keyword").field("authors.name.keyword");

        srb.highlighter(highlighter).setQuery(boolQuery).setFrom((request.getPageNo() - 1) * request.getPageSize())
				.setSize(request.getPageSize());

        addSortByPubDate(request, srb);

        String annotationField = getAnnotationFieldName(request);
        if (annotationField != null) {
            AggregationBuilder knowledge = AggregationBuilders.terms("knowledge_class").field(annotationField);
            srb.addAggregation(knowledge);
        }

        // 发表时间
        AggregationBuilder aggPubYear = AggregationBuilders.histogram("publication_year")
                .field("earliest_publication_date").interval(10000).minDocCount(1).order(Histogram.Order.KEY_DESC);
        srb.addAggregation(aggPubYear);

        // 文献类型
        AggregationBuilder paperTypes = AggregationBuilders.terms("paper_type").field("paperType");
        srb.addAggregation(paperTypes);

        System.out.println(srb.toString());
		SearchResponse response =  srb.execute().get();
		SearchResultBean result = new SearchResultBean(request.getKw());
		result.setRsCount(response.getHits().totalHits);
		for (SearchHit hit : response.getHits()) {
			ItemBean item = extractItem(hit);
			result.getRsData().add(item);
		}

        String annotation = getAnnotationFieldName(request);
        setKnowledgeAggResult(request, response, result, annotation);

        Helper.setYearAggFilter(result,response,"publication_year", "发表年份","earliest_publication_date");
        //Helper.setTermAggFilter(result,response, "paper_type", "文献类型", "paperType");
        Terms paperTypesAggs = response.getAggregations().get("paper_type");
        KVBean<String, Map<String, ?>> paperTypeFilter = new KVBean<>();
        paperTypeFilter.setD("文献类型");
        paperTypeFilter.setK("paperType");
        Map<String, Long> paperMap = new HashMap<>();
        for (Terms.Bucket bucket : paperTypesAggs.getBuckets()) {
            String key = paperTypeNameMap.get(bucket.getKeyAsString());
            if(key == null){
                continue;
            }
            paperMap.put(key, bucket.getDocCount());
        }
        paperMap.put("_end", -1l);
        paperTypeFilter.setV(paperMap);
        result.getFilters().add(paperTypeFilter);
        return result;
	}

    private void makePaperFilter(QueryRequest request, BoolQueryBuilder boolQuery) {
        if (request.getFilters() != null) {
            List<KVBean<String, List<String>>> filters = request.getFilters();
            for (KVBean<String, List<String>> filter : filters) {
                if ("paperType".equals(filter.getK())) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        filterQuery.should(QueryBuilders.termQuery(filter.getK(), paperNameTypeMap.get(v)));
                    }
                    boolQuery.must(filterQuery);
                }
            }
        }
    }

    @Override
    public Map<String,String> formatCite(ItemBean bean, Integer format, List<String> customizedFields) throws Exception {

	    String type = "[C]";
        String authors = Helper.toStringFromList(bean.getAuthors(),",");
        String pubDate = bean.getPubDate();
        StringBuilder citeBuilder = new StringBuilder();
        citeBuilder.append(authors).append(".").append(bean.getTitle());
	    if (bean instanceof PaperItem) {
	        PaperItem paperItem = (PaperItem) bean;
	        if (PaperType.DEGREE.equals(paperItem.getPaperType()) && paperItem instanceof Degree) {
	            type = "[D]";
	            Degree d = (Degree)paperItem;

                citeBuilder.append(type).append(".").append(d.getUniversity())
                        .append(",").append(d.getDegreeYear()).append("[")
                        .append(pubDate).append("]");
            } else if (PaperType.JOURNAL.equals(paperItem.getPaperType()) && paperItem instanceof Journal) {
	            type = "[J]";
                Journal j = (Journal)paperItem;
                citeBuilder.append(type).append(".").append(j.getJournal())
                        .append(",").append(j.getJournalYear())
                        .append(",").append(j.getPeriod())
                        .append(".");
                if (!StringUtils.isEmpty(j.getDoi())) {
                    citeBuilder.append("DOI:").append(j.getDoi()).append(".");
                }
            } else if (PaperType.CONFERENCE.equals(paperItem.getPaperType()) && paperItem instanceof Conference) {
                Conference c = (Conference)paperItem;
                citeBuilder.append(":").append(c.getConference()).append(type)
                        .append(".").append(c.getConferenceDate()).append(".");
            }



            Map<String, String> results = new HashMap<>();
            results.put("cite",citeBuilder.toString());
            setCiteInfo(bean, format, customizedFields, authors, results);
            if (Integer.valueOf(3).equals(format)) {
                for(String field: customizedFields) {
                    if ("orgs".equals(field)) {
                        results.put("orgs", Helper.toStringFromList(paperItem.getOrgs(), ","));
                    } else if ("keywords".equals(field)) {
                        results.put("keywords", Helper.toStringFromList(paperItem.getKeywords(), ","));
                    } else if ("doi".equals(field)) {
                        results.put("doi", paperItem.getDoi());
                    }
                }
            }


            return results;
        }



	    return Maps.newHashMap();
    }


}
