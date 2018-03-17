package com.hiekn.service;

import com.hiekn.search.bean.DocType;
import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.CompositeQueryRequest;
import com.hiekn.search.bean.request.CompositeRequestItem;
import com.hiekn.search.bean.request.Operator;
import com.hiekn.search.bean.request.QueryRequestInternal;
import com.hiekn.search.bean.result.*;
import com.hiekn.search.exception.ServiceException;
import com.hiekn.util.CommonResource;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;

import java.util.List;
import java.util.Map;

import static com.hiekn.service.Helper.addSortByPubDate;
import static com.hiekn.service.Helper.getAnnotationFieldName;
import static com.hiekn.service.Helper.setKnowledgeAggResult;
import static com.hiekn.util.CommonResource.BOOK_INDEX;

public class BookService extends AbstractService {

    public BookService (TransportClient client) {
        esClient = client;
    }

    @Override
    public SearchResultBean doCompositeSearch(CompositeQueryRequest request) throws Exception {
        QueryBuilder boolQuery = buildEnhancedQuery(request);
        if (boolQuery==null) {
            throw new ServiceException(Code.SEARCH_UNKNOWN_FIELD_ERROR.getCode());
        }
        SearchRequestBuilder srb = esClient.prepareSearch(BOOK_INDEX);
        HighlightBuilder highlighter = new HighlightBuilder().field("title").field("abstract")
                .field("title.smart").field("abstract.smart").field("authors.name");

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

        return result;
    }

    @Override
    public QueryBuilder buildQuery(QueryRequestInternal request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);

        if (!StringUtils.isEmpty(request.getId())) {
            boolQuery.must(QueryBuilders.nestedQuery("annotation_tag", QueryBuilders.termQuery("annotation_tag.id", Long.valueOf(request.getId())).boost(8f), ScoreMode.Max));
            boolQuery.filter(QueryBuilders.termQuery("_type", "book_data")).boost(3f);
            return boolQuery;
        }

        if (!StringUtils.isEmpty(request.getCustomQuery())) {
            BoolQueryBuilder query = buildCustomQuery(request);
            query.filter(QueryBuilders.termQuery("_type", "book_data")).boost(3f);
            boolQuery.should(query);
            if(StringUtils.isEmpty(request.getKw())){
                return boolQuery;
            }
        }

        // max segments query
        BoolQueryBuilder titleQueryMax = null;
        if (request.getUserSplitSegList()!=null && !request.getUserSplitSegList().isEmpty()) {
            titleQueryMax = createSegmentsTermQuery(request, BOOK_INDEX, "title");
        }
        QueryBuilder titleTerm = createTermsQuery("title", request.getUserSplitSegList(), CommonResource.search_user_input_title_weight);
        QueryBuilder titleQuerySmart = createMatchPhraseQuery("title.smart", request.getUserSplitSegList(), CommonResource.search_user_input_title_weight);

        QueryBuilder abstractTerm = createTermsQuery("abstract", request.getUserSplitSegList(), 1f);
        QueryBuilder abstractTermSmart = createMatchPhraseQuery("abstract.smart", request.getUserSplitSegList(), 1f);

        QueryBuilder authorTerm = createTermsQuery("authors.name", request.getUserSplitSegList(), CommonResource.search_person_weight);
        //QueryBuilder orgsTerm = createTermsQuery("publisher", request.getUserSplitSegList(), CommonResource.search_org_weight);
        QueryBuilder kgTerm = createTermsQuery("_kg_knowledge_tag.name", request.getUserSplitSegList(), 1f);
        QueryBuilder annotationTagTerm = createNestedQuery("annotation_tag","annotation_tag.name", request.getUserSplitSegList(), 1f);

        String userInputPersonName = request.getRecognizedPerson();
        String userInputOrgName = request.getRecognizedOrg();

        BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);

            should(termQuery, titleTerm);
            should(termQuery, titleQueryMax);
            should(termQuery, titleQuerySmart);
            should(termQuery, kgTerm);
            should(termQuery, abstractTerm);
            should(termQuery, abstractTermSmart);
            should(termQuery, annotationTagTerm);
            if(userInputPersonName != null){
                should(termQuery, QueryBuilders.termQuery("authors.name", userInputPersonName).boost(CommonResource.search_recognized_person_weight));
            }else{
                should(termQuery,authorTerm);
            }

            if(userInputOrgName != null){
                should(termQuery, QueryBuilders.termQuery("publisher", userInputOrgName).boost(CommonResource.search_recognized_org_weight));
            }else{
                //should(termQuery,orgsTerm);
            }

//            if (userInputPersonName != null && userInputOrgName != null) {
//                BoolQueryBuilder bool = QueryBuilders.boolQuery().boost(CommonResource.search_recognized_org_weight * CommonResource.search_recognized_person_weight);
//                bool.should(QueryBuilders.termQuery("publisher", userInputOrgName));
//                bool.should(QueryBuilders.termQuery("authors.name", userInputPersonName));
//                should(termQuery,bool);
//            }


        boolQuery.must(termQuery);
        boolQuery.filter(QueryBuilders.termQuery("_type", "book_data"));

        return boolQuery;
    }

    @Override
    public QueryBuilder buildEnhancedQuery(CompositeQueryRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);

        if (request.getConditions()!=null && !request.getConditions().isEmpty()) {
            for (CompositeRequestItem reqItem: request.getConditions()) {
                String key = null;
                if(reqItem.getKv()!=null) {
                    key = reqItem.getKv().getK();
                }

                if ("title".equals(key)) {
                    buildLongTextQueryCondition(boolQuery, reqItem, BOOK_INDEX, "title", false, false, null);
                }else if ("abs".equals(key)) {
                    buildLongTextQueryCondition(boolQuery, reqItem, BOOK_INDEX,"abstract", false, false, null);
                }else if ("author".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "authors.name", false,false);
                }else if ("isbn".equals(key)) {
                    buildQueryCondition(boolQuery, reqItem, "isbn", false,false);
                }else if ("all".equals(key)) {
                    BoolQueryBuilder allQueryBuilder = makeFiledAllQueryBuilder(reqItem, Operator.OR);
                    setOperator(boolQuery, reqItem, allQueryBuilder);
                }else if (!StringUtils.isEmpty(key)) {
                    // 搜索未知域，期望搜索本资源失败
                    if(Operator.AND.equals(reqItem.getOp()))
                        return null;
                }
            }

        }

        return boolQuery;
    }

    @Override
    public void searchSimilarData(String docId, SearchResultBean result) throws Exception {
        BookDetail bookDetail = (BookDetail) result.getRsData().get(0);
        KVBean<String, List<Object>> references = new KVBean<>();
        references.setD("参考文献");
        references.setK("references");
        references.setV(bookDetail.getReferences());
        result.getSimilarData().add(references);

        KVBean<String, List<Object>> catalogues = new KVBean<>();
        catalogues.setD("目录");
        catalogues.setK("catalogues");
        catalogues.setV(bookDetail.getCatalog());
        result.getSimilarData().add(catalogues);
    }

    @Override
    BoolQueryBuilder makeFiledAllQueryBuilder(CompositeRequestItem reqItem, Operator op) {
        BoolQueryBuilder allQueryBuilder = QueryBuilders.boolQuery();
        buildLongTextQueryCondition(allQueryBuilder, reqItem, BOOK_INDEX,"title", false,false, op);
        buildLongTextQueryCondition(allQueryBuilder, reqItem, BOOK_INDEX,"abstract", false,false, op);

        if (reqItem.getKv() != null && reqItem.getKv().getV() != null && reqItem.getKv().getV().size() > 0) {
            QueryBuilder annotation = createNestedQuery("annotation_tag", "annotation_tag.name", reqItem.getKv().getV(), 1f);
            setOperator(allQueryBuilder, op, annotation);
        }
        buildQueryCondition(allQueryBuilder, reqItem, "_kg_knowledge_tag.name", false,false, op);
        buildQueryCondition(allQueryBuilder, reqItem, "authors.name", false,false, op);
        //buildQueryCondition(allQueryBuilder, reqItem, "authors.organization.name", false,false, op);
        //buildQueryCondition(allQueryBuilder, reqItem, "refer.name.keyword", false,false, op);
        return allQueryBuilder;
    }

    @Override
    public ItemBean extractItem(SearchHit hit) {
        Map<String, Object> source = hit.getSource();
        BookItem item = new BookItem();

        item.setDocId(hit.getId().toString());
        item.setIsbn(Helper.getString(source.get("isbn")));
        item.setTitle(Helper.getString(source.get("title")));
        item.setAuthors(Helper.toStringListByKey(source.get("authors"), "name"));
        item.setPublisher(Helper.getString(source.get("publisher")));
        item.setPubDate(Helper.toDateString(Helper.getString(source.get("earliest_publication_date")), "-"));
        item.setAbs(Helper.getString(source.get("abstract")));
        item.setDocType(DocType.BOOK);
        return item;
    }

    @Override
    public ItemBean extractDetail(SearchHit hit) {
        BookDetail detail = new BookDetail();
        BookItem item = (BookItem)extractItem(hit);
        BeanUtils.copyProperties(item,detail);

        Map<String, Object> source = hit.getSource();
        detail.setCipno(Helper.getString(source.get("cipno")));
        detail.setFormat(Helper.getString(source.get("format")));
        detail.setPaperBookPrice(Helper.getString(source.get("paper_book_price")));
        detail.setPrintings(Helper.getString(source.get("printings")));
        detail.setPrintNum(Helper.getString(source.get("print_num")));

        if (source.get("bibliography")!=null) {
            List<Object> references = new ArrayList<>();
            if(source.get("bibliography") instanceof List){
                references.addAll((List)source.get("bibliography"));
            }
            detail.setReferences(references);
        }

        if (source.get("catalogue")!=null) {
            List<Object> catalogues = new ArrayList<>();
            if(source.get("catalogue") instanceof List){
                catalogues.addAll((List)source.get("catalogue"));
            }
            detail.setCatalog(catalogues);
        }

        return detail;
    }
}
