package com.hiekn.service;

import com.hiekn.plantdata.bean.graph.EntityBean;
import com.hiekn.plantdata.service.IGeneralSSEService;
import com.hiekn.search.bean.KVBean;
import com.hiekn.search.bean.request.*;
import com.hiekn.search.bean.result.Code;
import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.PaperItem;
import com.hiekn.search.bean.result.SearchResultBean;
import com.hiekn.search.exception.BaseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.hiekn.service.Helper.isChinese;


public abstract class AbstractService {

    TransportClient esClient;
    String kgName;
    IGeneralSSEService generalSSEService;

    private static Logger log = LoggerFactory.getLogger(AbstractService.class);

    void makeFilters(QueryRequest request, BoolQueryBuilder boolQuery) {
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
                } else if ("_type".equals(filter.getK()) || filter.getK().startsWith("_kg_annotation_")) {
                    BoolQueryBuilder filterQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
                    for (String v : filter.getV()) {
                        filterQuery.should(QueryBuilders.termQuery(filter.getK(), v));
                    }
                    boolQuery.must(filterQuery);
                }
            }
        }
    }

    public abstract SearchResultBean doCompositeSearch(CompositeQueryRequest request) throws Exception;

    public abstract QueryBuilder buildQuery(QueryRequestInternal request);

    public abstract QueryBuilder buildEnhancedQuery(CompositeQueryRequest request);

    public abstract void searchSimilarData(String docId, SearchResultBean result) throws Exception;

    /**
     * 生成引用词条
     * @param bean
     * @return
     * @throws Exception
     */
    public  Map<String,String>  formatCite(ItemBean bean, Integer format, List<String> customizedFields) throws Exception { return null;}

    public ItemBean extractItem(SearchHit hit) {return new ItemBean();}

    public ItemBean extractDetail(SearchHit hit) {return extractItem(hit);}

    protected void setCiteInfo(ItemBean bean, Integer format, List<String> customizedFields, String authors, Map<String, String> results) {
        // 查新
        if (Integer.valueOf(2).equals(format)) {
            results.put("abs", bean.getAbs());
        }else if (Integer.valueOf(3).equals(format)) {
            if (customizedFields != null) {
                for (String field : customizedFields) {
                    if ("title".equals(field)) {
                        results.put("title", bean.getTitle());
                    } else if ("abs".equals(field)) {
                        results.put("abs", bean.getAbs());
                    } else if ("authors".equals(field)) {
                        results.put("authors", authors);
                    }
                }
            }
        }
    }
    List<AnalyzeResponse.AnalyzeToken> esSegment(QueryRequestInternal request, String index){
        //利用es分词
        if(request.getSegmentList() == null) {
            List<AnalyzeResponse.AnalyzeToken> segList = Helper.esSegment(request.getKw(),index,esClient);
            request.setSegmentList(segList);
        }
        return request.getSegmentList();
    }

    /**
     *
     * @param boolQuery
     * @param reqItem
     * @param esField
     * @param needPrefix
     * @param ignoreStrCase
     */
    void buildQueryCondition(BoolQueryBuilder boolQuery, CompositeRequestItem reqItem, String esField, boolean needPrefix, boolean ignoreStrCase) {
        List<String> values = reqItem.getKv().getV();
        buildQueryCondition(boolQuery, reqItem, esField, needPrefix, ignoreStrCase, Arrays.asList(values.toArray()));
    }

    void buildQueryCondition(BoolQueryBuilder boolQuery, CompositeRequestItem reqItem, String esField, boolean needPrefix, boolean ignoreStrCase, Operator overrideOperator) {
        List<String> values = reqItem.getKv().getV();
        buildQueryCondition(boolQuery, reqItem, esField, needPrefix, ignoreStrCase, Arrays.asList(values.toArray()),overrideOperator);
    }

    void buildQueryCondition(BoolQueryBuilder boolQuery, CompositeRequestItem reqItem, String esField, boolean needPrefix, boolean ignoreStrCase, List<Object> values, Operator overrideOperator) {
        BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        for (Object value : values) {
            if (ignoreStrCase) {
                String str = value.toString().toLowerCase();
                doBuildQueryCondition(reqItem, esField, needPrefix, termQuery, str);
                str = value.toString().toUpperCase();
                doBuildQueryCondition(reqItem, esField, needPrefix, termQuery, str);
            } else {
                doBuildQueryCondition(reqItem, esField, needPrefix, termQuery, value);
            }
        }
        Operator op = reqItem.getOp();
        if (overrideOperator != null) {
            op = overrideOperator;
        }
        if (Operator.OR.equals(op)) {
            boolQuery.should(termQuery);
        } else if (Operator.AND.equals(reqItem.getOp())){
            boolQuery.must(termQuery);
        } else if (Operator.NOT.equals(reqItem.getOp())) {
            boolQuery.mustNot(termQuery);
        }
    }

    /**
     * Need provide values
     * @param boolQuery
     * @param reqItem
     * @param esField
     * @param needPrefix
     * @param ignoreStrCase
     * @param values
     */
    void buildQueryCondition(BoolQueryBuilder boolQuery, CompositeRequestItem reqItem, String esField, boolean needPrefix, boolean ignoreStrCase, List<Object> values) {
        buildQueryCondition(boolQuery, reqItem, esField, needPrefix, ignoreStrCase,values, null);
    }

    void setOperator(BoolQueryBuilder boolQuery, CompositeRequestItem reqItem, QueryBuilder allQueryBuilder) {
        setOperator(boolQuery, reqItem.getOp(), allQueryBuilder);
    }

    void setOperator(BoolQueryBuilder boolQuery, Operator op,  QueryBuilder allQueryBuilder) {
        if (Operator.AND.equals(op)) {
            boolQuery.must(allQueryBuilder);
        }else if (Operator.OR.equals(op)) {
            boolQuery.should(allQueryBuilder);
        }else {
            boolQuery.mustNot(allQueryBuilder);
        }
    }

    void doBuildQueryCondition(CompositeRequestItem reqItem, String esField, boolean needPrefix, BoolQueryBuilder termQuery, Object value) {
        if (needPrefix) {
            termQuery.should(QueryBuilders.prefixQuery(esField, value.toString()));
        }else if (Integer.valueOf(1).equals(reqItem.getPrecision()) || value instanceof Integer) { // 精确匹配
            termQuery.should(QueryBuilders.termQuery(esField, value));
            //termQuery.should(QueryBuilders.matchPhraseQuery(esField, value).slop(50).analyzer("ik_smart"));
            if(esField.indexOf(".keyword") < 0 && value instanceof String){
                esField = esField.concat(".keyword");
            }
            if (value instanceof String) {
                termQuery.should(QueryBuilders.termQuery(esField, value));
            }
        } else if (Integer.valueOf(2).equals(reqItem.getPrecision())){ // 模糊匹配
            //termQuery.should(QueryBuilders.wildcardQuery(esField, "*" + value + "*"));
            termQuery.should(QueryBuilders.termQuery(esField, value));
            QueryRequest rq = new QueryRequest();
            rq.setKw(reqItem.getKv().getV().get(0));
            QueryBuilder absQuery = createSegmentsTermQuery(new QueryRequestInternal(rq), "gw_paper", esField); // TODO index name for test
            setOperator((BoolQueryBuilder) termQuery, (CompositeRequestItem) reqItem, (QueryBuilder) absQuery);
        }
    }

    protected void doBuildDateCondition(BoolQueryBuilder boolQuery, CompositeRequestItem reqItem, String esField) {
        Map<String,String> dates = reqItem.getKvDate().getV();
        BoolQueryBuilder appDateQuery = QueryBuilders.boolQuery();
        for (Map.Entry<String,String> entry: dates.entrySet()) {
            if ("start".equalsIgnoreCase(entry.getKey())) {
                appDateQuery.must(QueryBuilders.rangeQuery(esField).gte(Helper.fromDateString(entry.getValue())));
            }else if ("end".equalsIgnoreCase(entry.getKey())) {
                appDateQuery.must(QueryBuilders.rangeQuery(esField).lte(Helper.fromDateString(entry.getValue())));
            }
        }

        if(Operator.AND.equals(reqItem.getOp())){
            boolQuery.must(appDateQuery);
        }else {
            boolQuery.should(appDateQuery);
        }
    }

    protected void buildLongTextQueryCondition(BoolQueryBuilder boolQuery, CompositeRequestItem reqItem, String index, String field, boolean needPrefix, boolean ignoreStrCase, Operator op) {
        if (reqItem.getPrecision().equals(2)) {
            QueryRequest rq = new QueryRequest();
            rq.setKw(reqItem.getKv().getV().get(0));
            QueryBuilder absQuery = createSegmentsTermQuery(new QueryRequestInternal(rq), index, field);
            if (op != null) {
                setOperator(boolQuery, op, absQuery);
            } else {
                setOperator(boolQuery, reqItem, absQuery);
            }
        } else {
            String str = reqItem.getKv().getV().get(0);
            if(!field.endsWith(".smart")){
                field = field.concat(".smart");
            }
            QueryBuilder builder = QueryBuilders.matchPhraseQuery(field, str).analyzer("ik_smart");
            if (op != null) {
                setOperator(boolQuery, op, builder);
            } else {
                setOperator(boolQuery, reqItem, builder);
            }
            //buildQueryCondition(boolQuery, reqItem, field, needPrefix, ignoreStrCase, op);
        }
    }

    protected BoolQueryBuilder createSegmentsTermQuery(QueryRequestInternal request, String index, String field) {
        BoolQueryBuilder boolTitleQuery;
        List<AnalyzeResponse.AnalyzeToken> tokens = esSegment(request, index);

        boolTitleQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        List<String> oneWordList = new ArrayList<>();
        for (AnalyzeResponse.AnalyzeToken token : tokens) {
            String t = token.getTerm();

            if (t.length() == 1) {
                oneWordList.add(t);
            } else {
                boolTitleQuery.should(QueryBuilders.termQuery(field, t));
            }
        }

        if (oneWordList.size() == tokens.size() && !tokens.isEmpty()) {
            boolTitleQuery.should(QueryBuilders.termsQuery(field, oneWordList)).minimumShouldMatch(oneWordList.size());
        } else if(tokens==null || tokens.isEmpty()) {
            boolTitleQuery = null;
        }

        return boolTitleQuery;
    }

    protected QueryBuilder createTermsQuery(String key, List<String> values, float boost){
        if(values != null && !values.isEmpty()){
            return QueryBuilders.termsQuery(key, values).boost(boost);
        }
        return null;
    }

    protected QueryBuilder createMatchPhraseQuery(String key, List<String> values, float boost){
        if(values != null && !values.isEmpty()){
            String v = String.join(" ", values);
            return QueryBuilders.matchPhraseQuery(key, v).boost(boost);
        }
        return null;
    }

    protected QueryBuilder createNestedQuery(String path, String key, List<String> values, float boost) {
        if(values != null && !values.isEmpty()){
            return QueryBuilders.nestedQuery(path,QueryBuilders.termsQuery(key, values).boost(boost), ScoreMode.Max);
        }
        return null;
    }

    protected BoolQueryBuilder should(BoolQueryBuilder parent, QueryBuilder son){
        if(son!=null){
            parent.should(son);
        }
        return parent;
    }

    abstract BoolQueryBuilder makeFiledAllQueryBuilder(CompositeRequestItem reqItem, Operator op);

    protected BoolQueryBuilder buildCustomQuery(QueryRequest request) {
        try {
            Integer precision = request.getPrecision();
            if(!Integer.valueOf(1).equals(precision) &&  !Integer.valueOf(2).equals(precision)){
                precision = 1;
            }
            List<String> parsed = new ArrayList<>();
            int i = 0;
            char[] contents = request.getCustomQuery().toLowerCase().toCharArray();
            boolean wordStart = false;
            boolean notOp = false;
            int s = 0, e = 0;
            while (i < contents.length) {
                char c = contents[i];
                if (c == '(') {
                    if(notOp){
                        parsed.add("not");
                        notOp = false;
                    }
                    parsed.add("(");
                } else if (c == 'a') {
                    if ((i + 3) < contents.length && contents[i + 1] == 'n' && contents[i + 2] == 'd'
                            && (contents[i + 3] == '(' || contents[i + 3] == ' ')) {
                        parsed.add("and");
                        i += 2;
                    }
                } else if (c == 'o') {
                    if ((i + 2) < contents.length && contents[i + 1] == 'r'
                            && (contents[i + 2] == '(' || contents[i + 2] == ' ')) {
                        parsed.add("or");
                        i += 1;
                    }
                } else if (c == 'n') {
                    if ((i + 3) < contents.length && contents[i + 1] == 'o' && contents[i + 2] == 't'
                            && (contents[i + 3] == '(' || contents[i + 3] == ' ')) {
                        //parsed.add("not");
                        if (notOp){
                            parsed.add("not");
                        }
                        notOp = true;
                        i += 2;
                    }
                } else if (isChinese(String.valueOf(c)) || (c >= 'a' && c <= 'z')) {
                    if (!wordStart) {
                        s = i;
                        wordStart = true;
                    }
                } else if (c == ' ' || c == ')') {
                    if (wordStart) {
                        e = i - 1;
                        wordStart = false;
                        String word = new String(contents, s, e - s + 1);
                        if (notOp) {
                            notOp = false;
                            word = "not_" + word;
                        }
                        parsed.add(word);
                    }

                    if (c == ')') {
                        parsed.add(")");
                    }
                }
                i++;
            }
            if (wordStart) {
                e = i - 1;
                String word = new String(contents, s, e - s + 1);
                if (notOp) {
                    word = "not_" + word;
                }
                parsed.add(word);
            }
            // System.out.println(parsed);

            Stack<String> stack = new Stack<>();
            List<String> parsedSuffix = new ArrayList<>();
            for (i = 0; i < parsed.size(); i++) {
                String element = parsed.get(i);
                if ("(".equals(element)) {
                    stack.push(element);
                } else if (")".equals(element)) {
                    while (!"(".equals(stack.peek())) {
                        parsedSuffix.add(stack.pop());
                    }
                    stack.pop();
                } else if ("or".equals(element)) {
                    while (!stack.isEmpty() && !"(".equals(stack.peek())) {
                        parsedSuffix.add(stack.pop());
                    }
                    stack.push(element);
                } else if ("and".equals(element)) {
                    while (!stack.isEmpty() && ("and".equals(stack.peek()))) {
                        parsedSuffix.add(stack.pop());
                    }
                    stack.push(element);
                } else if ("not".equals(element)) {
//                    while (!stack.isEmpty() && !"(".equals(stack.peek())) {
//                        parsedSuffix.add(stack.pop());
//                    }
                    stack.push(element);
                } else {
                    parsedSuffix.add(element);
                }
            }
            while (!stack.isEmpty()) {
                parsedSuffix.add(stack.pop());
            }

            System.out.println(parsedSuffix);

            Stack<BoolQueryBuilder> queryStack = new Stack<>();
            for (i = 0; i < parsedSuffix.size(); i++) {
                String element = parsedSuffix.get(i);
                if ("or".equals(element)) {

                    BoolQueryBuilder q1 = queryStack.pop();
                    BoolQueryBuilder q2 = queryStack.pop();
                    queryStack.push(QueryBuilders.boolQuery().should(q1).should(q2).minimumShouldMatch(1));
                } else if ("and".equals(element)) {
                    BoolQueryBuilder q1 = queryStack.pop();
                    BoolQueryBuilder q2 = queryStack.pop();
                    queryStack.push(QueryBuilders.boolQuery().must(q1).must(q2));
                } else if ("not".equals(element)){
                    BoolQueryBuilder q1 = queryStack.pop();
                    queryStack.push(QueryBuilders.boolQuery().mustNot(q1));
                }else {
                    String value = element;
                    boolean isNotOp = false;
                    if (value.startsWith("not_")) {
                        value = value.substring(4);
                        isNotOp = true;
                    }
                    CompositeRequestItem item = new CompositeRequestItem();
                    KVBean<String, List<String>> bean = new KVBean();
                    bean.setK("all");
                    bean.setV(new ArrayList<>());
                    bean.getV().add(value);

                    item.setPrecision(precision);
                    Operator op = Operator.OR;
                    if (isNotOp) {
                        op = Operator.NOT;
                        item.setPrecision(Integer.valueOf(1)); // 精确
                    }
                    item.setOp(op);
                    item.setKv(bean);
                    queryStack.push(makeFiledAllQueryBuilder(item, op));
                }
            }

            if (queryStack.size() > 1) {
                throw new RuntimeException("表达式解析错误." + request.getCustomQuery());
            }

            BoolQueryBuilder query = queryStack.peek();
            return query;
        }catch (Exception e){
            System.out.println(e.getMessage());
            throw new BaseException(Code.EXPRESS_ERROR.getCode());
        }
    }
}
