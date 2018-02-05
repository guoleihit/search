package com.hiekn.search.rest;

import com.alibaba.fastjson.JSONObject;
import com.google.common.primitives.Floats;
import com.hiekn.search.bean.result.PaperItem;
import com.hiekn.search.bean.result.RestResp;
import com.hiekn.service.Helper;
import com.hiekn.service.PaperService;
import com.hiekn.util.CommonResource;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Controller;

import javax.annotation.Resource;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.*;


@Controller
@Path("/c")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
@Api(tags = {"查重"})
public class DuplicateCheckRestApi implements InitializingBean{

    @Resource
    private TransportClient esClient;

    private PaperService paperService = null;


    @ApiOperation(value = "计算文档重复率")
    @POST
    @Path("/dup/stats")
    public RestResp<List<Map<String, Object>>> dupStats(@FormParam("abs")  String abs, @FormParam("article")  String article,
                           @QueryParam("token") String token, @QueryParam("tt") Long tt) {

        QueryBuilder query = QueryBuilders.moreLikeThisQuery(new String[]{"abstract.smart"}, new String[]{abs}, null)
                .minDocFreq(1).minTermFreq(1).maxQueryTerms(100000);
        SearchRequestBuilder srb = esClient.prepareSearch(CommonResource.PAPER_INDEX).setQuery(query).setSize(20);
        HighlightBuilder highlighter = new HighlightBuilder().field("abstract.smart");
        srb.highlighter(highlighter);

        List<Map<String, Object>> result = new LinkedList<>();

        try {
            SearchResponse response = srb.execute().get();
            int count = 20;
            for (SearchHit hit : response.getHits()) {
                Map<String,Object> source = hit.getSource();
                String abstractStr = Helper.getString(source.get("abstract"));

                PaperItem item = paperService.extractItem(hit);
                List<Map<String,Integer>>  words = Helper.hightedwords(item.getAbs());

                // merge adjacent words
                int slop = 2;
                StringBuilder lastWord = new StringBuilder();
                int lastPosition = 0;
                ListIterator<Map<String, Integer>> itr = words.listIterator();
                Set<String> wordSet = new HashSet<>();
                while(itr.hasNext()) {
                    Map<String, Integer> map = itr.next();
                    String k = null;
                    Integer p = null;
                    for(Map.Entry<String,Integer> entry: map.entrySet()){
                        k = entry.getKey();
                        p = entry.getValue();
                    }

                    if(k==null || p==null) {
                        continue;
                    }
                    if (lastPosition + slop >= p) {
                        lastWord.append(k);
                    }else {
                        wordSet.add(lastWord.toString());
                        lastWord = new StringBuilder();
                        lastWord.append(k);
                    }
                    lastPosition = p;
                }


                // skip isolated keyword because it may be common topic
                if (item.getKeywords() != null) {
                    for(String keyword: item.getKeywords()) {
                        if (wordSet.contains(keyword)) {
                            wordSet.remove(keyword);
                        }
                    }
                }

                // skip too short word
                Iterator<String> wordSetItr = wordSet.iterator();
                while(wordSetItr.hasNext()) {
                    String word = wordSetItr.next();
                    if(word.length() < 3) {
                        wordSetItr.remove();
                    }
                }

                // calculate word length
                int currentPaperDupWordLength = 0;
                int currentMaxWordLength = 0;
                for(String word: wordSet) {
                    if(word.length() > currentMaxWordLength){
                        currentMaxWordLength = word.length();
                    }
                    currentPaperDupWordLength += word.length();
                }
                float similarity = (float)currentPaperDupWordLength/abstractStr.length();
                System.out.println("abstract similar:"+ similarity);
                JSONObject r = new JSONObject();
                r.put("doc", item);
                r.put("similarity", similarity);
                result.add(r);
                count --;
                if (count == 0) {
                    break;
                }
            }

            result.sort(new Comparator<Map<String, Object>>() {
                @Override
                public int compare(Map<String, Object> o1, Map<String, Object> o2) {
                    return Floats.compare((Float)o2.get("similarity"), (Float)o1.get("similarity") );
                }
            });
        }catch (Exception e) {
            e.printStackTrace();
        }
        return new RestResp<List<Map<String, Object>>>(result,tt);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        paperService = new PaperService(esClient, null, null);
    }
}
