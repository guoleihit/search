package com.hiekn.service;

import com.hiekn.search.bean.result.BaikeItem;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hiekn.service.Helper.getString;

public class BaikeService {
    @SuppressWarnings("unchecked")
    public BaikeItem extractItem(SearchHit hit) {
        Map<String, Object> source = hit.getSource();
        BaikeItem item = new BaikeItem();
        item.setTitle(getString(source.get("title")));
        item.seteTitle(getString(source.get("etitle")));
        item.setPyTitle(getString(source.get("pinyinTitle")));
        Object contentsObj = source.get("content");
        if (contentsObj instanceof List) {
            for (Object content : (List<Object>) contentsObj) {
                item.getContents().add(getString(content));
            }
        }

        Object picturesObj = source.get("pictures");
        if (picturesObj != null && picturesObj instanceof  List) {
            Map<String, String> image = new HashMap<>();
            for (Object picture: (List<Object>)picturesObj) {
                if(picture instanceof Map){
                    Map<String, Object> imageData = (Map<String, Object>) picture;
                    if(imageData.get("title")!=null)
                        image.put("title", imageData.get("title").toString());
                    if(imageData.get("picdesc")!=null)
                        image.put("picdesc", imageData.get("picdesc").toString());
                    if(imageData.get("inndernum")!=null)
                        image.put("inndernum", imageData.get("inndernum").toString());
                    if(imageData.get("displaynum")!=null)
                        image.put("displaynum", imageData.get("displaynum").toString());
                }
            }
            item.getPictures().add(image);
        }
        return item;
    }

    public BoolQueryBuilder buildQuery(String baike) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        TermQueryBuilder titleTerm = QueryBuilders.termQuery("title", baike);
        TermQueryBuilder etitleTerm = QueryBuilders.termQuery("etitle", baike);
        TermQueryBuilder pytitleTerm = QueryBuilders.termQuery("pinyintitle", baike);
        boolQuery.should(titleTerm);
        boolQuery.should(etitleTerm);
        boolQuery.should(pytitleTerm);
        return boolQuery;
    }
}
