package com.hiekn.service;

import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.StandardItem;
import com.hiekn.search.bean.result.StandardDetail;
import org.elasticsearch.search.SearchHit;

import java.util.List;
import java.util.Map;

import static com.hiekn.service.Helper.getString;
import static com.hiekn.service.Helper.toDateString;
import static com.hiekn.service.Helper.toStringList;

public class StandardService {

    public static ItemBean extractStandardItem(SearchHit hit) {
        StandardItem item = new StandardItem();
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

        Object inventorsObj = source.get("persons");
        List<String> inventors = toStringList(inventorsObj);
        if (!inventors.isEmpty()) {
            item.setAuthors(inventors);
        }

        if (source.get("earliest_publication_date") != null) {
            item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
        }


        item.setYield(getString(source.get("yield")));

        Object quotes = source.get("quote");
        List<String> quoteList = toStringList(quotes);
        if (!quoteList.isEmpty()) {
            item.setQuote(quoteList);
        }

        Object terms = source.get("term");
        List<String> termList = toStringList(terms);
        if (!termList.isEmpty()) {
            item.setTerm(termList);
        }

        item.setPubDep(getString(source.get("pub_dep")));
        item.setTelephone(getString(source.get("telephone")));
        item.setAddress(getString(source.get("address")));
        item.setISBN(getString(source.get("ISBN")));
        item.setFormat(getString(source.get("format")));
        item.setBY(getString(source.get("BY")));
        item.setPrintNum(getString(source.get("print_num")));
        item.setWordNum(getString(source.get("word_num")));
        item.setPage(getString(source.get("page")));
        item.setPdfPage(getString(source.get("Pdf_page")));
        item.setPrice(getString(source.get("price")));
        return item;
    }


    public static ItemBean extractStandardDetail(SearchHit hit) {
        StandardDetail item = new StandardDetail();
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

        Object inventorsObj = source.get("persons");
        List<String> inventors = toStringList(inventorsObj);
        if (!inventors.isEmpty()) {
            item.setAuthors(inventors);
        }

        if (source.get("earliest_publication_date") != null) {
            item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
        }

        item.setType(getString(source.get("type")));
        item.setLanguage(getString(source.get("language")));
        item.setCcs(getString(source.get("ccs")));
        item.setIcs(getString(source.get("ics")));
        item.setNum(getString(source.get("num")));
        item.seteName(getString(source.get("e_name")));
        item.setCarryonDate(getString(source.get("carryon_date")));
        item.setManageDep(getString(source.get("manage_dep")));
        item.setState(getString(source.get("state")));
        item.setAuthorDep(getString(source.get("author_dep")));
        item.setSubNum(getString(source.get("sub_num")));
        item.setInterNum(getString(source.get("inter_num")));
        item.setInterName(getString(source.get("inter_name")));
        item.setConsistent(getString(source.get("consistent")));

        return item;
    }
}
