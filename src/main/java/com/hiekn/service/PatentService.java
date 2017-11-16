package com.hiekn.service;

import com.hiekn.search.bean.request.QueryRequest;
import com.hiekn.search.bean.result.ItemBean;
import com.hiekn.search.bean.result.PatentDetail;
import com.hiekn.search.bean.result.PatentItem;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.util.*;

import static com.hiekn.service.Helper.*;

public class PatentService {

    @SuppressWarnings("rawtypes")
    public ItemBean extractPatentDetail(SearchHit hit) {
        PatentDetail item = new PatentDetail();

        Map<String, Object> source = hit.getSource();
        item.setDocId(hit.getId().toString());

        Object titleObj = source.get("title");
        if (titleObj != null && titleObj instanceof Map) {
            item.setTitle(((Map) titleObj).get("original") != null ? ((Map) titleObj).get("original").toString() : "");
        }
        Object absObj = source.get("abstract");
        if (absObj != null && absObj instanceof Map) {
            item.setAbs(((Map) absObj).get("original") != null ? ((Map) absObj).get("original").toString() : "");
        }
        Object agenciesObj = source.get("agencies");
        List<String> agencies = toStringList(agenciesObj);
        if (!agencies.isEmpty()) {
            item.setAgencies(agencies);
        }

        Object agents = source.get("agents");
        List<String> agentList = toStringList(agents);
        if (!agentList.isEmpty()) {
            item.setAgents(agentList);
        }

        if (source.get("application_number") != null) {
            item.setApplicationNumber(source.get("application_number").toString());
        }
        if (source.get("publication_number") != null) {
            item.setPublicationNumber(source.get("publication_number").toString());
        }
        if (source.get("application_date") != null) {
            item.setApplicationDate(toDateString(source.get("application_date").toString(), "-"));
        }
        if (source.get("earliest_publication_date") != null) {
            item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
        }
        Object applicantsObj = source.get("applicants");

        List<Map<String, Object>> applicants = new ArrayList<>();
        if (applicantsObj != null && applicantsObj instanceof List) {
            for (Object applicant : (List) applicantsObj) {
                Map<String, Object> app = new HashMap<>();
                if (applicant == null) {
                    continue;
                }
                if (((Map) applicant).get("name") != null) {
                    Object nameObj = ((Map) applicant).get("name");
                    if (nameObj instanceof Map) {
                        app.put("name", ((Map) nameObj).get("original"));
                    } else {
                        app.put("name", nameObj);
                    }
                }
                if (((Map) applicant).get("address") != null) {
                    Object addressObj = ((Map) applicant).get("address");
                    if (addressObj instanceof Map) {
                        app.put("address", ((Map) addressObj).get("original"));
                    } else {
                        app.put("address", addressObj);
                    }
                }
                if (((Map) applicant).get("type") != null) {
                    app.put("type", ((Map) applicant).get("type"));
                }
                applicants.add(app);
            }
        }

        if (!applicants.isEmpty()) {
            item.setApplicants(applicants);
        }

        Object inventorsObj = source.get("inventors");
        List<String> inventors = getStringListFromNameOrgObject(inventorsObj);
        if (!inventors.isEmpty()) {
            item.setAuthors(inventors);
        }

        Object mainIpcObj = source.get("main_ipc");
        if (mainIpcObj != null && mainIpcObj instanceof Map) {
            item.setMainIPC(String.valueOf(((Map) mainIpcObj).get("ipc")));
        }

        Object ipcsObj = source.get("ipcs");
        List<String> ipcs = toStringListByKey(ipcsObj, "ipc");
        if (!ipcs.isEmpty()) {
            item.setIpces(ipcs);
        }
        try {
            if (source.get("fulltext_pages") != null) {
                item.setPages(Integer.valueOf(source.get("fulltext_pages").toString()));
            }
        } catch (Exception e) {
            item.setPages(0);
        }
        return item;
    }


    @SuppressWarnings("rawtypes")
    public ItemBean extractPatentItem(SearchHit hit) {
        PatentItem item = new PatentItem();
        Map<String, Object> source = hit.getSource();
        // use application_number.lowercase as doc id for detail search
        item.setDocId(hit.getId().toString());

        Object titleObj = source.get("title");
        if (titleObj != null && titleObj instanceof Map) {
            item.setTitle(((Map) titleObj).get("original") != null ? ((Map) titleObj).get("original").toString() : "");
        }
        Object absObj = source.get("abstract");
        if (absObj != null && absObj instanceof Map) {
            item.setAbs(((Map) absObj).get("original") != null ? ((Map) absObj).get("original").toString() : "");
        }
        Object agenciesObj = source.get("agencies");
        List<String> agencies = toStringList(agenciesObj);
        if (!agencies.isEmpty()) {
            item.setAgencies(agencies);
        }

        Object applicantsObj = source.get("applicants");
        List<String> applicants = getStringListFromNameOrgObject(applicantsObj);
        if (!applicants.isEmpty()) {
            item.setApplicants(applicants);
        }

        Object inventorsObj = source.get("inventors");
        List<String> inventors = getStringListFromNameOrgObject(inventorsObj);
        if (!inventors.isEmpty()) {
            item.setAuthors(inventors);
        }
        if (source.get("earliest_publication_date") != null) {
            item.setPubDate(toDateString(source.get("earliest_publication_date").toString(), "-"));
        }


        //highlight
        if (hit.getHighlightFields() != null) {
            for (Map.Entry<String, HighlightField> entry : hit.getHighlightFields().entrySet()) {
                Text[] frags = entry.getValue().getFragments();
                switch (entry.getKey()) {
                    case "title.original":
                        if (frags != null && frags.length > 0) {
                            item.setTitle(frags[0].string());
                        }
                        break;
                    case "abstract.original":
                        if (frags != null && frags.length > 0) {
                            item.setAbs(frags[0].string());
                        }
                        break;
                    case "applicants.name.original.keyword":
                        if (frags != null && frags.length > 0) {
                            ListIterator<String> itr = item.getApplicants().listIterator();
                            setHighlightElements(frags, itr);
                        }
                        break;
                    case "inventors.name.original.keyword":
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

    public BoolQueryBuilder buildQueryPatent(QueryRequest request) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        makeFilters(request, boolQuery);

        TermQueryBuilder titleTerm = QueryBuilders.termQuery("title.original", request.getKw()).boost(1.5f);
        TermQueryBuilder abstractTerm = QueryBuilders.termQuery("abstract.original", request.getKw());
        TermQueryBuilder inventorTerm = QueryBuilders.termQuery("inventors.name.original.keyword", request.getKw())
                .boost(2);
        TermQueryBuilder applicantTerm = QueryBuilders.termQuery("applicants.name.original.keyword", request.getKw())
                .boost(1.5f);
        TermQueryBuilder agenciesTerm = QueryBuilders.termQuery("agencies_standerd.agency", request.getKw())
                .boost(1.5f);
        TermQueryBuilder annotationTagTerm = QueryBuilders.termQuery("annotation_tag.name", request.getKw())
                .boost(1.5f);

        BoolQueryBuilder termQuery = QueryBuilders.boolQuery().minimumShouldMatch(1);
        if (request.getKwType() == null || request.getKwType() == 0) {
            termQuery.should(titleTerm);
            termQuery.should(abstractTerm);
            termQuery.should(inventorTerm);
            termQuery.should(agenciesTerm);
            termQuery.should(applicantTerm);
            termQuery.should(annotationTagTerm);
            if (!StringUtils.isEmpty(request.getOtherKw())) {
                termQuery.should(QueryBuilders.termQuery("applicants.name.original.keyword", request.getOtherKw()));
            }
        } else if (request.getKwType() == 1) {
            termQuery.should(inventorTerm);
            termQuery.should(applicantTerm);
            if (!StringUtils.isEmpty(request.getOtherKw())) {
                termQuery.should(QueryBuilders.termQuery("applicants.name.original.keyword", request.getOtherKw()));
            }
        } else if (request.getKwType() == 2) {
            termQuery.should(agenciesTerm);
            termQuery.should(applicantTerm);
        } else if (request.getKwType() == 3) {
            termQuery.should(titleTerm);
            termQuery.should(abstractTerm);
            termQuery.should(annotationTagTerm);
        }

        boolQuery.must(termQuery);
        boolQuery.filter(QueryBuilders.termQuery("_type", "patent_data"));
        return boolQuery;
    }
}
