package com.hiekn.service.nlp;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.corpus.tag.Nature;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.dictionary.py.Pinyin;
import com.hankcs.hanlp.seg.CRF.CRFSegment;
import com.hankcs.hanlp.seg.NShort.NShortSegment;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.summary.TextRankKeyword;
import com.hankcs.hanlp.tokenizer.IndexTokenizer;
import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import com.hiekn.search.bean.SimpleTerm;
import org.ansj.app.summary.SummaryComputer;
import org.ansj.app.summary.TagContent;
import org.ansj.app.summary.pojo.Summary;
import org.ansj.dic.LearnTool;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

@Service
public class NLPServiceImpl {

    private static Log log = LogFactory.getLog(NLPServiceImpl.class);

    private Segment nShortSegment = null;
    private Segment crfSegment = null;

    @PostConstruct
    void doInit() throws Exception{

        //nshorest
        nShortSegment = new NShortSegment().enableCustomDictionary(true).enableAllNamedEntityRecognize(true);

        //crfSegment
        crfSegment = new CRFSegment().enablePartOfSpeechTagging(true);

    }
    /**
     * 简体转繁体
     * @Title: j2fService
     * @param text
     * @return
     * @throws
     * @author wzh
     * @date 2017年9月5日 下午5:24:33
     * @Email hexiang@hiekn.com
     */
    public String j2f(String text){
        return HanLP.convertToTraditionalChinese(text);
    }

    /**
     * 繁体转简体
     * @Title: f2jService
     * @param text
     * @return
     * @throws
     * @author wzh
     * @date 2017年9月5日 下午5:25:35
     * @Email hexiang@hiekn.com
     */
    public String f2j(String text){
        return HanLP.convertToSimplifiedChinese(text);
    }


    /**
     * 转拼音
     * @Title: pinyinService
     * @param text
     * @return
     * @throws
     * @author wzh
     * @date 2017年9月5日 下午5:29:08
     * @Email hexiang@hiekn.com
     */
    public String pinyin(String text){
        StringBuffer sbPy = new StringBuffer();
        List<Pinyin> pinyinList = HanLP.convertToPinyinList(text);
        for (Pinyin pinyin : pinyinList){
            sbPy.append(pinyin.getPinyinWithToneMark() + " ");
        }
        return sbPy.toString().trim();
    }

    /**
     * 分词
     * @param type
     * @param text
     * @param dicList
     * @return
     */
    public List<SimpleTerm> seg(String type, String text, List<String> dicList, boolean useNewwordDetect){


        /**
         * 目前测试此处新词发现ansj的效果比hanlp的crf好
         * 1.索性每次先用ansj做新词发现
         * 2.把发现的词加入后续的词典
         * 3.调整hanlp返回的分词newword=true
         */
        List<String> newwordList = null;
        if(useNewwordDetect){
            newwordList = this.newword(text,10);
            for (String s : newwordList) {
                CustomDictionary.add(s);
            }
        }

        List<SimpleTerm> simpleTermList = Lists.newArrayList();

        for(String word : dicList){
            if(word == null || word.equals("")) continue;
            CustomDictionary.add(word);
        }
        switch (type) {
            //NLP分词
            case "nlp":
                List<Term> nlpTermList = NLPTokenizer.segment(text);
                for(Term term : nlpTermList){
                    simpleTermList.add(new SimpleTerm(term.word, natrueMappingService(term.nature)));
                }
                break;
            //索引分词
            case "index":
                List<Term> indexTermList = IndexTokenizer.segment(text);
                for(Term term : indexTermList){
                    simpleTermList.add(new SimpleTerm(term.word, natrueMappingService(term.nature)));
                }
                break;
            //N最短路径分词
            case "nshort":

                List<Term> nshortTermList = this.nShortSegment.seg(text);
                for(Term term : nshortTermList){
                    simpleTermList.add(new SimpleTerm(term.word, natrueMappingService(term.nature)));
                }
                break;

            //crf分词
            case "crf":

                List<Term> crfTermList = this.crfSegment.seg(text);
                for(Term term : crfTermList){
                    simpleTermList.add(new SimpleTerm(term.word, natrueMappingService(term.nature)));
                }
                break;

            //标准分词
            case "std":
                List<Term> stdTermList = HanLP.segment(text);
                for(Term term : stdTermList){
                    simpleTermList.add(new SimpleTerm(term.word, natrueMappingService(term.nature)));
                }
                break;

            default:
                List<Term> defaultTermList = HanLP.segment(text);
                for(Term term : defaultTermList){
                    simpleTermList.add(new SimpleTerm(term.word, natrueMappingService(term.nature)));
                }
                break;
        }

        /**
         * ansj的新词发现 反向去匹配hanlp的分词
         */
        if(useNewwordDetect && newwordList.size()>0){
            for (SimpleTerm simpleTerm : simpleTermList) {
                if(newwordList.contains(simpleTerm.getWord())){
                    simpleTerm.setNewWord(true);
                }
            }
        }

        return simpleTermList;
    }

    /**
     * ansj 暂时不用
     * @param text
     * @param size
     * @return
     */
    public List<String> newword(String text, Integer size){

        List<String> rsList = Lists.newArrayList();

        LearnTool learnTool = new LearnTool() ;

        NlpAnalysis nlpAnalysis = new NlpAnalysis();

        nlpAnalysis.setLearnTool(learnTool);

        nlpAnalysis.parseStr(text);


        if (learnTool.getTopTree(size) == null) {
            return rsList;
        }

        for (Map.Entry<String, Double> stringDoubleEntry : learnTool.getTopTree(size)) {
            rsList.add(stringDoubleEntry.getKey());
        }

        return rsList;
    }

    /**
     * 关键词抽取 hanlp
     */
    public List<Map<String,Object>> keywords(String text, Integer size) throws Exception{

        List<Map<String,Object>> rsList = Lists.newArrayList();

        Map<String, Float> keywordsMap = new TextRankKeyword().getTermAndRank(text, size);

        if(keywordsMap!=null){

            for(Map.Entry<String, Float> map : keywordsMap.entrySet()){

                Map one = Maps.newHashMap();

                one.put("word", map.getKey());
                one.put("score", map.getValue());
                rsList.add(one);
            }
        }

        return rsList;
    }

    /**
     * 实体识别
     * @Title: entity
     * @param text
     * @return
     * @throws
     * @author wzh
     * @date 2017年9月7日 上午11:52:45
     * @Email hexiang@hiekn.com
     */
    public Map<String, LinkedHashSet<String>> entity(String text){

        Map<String, LinkedHashSet<String>> entityMap = Maps.newHashMap();
        entityMap.put("机构", new LinkedHashSet<String>());
        entityMap.put("地点", new LinkedHashSet<String>());
        entityMap.put("时间", new LinkedHashSet<String>());
        entityMap.put("人名", new LinkedHashSet<String>());
        entityMap.put("其他专名", new LinkedHashSet<String>());

        Segment nShortSegment = new NShortSegment().enableCustomDictionary(false).enableAllNamedEntityRecognize(true);

        List<Term> nshortTermList = nShortSegment.seg(text);

        for(Term term : nshortTermList){

            String word = term.word;
            String nature = natrueMappingService(term.nature);
            switch (nature) {
                case "nr":
                    entityMap.get("人名").add(word);
                    break;
                case "ns":
                    entityMap.get("地点").add(word);
                    break;
                case "nt":
                    entityMap.get("机构").add(word);
                    break;
                case "nz":
                    entityMap.get("其他专名").add(word);
                    break;
                default:
                    break;
            }
        }

        return entityMap;
    }

    /**
     * 自动摘要
     * @Title: summary
     * @param text
     * @return
     * @throws
     * @author wzh
     * @date 2017年9月8日 下午4:20:11
     * @Email hexiang@hiekn.com
     */
    public String summary(String text){

        SummaryComputer summaryComputer = new SummaryComputer("", text);

        Summary summary = summaryComputer.toSummary();

        TagContent tw = new TagContent("<font color=\"red\">", "</font>");

        String tagContent = tw.tagContent(summary); // 标记后的摘要

        return tagContent;
    }





    /**
     * 词性映射
     * @Title: natrueMappingService
     * @return
     * @throws
     * @author wzh
     * @date 2017年9月5日 下午6:58:54
     * @Email hexiang@hiekn.com
     */
    private String natrueMappingService(Nature nature){

        if(nature==null){
            return "newword";
        }

        String oldNature = nature.name();

        char firstChar = oldNature.charAt(0);

        if(firstChar == 'n'){
            if(oldNature.startsWith("nr")){
                return "nr";
            }else if(oldNature.startsWith("nt")){
                return "nt";
            }else if(oldNature.startsWith("ns")){
                return "ns";
            }else if(oldNature.startsWith("nz")){
                return "nz";
            }else{
                return firstChar + "";
            }
        }else{
            return firstChar + "";
        }
    }

    /**
     * 语法依存分析 haNLP
     * @param text
     * @return
     */
//    public CoNLLSentence dependency(String text){
//        return HanLP.parseDependency(text);
//    }
}
