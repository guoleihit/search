package com.hiekn.search.bean.result;

import java.util.List;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.hiekn.search.bean.DocType;


@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class StandardItem extends ItemBean{

	public StandardItem() {
		setDocType(DocType.STANDARD);
	}

    public String getNum() {
		return num;
	}

	public void setNum(String num) {
		this.num = num;
	}

	public String getInterNum() {
		return interNum;
	}

	public void setInterNum(String interNum) {
		this.interNum = interNum;
	}

	public String getInterName() {
		return interName;
	}

	public void setInterName(String interName) {
		this.interName = interName;
	}

	public String getConsistent() {
		return consistent;
	}

	public void setConsistent(String consistent) {
		this.consistent = consistent;
	}

	public String getRelation() {
		return relation;
	}

	public void setRelation(String relation) {
		this.relation = relation;
	}

	public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getCcs() {
        return ccs;
    }

    public void setCcs(String ccs) {
        this.ccs = ccs;
    }

    public String getIcs() {
        return ics;
    }

    public void setIcs(String ics) {
        this.ics = ics;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getSubNum() {
        return subNum;
    }

    public void setSubNum(String subNum) {
        this.subNum = subNum;
    }

    public String getYield() {
        return yield;
    }

    public void setYield(String yield) {
        this.yield = yield;
    }

    public List<String> getQuote() {
        return quote;
    }

    public void setQuote(List<String> quote) {
        this.quote = quote;
    }

    public List<String> getTerm() {
        return term;
    }

    public void setTerm(List<String> term) {
        this.term = term;
    }

    public String getPubDep() {
        return pubDep;
    }

    public void setPubDep(String pubDep) {
        this.pubDep = pubDep;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getISBN() {
        return ISBN;
    }

    public void setISBN(String ISBN) {
        this.ISBN = ISBN;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getBY() {
        return BY;
    }

    public void setBY(String BY) {
        this.BY = BY;
    }

    public String getPrintNum() {
        return printNum;
    }

    public void setPrintNum(String printNum) {
        this.printNum = printNum;
    }

    public String getWordNum() {
        return wordNum;
    }

    public void setWordNum(String wordNum) {
        this.wordNum = wordNum;
    }

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public String getPdfPage() {
        return pdfPage;
    }

    public void setPdfPage(String pdfPage) {
        this.pdfPage = pdfPage;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    /**
     * 标注类型: GB
     */
	private String type;

	private String language;

    /**
     * 中国分类标准
     */
	private String ccs;

    /**
     * 国际标准分类
     */
	private String ics;


    /**
     * 现行,作废,待出版
     */
	private String state;

    /**
     * 代替标准
     */
	private String subNum;
	
	/**
	 * 标准号
	 */
	private String num;


	private String interNum;
	
	private String interName;
	
    private String consistent;
    
    /**
     * 范围
     */
    private String yield;

    private List<String> quote;

    private List<String> term;

    /**
     * 发行单位
     */
    private String pubDep;

    private String telephone;

    private String address;

    private String ISBN;

    /**
     * 开本
     */
    private String format;

    /**
     * 版次印次
     */
    private String BY;

    private String printNum;

    private String wordNum;

    private String page;

    private String pdfPage;

    private String price;
    
    private String relation;
}
