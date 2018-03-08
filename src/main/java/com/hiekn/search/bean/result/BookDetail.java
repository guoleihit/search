package com.hiekn.search.bean.result;

import java.util.List;

public class BookDetail extends BookItem {
    private String cipno;
    /**
     * 版次印次
     */
    private String printNum;
    /**
     * 开本
     */
    private String format;

    /**
     * 印数
     */
    private String printings;

    /**
     * 纸质书定价
     */
    private String paperBookPrice;

    private List<Object> catalog;

    private List<Object> references;

    public String getCipno() {
        return cipno;
    }

    public void setCipno(String cipno) {
        this.cipno = cipno;
    }

    public String getPrintNum() {
        return printNum;
    }

    public void setPrintNum(String printNum) {
        this.printNum = printNum;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getPrintings() {
        return printings;
    }

    public void setPrintings(String printings) {
        this.printings = printings;
    }

    public String getPaperBookPrice() {
        return paperBookPrice;
    }

    public void setPaperBookPrice(String paperBookPrice) {
        this.paperBookPrice = paperBookPrice;
    }

    public List<Object> getCatalog() {
        return catalog;
    }

    public void setCatalog(List<Object> catalog) {
        this.catalog = catalog;
    }

    public List<Object> getReferences() {
        return references;
    }

    public void setReferences(List<Object> references) {
        this.references = references;
    }
}
