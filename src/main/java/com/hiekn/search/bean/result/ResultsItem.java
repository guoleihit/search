package com.hiekn.search.bean.result;

public class ResultsItem extends ItemBean {

    /**
     * 登记号
     */
    private String no;

    /**
     * 来源
     **/
    private String from;

    /**
     * 省市
     **/
    private String province_city;

    /**
     * 技术类别
     */
    private String tech_type;

    /**
     * 完成单位
     **/
    private String complete_department;

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getProvince_city() {
        return province_city;
    }

    public void setProvince_city(String province_city) {
        this.province_city = province_city;
    }

    public String getTech_type() {
        return tech_type;
    }

    public void setTech_type(String tech_type) {
        this.tech_type = tech_type;
    }

    public String getComplete_department() {
        return complete_department;
    }

    public void setComplete_department(String complete_department) {
        this.complete_department = complete_department;
    }
}
