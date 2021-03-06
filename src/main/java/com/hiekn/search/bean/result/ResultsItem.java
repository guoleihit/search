package com.hiekn.search.bean.result;

import java.util.List;

public class ResultsItem extends ItemBean {

    /**
     * 登记号
     */
    private String no;

    /**
     * 来源
     **/
    private String origin;

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
    private List<String> complete_department;

    /**
     * 成果类型
     */
    private String resultsType;

    public String getResultsType() {
        return resultsType;
    }

    public void setResultsType(String resultsType) {
        this.resultsType = resultsType;
    }

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
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

    public List<String> getComplete_department() {
        return complete_department;
    }

    public void setComplete_department(List<String> complete_department) {
        this.complete_department = complete_department;
    }
}
