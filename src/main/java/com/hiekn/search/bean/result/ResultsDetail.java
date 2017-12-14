package com.hiekn.search.bean.result;

import java.util.List;

public class ResultsDetail extends ResultsItem {



    /**
     * 成果属性
     */
    private String attribute;


    /**
     * 经费投入
     */
    private String infest_money;

    /**
     * 研究形式
     **/
    private String org_type;
    /**
     * 推广形式
     **/
    private String pro_type;

    /**
     * 所处阶段
     **/
    private String round;



    /**
     * url
     **/
    private String url;

    /**
     * 项目年度编号
     **/
    private String project_number;

    /**
     * 限制使用
     **/
    private String restricted;



    /**
     * 中图分类号
     **/
    private String classification_number;

    /**
     * 关键词
     **/
    private List<String> keywords;

    /**
     * 鉴定部门
     **/
    private String authorized_department;

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public String getInfest_money() {
        return infest_money;
    }

    public void setInfest_money(String infest_money) {
        this.infest_money = infest_money;
    }

    public String getOrg_type() {
        return org_type;
    }

    public void setOrg_type(String org_type) {
        this.org_type = org_type;
    }

    public String getPro_type() {
        return pro_type;
    }

    public void setPro_type(String pro_type) {
        this.pro_type = pro_type;
    }

    public String getRound() {
        return round;
    }

    public void setRound(String round) {
        this.round = round;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getProject_number() {
        return project_number;
    }

    public void setProject_number(String project_number) {
        this.project_number = project_number;
    }

    public String getRestricted() {
        return restricted;
    }

    public void setRestricted(String restricted) {
        this.restricted = restricted;
    }

    public String getClassification_number() {
        return classification_number;
    }

    public void setClassification_number(String classification_number) {
        this.classification_number = classification_number;
    }

    public List<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }

    public String getAuthorized_department() {
        return authorized_department;
    }

    public void setAuthorized_department(String authorized_department) {
        this.authorized_department = authorized_department;
    }

    public String getRecommend_department() {
        return recommend_department;
    }

    public void setRecommend_department(String recommend_department) {
        this.recommend_department = recommend_department;
    }

    public String getIndustry_name() {
        return industry_name;
    }

    public void setIndustry_name(String industry_name) {
        this.industry_name = industry_name;
    }

    public String getIndustry_code() {
        return industry_code;
    }

    public void setIndustry_code(String industry_code) {
        this.industry_code = industry_code;
    }

    public String getTransfer_information() {
        return transfer_information;
    }

    public void setTransfer_information(String transfer_information) {
        this.transfer_information = transfer_information;
    }

    public String getContact_department_name() {
        return contact_department_name;
    }

    public void setContact_department_name(String contact_department_name) {
        this.contact_department_name = contact_department_name;
    }

    public String getContact_department_address() {
        return contact_department_address;
    }

    public void setContact_department_address(String contact_department_address) {
        this.contact_department_address = contact_department_address;
    }

    public String getFaxes() {
        return faxes;
    }

    public void setFaxes(String faxes) {
        this.faxes = faxes;
    }

    public String getPost_code() {
        return post_code;
    }

    public void setPost_code(String post_code) {
        this.post_code = post_code;
    }

    public List<Object> getRelated_standard() {
        return related_standard;
    }

    public void setRelated_standard(List<Object> related_standard) {
        this.related_standard = related_standard;
    }

    public List<Object> getRelated_patent() {
        return related_patent;
    }

    public void setRelated_patent(List<Object> related_patent) {
        this.related_patent = related_patent;
    }

    public List<Object> getRelated_scholar() {
        return related_scholar;
    }

    public void setRelated_scholar(List<Object> related_scholar) {
        this.related_scholar = related_scholar;
    }

    public List<String> getRelated_term() {
        return related_term;
    }

    public void setRelated_term(List<String> related_term) {
        this.related_term = related_term;
    }

    public String getProject_begin() {
        return project_begin;
    }

    public void setProject_begin(String project_begin) {
        this.project_begin = project_begin;
    }

    public String getProject_end() {
        return project_end;
    }

    public void setProject_end(String project_end) {
        this.project_end = project_end;
    }

    public String getProject_background() {
        return project_background;
    }

    public void setProject_background(String project_background) {
        this.project_background = project_background;
    }

    public String getInnovation_point() {
        return innovation_point;
    }

    public void setInnovation_point(String innovation_point) {
        this.innovation_point = innovation_point;
    }

    public String getReview_level() {
        return review_level;
    }

    public void setReview_level(String review_level) {
        this.review_level = review_level;
    }

    public String getAward_department() {
        return award_department;
    }

    public void setAward_department(String award_department) {
        this.award_department = award_department;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    /**

     * 推荐部门
     **/
    private String recommend_department;

    /**
     * 应用行业名称
     **/
    private String industry_name;

    /**
     * 应用行业码
     **/
    private String industry_code;

    /**
     * 转让信息
     **/
    private String transfer_information;

    /**
     * 联系单位名称
     **/
    private String contact_department_name;

    /**
     * 联系单位地址
     **/
    private String contact_department_address;

    /**
     * 传真
     **/
    private String faxes;

    /**
     * 邮政编码
     **/
    private String post_code;

    /**
     * 相关标准
     **/
    private List<Object> related_standard;

    /**
     * 相关专利
     **/
    private List<Object> related_patent;

    /**
     * 相关学者
     **/
    private List<Object> related_scholar;

    /**
     * 相关检索词
     **/
    private List<String> related_term;

    /**
     * 项目开始时间
     **/
    private String project_begin;
    /**
     * 项目结束时间
     **/
    private String project_end;

    /**
     * 立项背景
     **/
    private String project_background;

    /**
     * 创新点
     **/
    private String innovation_point;


    /**
     * 评审等级
     **/
    private String review_level;

    /**
     * 授奖单位
     **/
    private String award_department;

    /**
     * 备注
     **/
    private String remark;

}
