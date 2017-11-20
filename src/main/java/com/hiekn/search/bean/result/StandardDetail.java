package com.hiekn.search.bean.result;

import com.hiekn.search.bean.DocType;

public class StandardDetail extends StandardItem{

    public StandardDetail() {
        setDocType(DocType.STANDARD);
    }

    public String geteName() {
        return eName;
    }

    public void seteName(String eName) {
        this.eName = eName;
    }

    public String getIssueDep() {
        return issueDep;
    }

    public void setIssueDep(String issueDep) {
        this.issueDep = issueDep;
    }

    public String getManageDep() {
        return manageDep;
    }

    public void setManageDep(String manageDep) {
        this.manageDep = manageDep;
    }

    public String getAuthorDep() {
        return authorDep;
    }

    public void setAuthorDep(String authorDep) {
        this.authorDep = authorDep;
    }

    public String getConsistent() {
        return consistent;
    }

    public void setConsistent(String consistent) {
        this.consistent = consistent;
    }

    public String getCarryonDate() {
        return carryonDate;

    }

    public void setCarryonDate(String carryonDate) {
        this.carryonDate = carryonDate;
    }

    private String eName;

    /**
     * 实施时间
     */
    private String carryonDate;

    /**
     * 发布单位
     */
    private String issueDep;

    /**
     * 归口单位
     */
    private String manageDep;

    /**
     * 起草单位
     */
    private String authorDep;

    /**
     * 一致性
     */
    private String consistent;
}
