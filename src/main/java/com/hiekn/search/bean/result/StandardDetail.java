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

    private String eName;



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
