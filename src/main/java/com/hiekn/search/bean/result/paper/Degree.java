package com.hiekn.search.bean.result.paper;

import com.hiekn.search.bean.result.PaperDetail;
import java.util.List;

public class Degree extends PaperDetail {

    private String university;
    private String degree;
    private String major;
    private String degreeYear;
    private List<String> mentor;

    public Degree(){
        super();
        setPaperType(PaperType.DEGREE);
    }

    public String getUniversity() {
        return university;
    }

    public void setUniversity(String university) {
        this.university = university;
    }

    public String getDegree() {
        return degree;
    }

    public void setDegree(String degree) {
        this.degree = degree;
    }

    public String getMajor() {
        return major;
    }

    public void setMajor(String major) {
        this.major = major;
    }

    public String getDegreeYear() {
        return degreeYear;
    }

    public void setDegreeYear(String degreeYear) {
        this.degreeYear = degreeYear;
    }

    public List<String> getMentor() {
        return mentor;
    }

    public void setMentor(List<String> mentor) {
        this.mentor = mentor;
    }
}
