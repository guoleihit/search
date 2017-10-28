package com.hiekn.search.bean.result;

import java.util.List;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.hiekn.search.bean.DocType;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class PatentItem extends ItemBean {

	private List<String> agencies;
	private List<String> applicants;
	private String applicationDate;

	public PatentItem() {
		setDocType(DocType.PATENT);
	}
	public List<String> getAgencies() {
		return agencies;
	}

	public void setAgencies(List<String> agencies) {
		this.agencies = agencies;
	}

	public List<String> getApplicants() {
		return applicants;
	}

	public void setApplicants(List<String> applicants) {
		this.applicants = applicants;
	}

	public String getApplicationDate() {
		return applicationDate;
	}

	public void setApplicationDate(String applicationDate) {
		this.applicationDate = applicationDate;
	}
}
