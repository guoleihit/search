package com.hiekn.search.bean.result;

import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.hiekn.search.bean.DocType;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class PatentDetail extends ItemBean {

	private String applicationNumber;
	private String applicationDate;
	private String publicationNumber;

	private List<Map<String, Object>> applicants;
	private List<String> agencies;
	private List<String> agents;
	private List<String> priorities;
	private String mainIPC;
	private List<String> ipces;

	private Integer pages;
	
	public PatentDetail() {
		setDocType(DocType.PATENT);
	}

	public Integer getPages() {
		return pages;
	}

	public void setPages(Integer pages) {
		this.pages = pages;
	}

	public String getApplicationNumber() {
		return applicationNumber;
	}

	public void setApplicationNumber(String applicationNumber) {
		this.applicationNumber = applicationNumber;
	}

	public String getApplicationDate() {
		return applicationDate;
	}

	public void setApplicationDate(String applicationDate) {
		this.applicationDate = applicationDate;
	}

	public String getPublicationNumber() {
		return publicationNumber;
	}

	public void setPublicationNumber(String publicationNumber) {
		this.publicationNumber = publicationNumber;
	}

	public List<Map<String, Object>> getApplicants() {
		return applicants;
	}

	public void setApplicants(List<Map<String, Object>> applicants) {
		this.applicants = applicants;
	}

	public List<String> getAgencies() {
		return agencies;
	}

	public void setAgencies(List<String> agencies) {
		this.agencies = agencies;
	}

	public List<String> getAgents() {
		return agents;
	}

	public void setAgents(List<String> agents) {
		this.agents = agents;
	}

	public List<String> getPriorities() {
		return priorities;
	}

	public void setPriorities(List<String> priorities) {
		this.priorities = priorities;
	}

	public String getMainIPC() {
		return mainIPC;
	}

	public void setMainIPC(String mainIPC) {
		this.mainIPC = mainIPC;
	}

	public List<String> getIpces() {
		return ipces;
	}

	public void setIpces(List<String> ipces) {
		this.ipces = ipces;
	}

}
