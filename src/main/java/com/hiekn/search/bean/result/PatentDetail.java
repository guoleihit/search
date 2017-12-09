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

	private String mainIPC;
	private List<String> ipces;
	private List<String> priorities;
	private Integer pages;
	private List<String> countries;

	private String legalStatus;

	private String type;

	private String earliestPrioritiesDate;

	private List<String> priorityDetails;

    public List<String> getPriorityDetails() {
        return priorityDetails;
    }

    public void setPriorityDetails(List<String> priorityDetails) {
        this.priorityDetails = priorityDetails;
    }

    public String getEarliestPrioritiesDate() {
        return earliestPrioritiesDate;
    }

    public void setEarliestPrioritiesDate(String earliestPrioritiesDate) {
        this.earliestPrioritiesDate = earliestPrioritiesDate;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLegalStatus() {
        return legalStatus;
    }

    public void setLegalStatus(String legalStatus) {
        this.legalStatus = legalStatus;
    }

    public List<String> getCountries() {
        return countries;
    }

    public void setCountries(List<String> countries) {
        this.countries = countries;
    }

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
