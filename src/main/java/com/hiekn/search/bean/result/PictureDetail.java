package com.hiekn.search.bean.result;

import java.util.List;

import org.codehaus.jackson.map.annotate.JsonSerialize;

import com.hiekn.search.bean.DocType;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class PictureDetail extends ItemBean{

	private List<String> photographerInfo;
	private String photoPlace;
	private String storeSize;
	private Boolean allowedPublication;
	private String lang;
	private List<String> keywords;
	private List<String> categories;
	private String comments;
	private String size;

	public PictureDetail() {
		setDocType(DocType.PICTURE);
	}
	
	public List<String> getCategories() {
		return categories;
	}

	public void setCategories(List<String> categories) {
		this.categories = categories;
	}


	public List<String> getPhotographerInfo() {
		return photographerInfo;
	}
	public void setPhotographerInfo(List<String> photographerInfo) {
		this.photographerInfo = photographerInfo;
	}
	public String getPhotoPlace() {
		return photoPlace;
	}
	public void setPhotoPlace(String photoPlace) {
		this.photoPlace = photoPlace;
	}
	public String getStoreSize() {
		return storeSize;
	}
	public void setStoreSize(String storeSize) {
		this.storeSize = storeSize;
	}
	public Boolean getAllowedPublication() {
		return allowedPublication;
	}
	public void setAllowedPublication(Boolean allowedPublication) {
		this.allowedPublication = allowedPublication;
	}
	public String getLang() {
		return lang;
	}
	public void setLang(String lang) {
		this.lang = lang;
	}
	public List<String> getKeywords() {
		return keywords;
	}
	public void setKeywords(List<String> keywords) {
		this.keywords = keywords;
	}

	public String getComments() {
		return comments;
	}
	public void setComments(String comments) {
		this.comments = comments;
	}
	public String getSize() {
		return size;
	}
	public void setSize(String size) {
		this.size = size;
	}
}
