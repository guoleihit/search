package com.hiekn.search.bean.prompt;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * 提示结果条目
 * @author leiguo
 *
 */
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
public class PromptBean {
	
	/**
	 * 现实名称
	 */
	private String name;

	/**
	 * 提示词类别
	 * (作者=1、机构=2、关键词=3)
	 */
	private Integer type = 0;
	
	private String description;

	private String graphId;

	public String getGraphId() {
		return graphId;
	}

	public void setGraphId(String graphId) {
		this.graphId = graphId;
	}

	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Integer getType() {
		return type;
	}
	public void setType(Integer type) {
		this.type = type;
	}
	@Override
	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this, false);
	}
	@Override
	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj, false);
	}

}
