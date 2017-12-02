package com.hiekn.search.bean;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * 文档类型
 * (专利、论文、百科、新闻、案例)
 * @author leiguo
 *
 */
public enum DocType {
	PATENT("PATENT"), PAPER("PAPER"), STANDARD("STANDARD"), PICTURE("PICTURE") ,BAIKE("BAIKE"), NEWS("NEWS");

	private final String name;

	@JsonCreator
	DocType(String name) {
		this.name = name;
	}

	@JsonValue
	public String getName() {
		return name;
	}
}
