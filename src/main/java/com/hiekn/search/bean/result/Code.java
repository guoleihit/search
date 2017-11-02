package com.hiekn.search.bean.result;

import java.util.Objects;

public enum Code {
	
	PARAM_QUERY_EMPTY_ERROR(30001,"query string is empty"),
	PARAM_CAT_ERROR(30002,"cats json parse error."),
	PARAM_BRAND_ERROR(30003,"brands json parse error."),
	PARAM_STORE_ERROR(30004,"stores json parse error."),
	
	PARAM_PAGENO_TOO_SMALL_ERROR(30011,"pageno is too small, minimum value 1"),
	PARAM_PAGENO_TOO_BIG_ERROR(30012,"pageno is too large."),
	PARAM_PAGESIZE_TOO_SMALL_ERROR(30013,"pagesize is too small, minimum value 1"),
	PARAM_PAGESIZE_TOO_BIG_ERROR(30014,"pagesize is too large."),
	
	JSON_ERROR(40001,"json转换失败"),
	
	HTTP_ERROR(80001,"http相关错误"),
	
	SERVICE_ERROR(90000,"服务端错误"),
	REMOTE_SERVICE_PARSE_ERROR(90001,"远程数据解析错误"),
	REMOTE_SERVICE_ERROR(90002,"远程服务错误");
	
	private Integer code;
	private String errorInfo;
	
	private Code(Integer code,String errorInfo){
		this.code = code ;
		this.errorInfo = errorInfo;
	}
	
	public Integer getCode() {
		return code;
	}

	public String getInfo() {
		return errorInfo;
	}

	public static String getMsg(Integer code){
		for (Code error : Code.values()) {
			if (Objects.equals(code,error.getCode())) {
				return error.getInfo();
			}
		}
		return "";
	}

}
