package com.hiekn.search.exception;

import com.hiekn.search.bean.result.Code;

public class JsonException extends BaseException{
	
	private static final long serialVersionUID = 1L;
	
	public JsonException(Integer code) {
		super(code);
	}

	public static JsonException newInstance(){
		return newInstance(Code.JSON_ERROR.getCode());
	}
	
	public static JsonException newInstance(Integer code){
		return new JsonException(code);
	}

}
