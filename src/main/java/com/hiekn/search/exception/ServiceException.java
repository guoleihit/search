package com.hiekn.search.exception;

import com.hiekn.search.bean.result.Code;

public class ServiceException extends BaseException{
	
	private static final long serialVersionUID = 1L;
	
	public ServiceException(Integer code) {
		super(code);
	}

	public static ServiceException newInstance(){
		return newInstance(Code.SERVICE_ERROR.getCode());
	}
	
	public static ServiceException newInstance(Integer code){
		return new ServiceException(code);
	}

}
