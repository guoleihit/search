package com.hiekn.search.exception;

/** 
 * 异常基类，各个模块的运行期异常均继承与该类 
 */  
public class BaseException extends RuntimeException {  
   
    private static final long serialVersionUID = 1381325479896057076L;  
  
    private Integer code;

    public BaseException(Integer code) {
    	super();
    	this.code = code;
    }
    
	public Integer getCode() {
		return code;
	}

	public void setCode(Integer code) {
		this.code = code;
	}


}