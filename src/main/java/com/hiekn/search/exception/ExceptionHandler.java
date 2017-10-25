package com.hiekn.search.exception;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.NotAcceptableException;
import javax.ws.rs.NotAllowedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiekn.search.bean.result.Code;
import com.hiekn.search.bean.result.RestResp;

public class ExceptionHandler implements ExceptionMapper<Exception> {
	
	private static Logger log = LoggerFactory.getLogger(ExceptionHandler.class);
	
	@Context  
    private HttpServletRequest request;  
	
	@Override
	public Response toResponse(Exception exception) {
		String t = request.getParameter("tt");
		long tt = StringUtils.isBlank(t)?0L:Long.parseLong(t);
		Integer code = null;
		RestResp<Object> resp = null;
		Status statusCode = Status.OK;
		if(exception instanceof BaseException){
			code = ((BaseException) exception).getCode();
		}else if(exception instanceof WebApplicationException){
			code = Code.HTTP_ERROR.getCode();
			if(exception instanceof NotFoundException){
				statusCode = Status.NOT_FOUND;
			}else if(exception instanceof NotAllowedException){
				statusCode = Status.METHOD_NOT_ALLOWED;
			}else if(exception instanceof NotAcceptableException){
				statusCode = Status.NOT_ACCEPTABLE;
			}
		}else{
			code = Code.SERVICE_ERROR.getCode();
		}
		resp = new RestResp<Object>(code,tt);
		log.error(Code.getMsg(code), exception);  
		return Response.ok(resp).status(statusCode).build();  
	}
	
}
