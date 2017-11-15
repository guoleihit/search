package com.hiekn.search.rest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;

import com.hiekn.search.bean.result.Code;
import com.hiekn.search.bean.result.RestResp;
import com.hiekn.search.exception.BaseException;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Controller
@Path("/a")
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
@Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
@Api(tags = { "认证" })
public class AuthenticationRestApi implements InitializingBean{

	private static Logger log = LoggerFactory.getLogger(AuthenticationRestApi.class);

	//@Value("#{'${users}'.split(',')}")
	private List<String> users;
	
	private Map<String,String> userMap;
	@POST
	@Path("/auth")
	@ApiOperation(value = "authenticate")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "成功", response = RestResp.class),
			@ApiResponse(code = 500, message = "失败") })
	public RestResp<Boolean> auth(@FormParam("user")String user, @FormParam("password")String passwd,@QueryParam("tt") Long tt) {
		if(StringUtils.isEmpty(user)) {
			throw new BaseException(Code.NEED_USER_INFO_ERROR.getCode());
		}
		log.info("user:"+ user+ ",passwd:"+passwd);
		Boolean isSuccess = false;
		if(userMap.get(user)!=null && userMap.get(user).equals(passwd)) {
			isSuccess = true;
		}
		return new RestResp<>(isSuccess, tt);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		userMap = new ConcurrentHashMap<>();
		if(users != null && !users.isEmpty()) {
			for(String user: users) {
				String[] pair = user.split("/");
				if(pair.length == 2) {
					userMap.put(pair[0], pair[1]);
					log.info("got user:"+pair[0] + "," +pair[1]);
				}
			}
		}
		
	}
}
