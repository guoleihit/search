package com.hiekn.search.app;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

import com.hiekn.search.exception.ExceptionHandler;
import com.hiekn.util.CommonResource;

import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;

public class Application  extends ResourceConfig{
	public Application() {
		packages("com.hiekn.search.rest,com.hiekn.plantdata.rest");
		
		register(JacksonJsonProvider.class);
		register(MultiPartFeature.class);
		
		register(ExceptionHandler.class);
		
		register(ApiListingResource.class);
		register(SwaggerSerializers.class);
		initSwagger();
		
	}
	
	private void initSwagger(){
		BeanConfig beanConfig = new BeanConfig();
		beanConfig.setVersion("1.0.0");
		beanConfig.setTitle("search");
		beanConfig.setDescription("GW Search全部API");
		beanConfig.setHost(CommonResource.swagger_ip_port);
		beanConfig.setBasePath(CommonResource.swagger_base_path);
		beanConfig.setResourcePackage("com.hiekn.search.rest,com.hiekn.plantdata.rest");
		beanConfig.setScan(true);
	}
}
