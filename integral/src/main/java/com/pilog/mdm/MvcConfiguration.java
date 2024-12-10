package com.pilog.mdm;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import javax.servlet.MultipartConfigElement;

import org.springframework.web.servlet.View;
import org.springframework.web.servlet.ViewResolver;
import org.springframework.boot.web.servlet.MultipartConfigFactory;
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ResourceBundleMessageSource;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.util.unit.DataSize;
import org.springframework.util.unit.DataUnit;
import org.springframework.web.accept.ContentNegotiationManager;

import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.resource.PathResourceResolver;
import org.springframework.web.servlet.view.ContentNegotiatingViewResolver;
import org.springframework.web.servlet.view.InternalResourceViewResolver;

import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import org.springframework.web.servlet.LocaleResolver;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.DefaultServletHandlerConfigurer;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.i18n.LocaleChangeInterceptor;
import org.springframework.web.servlet.i18n.SessionLocaleResolver;

@Configuration
@ComponentScan
@EnableTransactionManagement
public class MvcConfiguration extends WebMvcConfigurerAdapter
{
//    @Override
//  public void configureViewResolvers(ViewResolverRegistry registry) {
//    InternalResourceViewResolver resolver = new InternalResourceViewResolver();
//    resolver.setPrefix("/WEB-INF/views/");
//    resolver.setSuffix(".jsp");
//    resolver.setViewClass(JstlView.class);
//    registry.viewResolver(resolver);
//  }
	
//	 @Override
//	    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
//	        registry.addHandler(new SocketHandler(), "/insights/socket")
//	            .setAllowedOrigins("*");
//	    }
//    
	
	
	@Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {

        registry.addResourceHandler("/resources/**").addResourceLocations("/resources/static/");
    }
	
    @Bean(name = "jspViewResolver")
    public InternalResourceViewResolver getJspViewResolver() {
        InternalResourceViewResolver viewResolver = new InternalResourceViewResolver();
        viewResolver.setPrefix("/WEB-INF/views/");
        viewResolver.setSuffix(".jsp");
        return viewResolver;
    }
    
    @Bean(name = "jacksonViewResolver")
    public MappingJackson2JsonView getMappingJacksonView() {
        MappingJackson2JsonView jsonViewResolver = new MappingJackson2JsonView();
        jsonViewResolver.setPrefixJson(false);
        jsonViewResolver.setPrettyPrint(false);
        return jsonViewResolver;
    }
    
    @Bean
    public ContentNegotiatingViewResolver getContentNegotiatingViewResolver() {
        ContentNegotiationManager contentNegotiationManager = new ContentNegotiationManager();

        ContentNegotiatingViewResolver resolver = new ContentNegotiatingViewResolver();
        List<ViewResolver> resolvers = new ArrayList<ViewResolver>();
        resolvers.add(getJspViewResolver());
        resolver.setViewResolvers(resolvers);

        List<View> defaultViews = new ArrayList<View>();
        defaultViews.add(getMappingJacksonView());
        resolver.setDefaultViews(defaultViews);

    //    resolver.setContentNegotiationManager(contentNegotiationManager);
      //  resolver.afterPropertiesSet();
        resolver.setOrder(1);

        return resolver;
    }
    
    @Override
    public void configureDefaultServletHandling(DefaultServletHandlerConfigurer configurer) {
        configurer.enable();
    }
    @Bean(name = "ThreadPoolTaskScheduler")
    public TaskScheduler getTaskScheduler() {
        return new ThreadPoolTaskScheduler();
    }
  

    @Bean
    public MessageSource messageSource() {
        ResourceBundleMessageSource messageSource = new ResourceBundleMessageSource();
        messageSource.setBasename("messages");
        return messageSource;
    }

    @Bean(name = "localeResolver")
    public LocaleResolver localeResolver() {
        SessionLocaleResolver resolver = new SessionLocaleResolver();
        resolver.setDefaultLocale(new Locale("en"));
        return resolver;
    }

    @Bean(name = "localeChangeInterceptor")
    public LocaleChangeInterceptor localeChangeInterceptor() {
        LocaleChangeInterceptor localeChangeInterceptor = new LocaleChangeInterceptor();
        localeChangeInterceptor.setParamName("language");
       return localeChangeInterceptor;
    }
    
    @Bean

    public MultipartConfigElement multipartConfigElement() {

         MultipartConfigFactory factory = new MultipartConfigFactory();
      
         factory.setMaxFileSize(DataSize.of(1024, DataUnit.MEGABYTES));
         factory.setMaxRequestSize(DataSize.of(1024, DataUnit.MEGABYTES));
         return factory.createMultipartConfig();

    }
    
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("*").allowedOrigins("*");
        
    }

//    @Bean
//    public SessionInterceptor sessionInterceptor() {
//        return new SessionInterceptor();
//    }
   

//    @Override
//    public void addInterceptors(InterceptorRegistry registry) {
//        registry.addInterceptor(sessionInterceptor()).addPathPatterns("/**");
//    }
}