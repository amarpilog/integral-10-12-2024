package com.pilog.mdm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter;
import org.springframework.security.web.header.writers.StaticHeadersWriter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@EnableWebSecurity
public class SecurityConfig extends WebSecurityConfigurerAdapter {

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.csrf().disable();
        http.cors().disable();
        http.headers().contentTypeOptions();
//        http
//                .cors().and()
//                .authorizeRequests()
//                .antMatchers("/cdn**","/nominatim**","/integraldataanalytics**","/test**","/gstatic**","/youtube**").permitAll().and()
//                .headers()
//                .httpStrictTransportSecurity().and()
//                .referrerPolicy(ReferrerPolicyHeaderWriter.ReferrerPolicy.SAME_ORIGIN).and()
//                .cacheControl().and()
//                .xssProtection().and()
//                .contentSecurityPolicy(
//                        "default-src 'self'; " +
//                                "style-src 'self' http://integraldataanalytics.com https://integraldataanalytics.com https://cdnjs.cloudflare.com https://fonts.googleapis.com; " +
//                                "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdnjs.cloudflare.com https://d3js.org/ https://www.gstatic.com/ https://polyfill.io/ http://integraldataanalytics.com https://integraldataanalytics.com https://cdnjs.cloudflare.com https://fonts.googleapis.com https://www.piloggroup.com/ https://test.ccavenue.com/ https://www.google.com/; " +
//                                "img-src 'self' data: https:; " +
//                                "frame-src 'self' https://www.youtube.com; " +
//                                "font-src 'self' https://fonts.gstatic.com; " +
//                                "connect-src 'self' https://maps.googleapis.com;").and()
//                .frameOptions().disable()
//                .addHeaderWriter(new StaticHeadersWriter("X-FRAME-OPTIONS",
//                        "ALLOW-FROM http://integraldataanalytics.com https://integraldataanalytics.com https://cdnjs.cloudflare.com https://fonts.googleapis.com https://www.gstatic.com/ https://fonts.googleapis.com https://www.piloggroup.com https://test.ccavenue.com https://www.google.com https://cdnjs.cloudflare.com https://www.gstatic.com https://maps.googleapis.com https://www.youtube.com"))
//                .xssProtection();

        http
        .cors()
            .and()
        .authorizeRequests()
            .antMatchers("/cdn**","/nominatim**","/integraldataanalytics**","/test**","/gstatic**","/youtube**").permitAll();
        http.headers().httpStrictTransportSecurity();
        http.headers().referrerPolicy(ReferrerPolicyHeaderWriter.ReferrerPolicy.SAME_ORIGIN);
       // http.headers().frameOptions().sameOrigin();
        http.headers().cacheControl();
        http.headers().xssProtection();
        http.headers()
                .contentSecurityPolicy("default-src 'self'; " +
                        "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdnjs.cloudflare.com https://www.gstatic.com https://polyfill.io https://js.arcgis.com https://www.google.com; " +
                        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; " +
                        "img-src 'self' data: https: blob:; " +
                        "frame-src 'self' https://www.youtube.com https://www.gartner.com https://test.ccavenue.com; " +
                        "child-src blob:; " +
                        "worker-src blob:; " +
                        "connect-src 'self' https://maps.googleapis.com https://nominatim.openstreetmap.org https://fastly.jsdelivr.net; " +
                        "font-src 'self' https://fonts.gstatic.com https://fonts.googleapis.com; ")
                .reportOnly()
                .and()
                .frameOptions().disable()
                .and()
                .headers()
                .contentSecurityPolicy("frame-ancestors 'self' https://integraldataanalytics.com https://fastly.jsdelivr.net https://cdnjs.cloudflare.com https://nominatim.openstreetmap.org https://maps.googleapis.com https://www.gartner.com https://www.piloggroup.com https://www.google.com https://www.youtube.com/;")
                .and()
                .frameOptions().disable().addHeaderWriter((new StaticHeadersWriter("X-FRAME-OPTIONS", "ALLOW-FROM http://integraldataanalytics.com https://integraldataanalytics.com https://fastly.jsdelivr.net https://cdnjs.cloudflare.com https://nominatim.openstreetmap.org https://maps.googleapis.com https://www.gartner.com/ https://d3js.org/ https://www.gstatic.com/ https://polyfill.io/ https://www.gartner.com https://www.piloggroup.com/ https://test.ccavenue.com/ https://www.google.com/ http://fonts.googleapis.com/ https://fonts.googleapis.com/css http://fonts.gstatic.com/ https://js.arcgis.com/ https://www.youtube.com/;"))).xssProtection();


//        http.headers().contentSecurityPolicy("default-src 'self' https: 'unsafe-inline' 'unsafe-eval';" +
//                " frame-src 'self' https://www.youtube.com/; " +  // Allow YouTube framing
//                " script-src 'self' 'unsafe-inline' 'unsafe-eval' https://www.gartner.com/ https://cdnjs.cloudflare.com/ https://d3js.org/ https://www.gstatic.com/ https://polyfill.io/ https://www.piloggroup.com/ https://test.ccavenue.com/ https://www.google.com/ http://fonts.googleapis.com/ http://fonts.googleapis.com/css http://fonts.gstatic.com/ https://js.arcgis.com/");

//        http.headers().contentSecurityPolicy("default-src 'self' style-src 'self' frame-src 'self' http://integraldataanalytics.com https://integraldataanalytics.com https://cdnjs.cloudflare.com https://nominatim.openstreetmap.org https://fastly.jsdelivr.net https://maps.googleapis.com http://fonts.googleapis.com/ https://fonts.googleapis.com/ https://www.youtube.com/").reportOnly();
//
//        http.headers().contentSecurityPolicy("default-src 'self' script-src style-src 'unsafe-inline' 'unsafe-eval' img-src 'self' frame-src 'self' https://www.youtube.com/ data: https: blob:;" +
//                        "child-src blob:;"
//        		+ "worker-src blob:; script-src 'self' 'unsafe-inline' 'unsafe-eval' frame-src 'self' https://www.youtube.com/ https://www.gartner.com/ https://cdnjs.cloudflare.com/ https://d3js.org/ https://www.gstatic.com/ https://polyfill.io/ https://www.gartner.com/ http://integraldataanalytics.com https://integraldataanalytics.com https://fastly.jsdelivr.net https://cdnjs.cloudflare.com https://nominatim.openstreetmap.org https://maps.googleapis.com https://www.piloggroup.com https://test.ccavenue.com/ https://www.google.com/ http://fonts.googleapis.com/ https://fonts.googleapis.com/css http://fonts.gstatic.com/ https://js.arcgis.com/ https://fonts.googleapis.com/ https://www.youtube.com/").
//        and().frameOptions().disable().addHeaderWriter((new StaticHeadersWriter("X-FRAME-OPTIONS",
//              "ALLOW-FROM http://integraldataanalytics.com https://integraldataanalytics.com https://fastly.jsdelivr.net https://cdnjs.cloudflare.com https://nominatim.openstreetmap.org https://maps.googleapis.com https://www.gartner.com/ https://d3js.org/ https://www.gstatic.com/ https://polyfill.io/ https://www.gartner.com https://www.piloggroup.com/ https://test.ccavenue.com/ https://www.google.com/ http://fonts.googleapis.com/ https://fonts.googleapis.com/css http://fonts.gstatic.com/ https://js.arcgis.com/ https://www.youtube.com/"))).xssProtection();
    }
    
    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        List allowedOrigins = new ArrayList();
        allowedOrigins.add("http://integraldataanalytics.com");
        allowedOrigins.add("https://integraldataanalytics.com");
        allowedOrigins.add("https://cdnjs.cloudflare.com");
        allowedOrigins.add("https://nominatim.openstreetmap.org");
        allowedOrigins.add("https://fastly.jsdelivr.net");
        allowedOrigins.add("https://maps.googleapis.com");
        allowedOrigins.add("https://test.ccavenue.com/");
        allowedOrigins.add("https://www.gstatic.com/");
        allowedOrigins.add("https://www.youtube.com/");

        
        //configuration.setAllowedOrigins(allowedOrigins);
        configuration.setAllowedOrigins(Arrays.asList("*"));
		/*
		 * configuration.addAllowedOrigin("https://integraldataanalytics.com");
		 * configuration.addAllowedOrigin("https://cdnjs.cloudflare.com");
		 * configuration.addAllowedOrigin("https://nominatim.openstreetmap.org");
		 * configuration.addAllowedOrigin("https://fastly.jsdelivr.net");
		 * configuration.addAllowedOrigin("https://maps.googleapis.com");
		 */
        
        configuration.addAllowedMethod("*"); 
        configuration.addAllowedHeader("*"); 

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);

        return source;
    }
    
//    
//    public void addCorsMappings(CorsRegistry registry) {
//		registry.addMapping("/**")
//			.allowedOrigins("https://integraldataanalytics.com")
//			.allowedMethods("*")
//			.allowedHeaders("*")
//			.exposedHeaders("*")
//			.allowCredentials(false).maxAge(3600);
//	}
    
//    @Bean
//    public WebMvcConfigurer corsConfigurer() {
//        return new WebMvcConfigurer() {
//            @Override
//            public void addCorsMappings(CorsRegistry registry) {
//                registry.addMapping("/**")
//                        .allowedOrigins("https://integraldataanalytics.com")
//                        .allowedMethods("GET", "POST", "PUT", "DELETE");
//            }
//        };
//    }

//    @Bean
//    public CorsFilter corsFilter() {
//        CorsConfiguration corsConfiguration = new CorsConfiguration();
//        corsConfiguration.setAllowCredentials(true);
//        corsConfiguration.setAllowedOrigins(Arrays.asList("*"));
//        corsConfiguration.setAllowedHeaders(Arrays.asList("Origin", "Access-Control-Allow-Origin",
//                "Content-Type", "Accept", "Authorization", "Origin,Accept", "X-Requested-With",
//                "Access-Control-Request-Method", "Access-Control-Request-Headers"));
//        corsConfiguration.setExposedHeaders(Arrays.asList("Origin", "Content-Type", "Accept", "Authorization",
//                "Access-Control-Allow-Origin", "Access-Control-Allow-Origin", "Access-Control-Allow-Credentials"));
//        corsConfiguration.setAllowedMethods(Arrays.asList("GET", "PUT", "POST", "DELETE", "OPTIONS"));
//        UrlBasedCorsConfigurationSource urlBasedCorsConfigurationSource = new UrlBasedCorsConfigurationSource();
//        urlBasedCorsConfigurationSource.registerCorsConfiguration("/**", corsConfiguration);
//        return new CorsFilter(urlBasedCorsConfigurationSource);
//    }
}

