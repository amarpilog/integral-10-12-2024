package com.pilog.mdm;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.AnnotationConfigServletWebServerApplicationContext;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

public class ServletInitializer extends SpringBootServletInitializer {

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		return application.sources(SpringJbossApplication.class);
	}

	
	  
	  @Override
	  public void onStartup(ServletContext servletContext) throws ServletException {
	      super.onStartup(servletContext);
	      Object obj = servletContext.getAttribute(WebApplicationContext.ROOT_WEB_APPLICATION_CONTEXT_ATTRIBUTE);
	      servletContext.setInitParameter("OPENPAGES", "SaveClientRecord,getGSTINDetails,getLatLongDetails,"
					+ "DupValCheckClientRecord,SaveClientRequestBody,DupValCheckRequestBody,SaveClientRecordSample,DupValCheckRequestSample,AESLogin,"
					+ "getTranslatedWord,savereqCookie,getSAPTablesDataToDB,getApiRequestResultsData,appUserLogin,appUserlogout,appUsertimeout,"
					+ "updateInsertApiRequestData,updateInsertDeleteApiRequestData,loginBypass,homePage,loginhandler,loginhandlerOpt,loginerror,logout,"
					+ "timeout,login,loginpage,loginLanguage,getIGFeaturesInfo,getIGinfo,homePage,/,cloudLogin,cloudLogout,getProcessLoginAuth"
	          );
	      if(obj instanceof AnnotationConfigServletWebServerApplicationContext) {
	          AnnotationConfigServletWebServerApplicationContext context = (AnnotationConfigServletWebServerApplicationContext) obj;
	          context.register(MvcConfiguration.class);
	              
	          ServletRegistration.Dynamic restDispatcher = servletContext.addServlet("rest-dispatcher", new DispatcherServlet(context));
	          restDispatcher.setLoadOnStartup(1);
	          restDispatcher.addMapping("/");

	      }
	  }
	  
	  
	 

//	private String TMP_FOLDER = "/tmp";
//
//    private int MAX_UPLOAD_SIZE = 10000 * 100000;
//	 @Override
//	    public void onStartup(ServletContext servletContext) throws ServletException {
//	        AnnotationConfigWebApplicationContext appContext = new AnnotationConfigWebApplicationContext();
//	        appContext.register(SpringJbossApplication.class);
//	        servletContext.setInitParameter("OPENPAGES", "homePage,menu,getSideSubMenu,httpError,timeout,"
//	                + "cloudTabs,getTabDataByCompId,cloudGrid,cloudGridResults,getBaseTableMenu,cloudForm,cloudLogin,cloudAnalytics,"
//	                + "cloudAnalyticDropdown,cloudJqwidgetAnalyticCharts,cloudAnalyticsCharts,cloudRegistration,cloudRegistrationForm,"
//	                + "getFilterGridForm,getTabDataByGridId,register,userResetPassword,userProfile,updateProfileData,changepassword"
//	                + "deleteCartApi,myCart,addToCart,cartCount,getCartItems,forgotPassword,forgotPass,feedbackForm,searchSuggestion,"
//	                + "dataDXPAnalytics,getVisualizationDataSources,showfirstSplitterMenu,showSecondSplitterMenu,"
//	                + "showTaxonomyRepository,showTaxonomyRepositorySubData,GenericSearch,searchTextResults,activateUser,"
//	                + "showSearchDxpResults,showSearchDxpClassResults,cloudGridResults,formData,itemRegisterForm,registerValidation"
//	                + "registration,showFilterpopoverDxpResults,advancedSearches,getParamSearchForm,getPersonalizationData"
//	                + "genericSearchGrid,genericSearchGridResults,fetchRowDetails,classificationSuggestions,getDescClassification,"
//	                + "classificationSearch,getClassificationSearchresults,descriptorSuggestions,descriptorProperties,"
//	                + "getCategoryData,getDiscplineTypes,getClassByDiscipline,unspscCategories,getServiceTerms,"
//	                + "categorySearchSuggestions,emailOtpVerification,checkOtpVal,vendorOnboardingForm,getStateList,"
//	                + "getCityList,registerVendor,emailVerification,accountActivation,activateUserThroughLink,getHomepageShowcaseCards,"
//	                + "getShowCasecardsFilterData,getEachCardFilterResults,getVideoComments,getScoreCardSortResults,userNameValidate");
//	        ContextLoaderListener contextLoaderListener = new ContextLoaderListener(appContext);
//	        servletContext.addListener(contextLoaderListener);
//	        HttpSessionListenerPublisher httpSessionListenerPublisher = new HttpSessionListenerPublisher();
//	        servletContext.addListener(httpSessionListenerPublisher);
////	      appContext.register(CustomHandlerExceptionResolver.class);
//	        //   DispatcherServlet dispatcherServlet = new DispatcherServlet(appContext);
//	        // dispatcherServlet.setThrowExceptionIfNoHandlerFound(true);
//	        ServletRegistration.Dynamic dispatcher = servletContext.addServlet("SpringDispatcher", new DispatcherServlet(appContext));
//	        dispatcher.setLoadOnStartup(1);
//	        dispatcher.addMapping("/");
//
//	        dispatcher.setInitParameter("throwExceptionIfNoHandlerFound", "true");
//
//	        MultipartConfigElement multipartConfigElement = new MultipartConfigElement(TMP_FOLDER,
//	                MAX_UPLOAD_SIZE, MAX_UPLOAD_SIZE * 2, MAX_UPLOAD_SIZE / 2);
//
//	        dispatcher.setMultipartConfig(multipartConfigElement);
//	    }

}
