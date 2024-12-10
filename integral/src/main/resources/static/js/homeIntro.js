/**
 * This File Contains Home Intro Tour for the new User.
 
   Created on 30-11-2022
 */
 
 //This function is to trigger intro guide on Login page
function loginPageGuide(){
var intro1 = introJs();
	intro1.setOptions({
	   steps:[
		   {
			   title:'Integral',
			   intro:'Hello,Welcome to Integral'
		   },
		   {
			   title:'Login Page',
			   element:'#mainWrapper',
			   intro:'This is Login Form',
			   position:'left'
		   },
		   {
			   title:'Username',
			   element:'#rsUsername',
			   intro:'You can type your usernam here',
			   position:'top'
		   },	
	       {
			   title:'Next',
			   element:'#showPassword',
			   intro:'click on Next button to get to the password feild'
			   
		   },
		   
	   ],
	  nextLabel: 'Next',
	   prevLabel: 'Back',
	    tooltipClass:'loginTooltip',
	  
   });
    intro1.start().oncomplete(function(){
	   intro1.exit();
   });
}
// Ends here



// This function is to trigger Intro on the home page
 function homePageGuide(){
	   const intro2 = introJs();
   intro2.setOptions({
	   steps:[
		   {
			   title:'<img src="images/Homepage-Main-Image.jpg" width="300px"/><span class="imageText">You have to be responsible for the energy you’re putting out into the world.</span>',
			   intro:'<div><h6> Welcome to <b>Integral</b></h6></div><br> <span class="homeText">Sample lorem text for checking if you can add your custom classses in the intro key or variable</span>',
		   },
		   {
			 title:'<img src="images/homepage-introjs.svg" width="300px"/><span class="mainCardsText">“Extraordinary things are always hiding in places people never think to look.”</span>',
			 element:'#homeSocialTrends',
			 intro:'<div><h6>Home</h6></div><br><span class="homeText>Here you can find all the latest news and updates</span>' , 
		   },
		   {
			   title:'<img src="images/calender-introJS.svg" width="300px"/><span class="calenderText">“Setting goals is the first step in turning the invisible into the visible.”</span>',
			   element:'.calendarIcon',
			   intro:'Description',
			   position:'left'
		   },
		   {
			   title:'<img src="images/settings-introjs.svg" width="300px"/><span class="settingIconText">“You can have it all. Just not all at once.”</span>',
			   element:'.settingIcon',
			   intro:'click here for more settings',
			   position:'left'
		   },
		   {
			 title:'<img src="images/help-introjs.svg" width="300px"/><span class="helpIconText">"Say something positive, and you’ll see something positive.”</span>',
			 element:'.helpIcon',
			 intro:'Click for any issues' ,
			 position:'left'
			 
		   },
		   {
			 title:'<img src="images/profile-introjs.svg" width="300px"/><span class="profileIconText">Only in the darkness can you see the stars.</span>',
			 element:'.userProfileIcon', 
			 intro:'Check your profile here'  ,
			 position:'left'
		   },
		    {
			  title:'<img src="images/searchbar-introjs.svg" width="300px"/><span class="imageTextBottom">“A goal is not always meant to be reached, it often serves simply as something to aim at.”</span>',
			  element:'#toggleSearchFeildDiv',
			  intro:'Search the Data you looking for here.',
			  position:'left' 
		   },
		    {
			  title:'<img src="images/sidemenu-introjs.svg" width="300px"/><span class="leftThreeDots">“A No. 2 pencil and a dream can take you anywhere.”</span>',
			  element:'#show-sidebar',
			  intro:'Search the Data you looking for here.',
			  position:'left' ,
		   },
		   	{
			   title:'<img src="images/Sidebar-Home.svg" width="300px"/><span class="imageTextBottom">"Inspiration comes from within yourself. One has to be positive. When youre positive good things happen."</span>',
			   element:'.sidebar-content',
			   intro:'Here you can find all the data',
			   position:'right'
			   
		   },
		   {
			   title:'<img src="images/Data-Analytics-Homepage.svg" width="300px"/><span class="imageTextBottom">"Sometimes you will never know the value of a moment, until it becomes a memory."</span>',
			   element:'.IntroClass0',
			   intro:'You can customize your chats here ',
			   position:'right'
		   },
		   {
			   title:'<img src="images/Data-Integration-Homepage.svg" width="300px"/><span class="imageTextBottom">"Just for the record darling, not all positive change feels positive in the beginning."</span>',
			   element:'.IntroClass1',
			   intro:'Do you wish to add jobs?',
			   position:'right'
		   },
		   {
			   title :'<img src="images/Analytics-Feature.svg" width="300px"/><span class="imageTextBottom">"No act of kindness, no matter how small, is ever wasted."</span>',
			   element:'.IntroClass2',
			   intro:'Do you want to know Analytics Features?',
			   position:'right'
		   },
		   {
			   title:'<img src="images/Transform-Features.svg" width="300px"/><span class="imageTextBottom">"No matter what people tell you, words and ideas can change the world."</span>',
			   element:'.IntroClass3',
			   intro:'Do you want to know Transform Features?',
			   position:'right'
		   },
		   {
			   title:'<img src="images/Ai-Homepage.svg" width="300px"/><span class="leftThreeDots">"Whoever is happy will make others happy too."</span>',
			   element:'.IntroClass4',
			   intro:'Do you want to know AI Features?',
			   position:'right'
		   },
		   {
			   title:'<img src="images/DS-Knowlegde-Homepage.svg" width="300px"/><span class="imageTextBottom">"Memories of our lives, of our works and our deeds will continue in others."</span>',
			   element:'.IntroClass6',
			   intro:'Do you want to know AI Knowlegde Base?',
			   position:'right'
		   },
		   {
			   title:'<img src="images/pricing-home.svg" width="300px"/><span class="imageTextBottom">"All our dreams can come true, if we have the courage to pursue them."</span>',
			   element:'.IntroClass7',
			   intro:'Do you want Subscribe?',
			   position:'right'
		   },   		  
	   ],
	   nextLabel: 'Next',
	   prevLabel: 'Back',
	   tooltipClass:'customTooltip',
   });
//    intro2.onbeforechange(function () {
//      if (this._currentStep === 8) {
//        console.log('what is happening');
//       sideBarMenuHomeGuide()
//        return false;
//      }
//      });
   intro2.start().oncomplete(function(){
	   intro2.exit();
   });
 }
 //Ends here
 
 //This function is to trigger intro for Side bar menu in home page
//function sideBarMenuHomeGuide(){
//	const intro11 = introJs();
//	intro11.setOptions({
//		steps:[
//		
//		],
//		 nextLabel: 'Next',
//	     prevLabel: 'Back',
//		 tooltipClass:'sideBarTooltip',
//		
//	})
//	intro11.start().oncomplete(function(){
//		intro11.exit();
//	})

 
 //This function is to trigger intro on  Data Analytics Home page
function dxpAnalyticsGuideHome(){
    const intro3 = introJs();
    intro3.refresh();
    intro3.setOptions({
        steps:[
           {
               title:'<span class="toggleIconsDA">Minimizing and maximizing the side menus and adjusting the window content.</span>',
               element:'#show-sidebar',
               intro:'Minimize side menu ',
               
           },
           {
               title:'<span class="toggleIconsDA">Switch between different languages for the application interface</span>',
               element:'#DXPlLanguageSelectionId',
               intro:'Change Language',
               
           },
           {
               title:'<span class="toggleIconsDA">View the current weather conditions and forecasts for your location.</span>',
               element:'.weatherIcon',
               intro:'Weather report',
               
           },
           {
               title:'<span class="toggleIconsDA">View information about your subscription plan, including active services and renewal dates.</span>',
               element:'#limitIconDivID',
               intro:'Subscription details',
               
           },
           {
               title:'<span class="toggleIconsDA">Toggle the visibility of the introductory section</span>',
               element:'#toggleintroID',
               intro:'Intro toggle',
               
           },
           {
               title:'<span class="toggleIconsDA">View and manage dates, events, and schedules in a calendar format.</span>',
               element:'.calendarIcon',
               intro:'Calendar',
               
           },
           {
               title:'<span class="toggleIconsDA">Access and modify preferences, configurations, and options for the application or system.</span>',
               element:'.settingIcon',
               intro:'Settings',
               
           },
           {
               title:'<span class="toggleIconsDA">Access support resources, guides, and assistance for using the application or system.</span>',
               element:'.helpIcon',
               intro:'Help',
               
           },
           {
               title:'<span class="toggleIconsDA">View and manage personal information, settings, and preferences associated with your account.</span>',
               element:'.userProfileIcon ',
               intro:'User profile',
               
           },
           {
               title:'<span class="toggleIconsDA">A vertical navigation panel that provides access to different sections or features of the application.</span>',
               element:'.sidebar-content',
               intro:'Side munu bar',
               
           },
           
           {
               title:'<span class="toggleIconsDA">Allows users to filter and visualize data by selecting specific ranges or categories.</span>',
               element:'#visionHomePageSlicer',
               intro:'Slicer',
               
           },
           {
               title:'<span class="toggleIconsDA">A tool or feature that allows users to narrow down and display specific data based on selected criteria or conditions.</span>',
               element:'#visionHomePageFilterId',
               intro:'Header Filter',
               
           },
           {
               title:'<span class="toggleIconsDA">Choose the dashboard you no longer need from the list of available dashboards and click the delete button on the Home page</span>',
               element:'#visionHomePageDeleteDashboard',
               intro:'Delete Dashboard',
               
           },
           {
               title:'<span class="toggleIconsDA">Naming convention can be changed for dashboard using edit option</span>',
               element:'#visionHomePageEditDashboard',
               intro:'Edit Dashboard',
               
           },
           {
               title:'<span class="toggleIconsDA">Share the current dashboard view or data with others via a email.</span>',
               element:'#visionHomePageShareDashboard',
               intro:'Share Dashboard',
               
           },
           {
               title:'<span class="toggleIconsDA">To send the current dashboard view or data via email to others.</span>',
               element:'#visionHomePageShareMailDashboard',
               intro:'Mail Dashboard',
               
           },
           {
               title:'<span class="toggleIconsDA">Download charts or graphs as image files or other formats for external use.</span>',
               element:'#visionHomePageDownloadChartsId',
               intro:'Download Charts',
               
           },
           {
               title:'<span class="toggleIconsDA">Click the Color Palette icon on the homepage dashboard to change the dashboard\'s color</span>',
               element:'#visionHomePageColorFilterId',
               intro:'Color Palette',
               
           },
           {
               title:'<span class="toggleIconsDA">To switch between different dashboard layouts or views to display various data or metrics.</span>',
               element:'#dropdownlistWrapperOptionDropdownData',
               intro:'Change dashboard',
               
           },
           {
               title:'<span class="toggleIconsDA">Reset buttons ignores edited features and takes back to previous mode.</span>',
               element:'#visionDashBoardResetDivId',
               intro:'Vision dashboard reset',
               
           },
        ],
      nextLabel: 'Next',
      prevLabel: 'Back',
      tooltipClass:'customTooltip',
        
    });
//	 intro3.onbeforechange(function(){
//		 if(this._currentStep === 2){
//			 console.log('ToggleDivSuccess')
//    		 toggleIntroToggleDA();
//			 return false;
//		 }
//	 });
    
   intro3.start().oncomplete(function(){
      intro3.exit();
      });
     
}
 //Ends here
 
// function toggleIntroToggleDA(){
//	 const intro12 = introJs();
//	 intro12.setOptions({
//		 steps:[
//			
//		 ],
//		   nextLabel: 'Next',
//	        prevLabel: 'Back',',
//		    tooltipClass:'toggleToolTips'
//	 });
//	 intro12.start().oncomplete(function(){
//		 intro12.exit();
//	 })
// }
 
 
 
 // This function is to trigger intro for the left Data Integration column on Data Analytics home page
 function dataIntegrationGuide(){
	 const intro4 = introJs();
	 intro4.setOptions({
		 steps:[
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 intro:'Add jobs here'
			 },
			
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'#visualConnectionLi',
				 intro:'click on the icon to add connections'
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'#treeDxpConnectionLi',
				 intro:'Click here to add new connection'
			 },		
             {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'#panelContentpanelVisualizationSources',
				 intro:'You can find, Files,Database,Online Services,ERP'
			 }

		 ],
		nextLabel: 'Next',
	     prevLabel: 'Back',
		 tooltipClass:'customTooltip',
	 });
	 intro4.start().oncomplete(function(){
		 intro4.exit();
	 });
 }
 //Ends here
 
 // This function is to trigger intro for the left Data Analytics column on Data Analytics home page
 function dataAnalyticGuide(){
	 const intro5 = introJs();
	  intro5.setOptions({
		  steps:[
			  {
				  title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				  element:'Here You can add charts',
			  },
			  {
				  title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				  element:'#Visualization',
				  intro:'You can customize your charts here',
				  position:'right'
			  }
		  ],
		 nextLabel: 'Next',
	      prevLabel: 'Back',
		  tooltipClass:'customTooltip',
	  });
	  
	   intro5.start().oncomplete(function(){
		 intro5.exit();
	 });
	  
 }
//Ends here


//This function is to trigger intro for Data Inegration home page
function dataIntegrationHomeGuide(){
	const intro6 = introJs();
	intro6.setOptions({
		steps:[
			{
				title:'<img src="images/Data-integration-Home-1.png" width="300px"/><span class="imageText">You have to be responsible for the energy you’re putting out into the world.</span>',
				intro:'Welcome to Data Integration',
			},
			{
				title:'<img src="images/Connections-Icons.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				element:'.showEtlIcons',
				intro:'Description',
			},
			{
				title:'<img src="images/new-connections.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				element:'#newConnections',
				intro:'Add new connections'
			},
			{
				title:'<img src="images/saved-connection.svg" width="300px"/><span class="toggleIconsDA">You have to be responsible for the energy you’re putting out into the world.</span>',
				element:'#availableConnections',
				intro:'Check for available connections'
			},
			{
				title:'<img src="images/Database-jobs.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				element:'#availableJobs',
				intro:'Check for available jobs'
			},
			{
				title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				element:'#schemaObjects',
				intro:'schema objects',
			},
			{
				title:'<img src="images/Files-DataBase-DI.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				element:'#connectionsTabs',
				intro:'You can find all the sources here',
			},
			{
				title:'<img src="images/Settings-icon-DI.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				element:'#feedListContainer',
				intro:'You can find all the settings related to ETL',
			},
		],
		nextLabel: 'Next',
	    prevLabel: 'Back',
		tooltipClass:'customTooltip',
	});
//	intro6.onbeforechange(function(){
//		if(this._currentStep === 1){
//			console.log("ETL icons")
//			iconsETL();
//			return false;
//		}
//	})
	intro6.start().oncomplete(function(){
		intro6.exit();
	});
}
//Ends here

//function iconsETL(){
//	const intro13 = introJs();
//	intro13.setOptions({
//		steps:[
//			//etl icons
//			
//			
//		],
//		nextLabel: '<img src="images/introjs-arrow-next.svg" width="20px"/>',
//	    prevLabel: 'Back',',
//		tooltipClass:'iconETLTooltip',
//	});
//	intro13.onbeforechange(function(){
//		if(this._currentStep === 1){
//			console.log("connection")
//			connectionsDI();
//			return false;
//		}
//	})
//	intro13.start().oncomplete(function(){
//		intro13.exit();
//	});
//}

//function connectionsDI(){
//	const intro14 = introJs();
//	intro14.setOptions({
//		steps:[
//			
//		],
//		nextLabel: '<img src="images/introjs-arrow-next.svg" width="20px"/>',
//	    prevLabel: 'Back',',
//		tooltipClass:'connectionDITooltip',
//	});
//	intro14.onbeforechange(function(){
//		if(this._currentStep === 4){
//			fileAndDataDI();
//			return false;
//		}
//	})
//	intro14.start().oncomplete(function(){
//		intro14.exit();
//	});
//}

//function fileAndDataDI(){
//	const intro15 = introJs();
//	intro15.setOptions({
//		steps:[
//			
//		],
//		nextLabel: '<img src="images/introjs-arrow-next.svg" width="20px"/>',
//	    prevLabel: 'Back',',
//		tooltipClass:'fileAndDataDITooltip',
//	});
//	intro15.onbeforechange(function(){
//		if(this._currentStep === 1){
//			settingDI();
//			return false;
//		}
//	})
//	
//	intro15.start().oncomplete(function(){
//		intro15.exit();
//	});
//}
//function settingDI(){
//	const intro16 = introJs();
//	intro16.setOptions({
//		steps:[
//			
//		],
//		nextLabel: '<img src="images/introjs-arrow-next.svg" width="20px"/>',
//	    prevLabel: 'Back',',
//		tooltipClass:' settingDITooltip',
//	});
//	
//	intro16.start().oncomplete(function(){
//		intro16.exit();
//	});
//}


//This function is to trigger intro for Transform Features home page
function transformFeaturesGuideHome(){
	const intro7 = introJs();
	intro7.setOptions({
		steps:[
			{
				title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				intro:'Welcome to Transform Features',
			},
			{
				title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				element:'#allFeaturesMenuListM',
				intro:'',
				position:'right'
			},
		],
		nextLabel: '<img src="images/introjs-arrow-next.svg" width="20px"/>',
	    prevLabel: 'Back',
		tooltipClass:'customTooltip',
	});
	intro7.start().oncomplete(function(){
		intro7.exit();
	})
}

function analyticsFeaturesGuideHome(){
	const intro8 = introJs();
	intro8.setOptions({
		steps:[
			{
				title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				intro:'Welcome to Analytics'
			},
			{
				title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				element:'#allFeaturesMenuListM',
				intro:'',
				position:'right'
			},
		],
		nextLabel: 'Next',
	     prevLabel: 'Back',
		tooltipClass:'customTooltip',
	});
	intro8.start().oncomplete(function(){
		intro8.exit();
});

}

//This function is to trigger intro for AI  home page
function aiFeatureGuideHome(){
	const intro9 = introJs();
	intro9.setOptions({
		steps:[
			{
				title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				intro:'Welcome to AI'
			},
			{
				title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				element:'#allFeaturesMenuListM',
				intro:'',
				position:'right'
			},
		],
		nextLabel: 'Next',
	     prevLabel: 'Back',
		tooltipClass:'customTooltip',
	});
	intro9.start().oncomplete(function(){
		intro9.exit();
});

}
//This function is to trigger intro for DS knowlegde home page
function dsKnowledgeHomeGuide(){
 const	intro10 = introJs();
    intro10.setOptions({
		steps:[
			{
				title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				intro:'Welcome to Ds'
			},
			{
				title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				element:'#allFeaturesMenuListM',
				intro:'',
				position:'right'
			},
		],
		nextLabel: 'Next',
	     prevLabel: 'Back',
		tooltipClass:'customTooltip',
	});
	intro10.start().oncomplete(function(){
		intro10.exit();
	});
}
 
 function  	searchToggleGuide(){
	 const intro18 = introJs();
	 intro18.setOptions({
		 steps:[
			 {
				 title:'<img src="images/search-left-side.svg" width="300px"/><span class="languageLeftSide">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'#SelectedValue',
				 intro:'select the feild you wanna search',
			 },
			 {
				 title:'<img src="images/search-bar.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'#SearchResult',
				 intro:'search here',
			 },
			 {
				 title:'<img src="images/settings-advance-search.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'#settingheaderImage',
				 intro:'advanced search',
			 },
			 {
				 title:'<img src="images/search-bar-language.svg" width="300px"/><span class="languageLeftSide">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.languageSelectionBox',
				 intro:'select the language you prefer',
			 },
		 ],
		 nextLabel: 'Next',
	     prevLabel: 'Back',
		tooltipClass:'customTooltip',
	});
	
	intro18.start().oncomplete(function(){
		intro18.exit();
	});
 }
function calenderGuide(){
	const intro19 =  introJs();
	intro19.setOptions({
		steps:[
			{
				title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
			    intro:'Calender',
			    
			},
			{
				title:'Calender',
				intro:'description',
				element:'#settingPannel'
			}
			
		],
		nextLabel: 'Next',
	     prevLabel: 'Back',
		tooltipClass:'customTooltip',
		
	})
	intro19.start();
}
 function settingsGuide(){
	 const intro20 =  introJs();
	 intro20.setOptions({
		 steps:[
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 intro:'Add settings here'
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.settingPannel',
				 intro:'You can find all the settings here',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.fontChangeIcon ',
				 intro:' You can change font her',
				 position:'left',
			 },
			  {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.fontSizeIcon ',
				 intro:'You can change font size here',
				 position:'left',
			 },
			  {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.themeChangeIcon',
				 intro:'You can change theme here',
				 position:'left',
			 },
			  {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.extendedViewIcon',
				 intro:'You can change the screen size to extended view',
				 position:'left',
			 },
			  {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.languageChangeIcon',
				 intro:'You can change language here',
				 position:'left',
			 },
			  {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.organizationIcon',
				 intro:'View you organizations here',
				 position:'left',
			 },
			  {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.feedbackIcon',
				 intro:'Give us a feedback!',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.ThemesIcon',
				 intro:'Change themes here',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.contactPreferencesIcon',
				 intro:'Choose your contact prefrences',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.passworIcon',
				 intro:'You can change your passwords here',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.aboutUsIcon',
				 intro:'About us',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.OtherIcon',
				 intro:'Others',
				 position:'left',
			 },
		 ],
		 nextLabel: 'Next',
	     prevLabel: 'Back',
		tooltipClass:'customTooltip',
		 
	 })
	 intro20.start();
 }
 
 function helpGuide(){
	 const intro21 = introJs();
	 intro21.setOptions({
		 steps:[
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 intro:'Help',
				 
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'#settingPannel',
				 intro:'Help',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'#chartIconID',
				 intro:'Chat with us?',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'#helpDocumentID',
				 intro:'Help with some document',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'#helpvideoID',
				 intro:'Hlep with video',
				 position:'left',
			 },
			 {
				 intro:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'#helpGifID',
				 intro:'Help with a gif',
				 position:'left',
			 }
		 ],
		nextLabel: 'Next',
	     prevLabel: 'Back',
		 tooltipClass:'customTooltip',
	 })
	 intro21.start();
 }
 
 function userProfileGuide(){
	 const intro22 = introJs();
	 intro22.setOptions({
		 steps:[
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 intro:'User Profile',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.shoppingIcon',
				 intro:'Your cart',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.subscriptionsClass',
				 intro:'Your subscription',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.TransactionClass',
				 intro:'Your transactions',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.walletClass',
				 intro:'Your wallet',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.workSpaceClass',
				 intro:'Your workspace',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.WorkspaceAnalyticsClass',
				 intro:'Your workspace analytics',
				 position:'left',
			 },
			 {
				 title:'<img src="images/.svg" width="300px"/><span class="imageTextBottom">You have to be responsible for the energy you’re putting out into the world.</span>',
				 element:'.logoutIcon ',
				 intro:'Logout',
				 position:'left',
			 },
		 ],
		 nextLabel: 'Next',
	     prevLabel: 'Back',
		 tooltipClass:'customTooltip',
		 
	 })
	 intro22.start();
 }
