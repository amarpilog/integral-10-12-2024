function showCustomScheduledCalendar() {


var calendar = document.getElementById("calendar-table");
var gridTable = document.getElementById("table-body");
var currentDate = new Date();
var selectedDate = currentDate;
var selectedDayBlock = null;
var globalEventObj = {};

var sidebar = document.getElementById("sidebar");

function createCalendar(date, side) {
   var currentDate = date;
   var startDate = new Date(currentDate.getFullYear(), currentDate.getMonth(), 1);

   var monthTitle = document.getElementById("month-name");
   var monthName = currentDate.toLocaleString("en-US", {
      month: "long"
   });
   var yearNum = currentDate.toLocaleString("en-US", {
      year: "numeric"
   });
   monthTitle.innerHTML = `${monthName} ${yearNum}`;

   if (side == "left") {
      gridTable.className = "animated fadeOutRight";
   } else {
      gridTable.className = "animated fadeOutLeft";
   }

   setTimeout(() => {
      gridTable.innerHTML = "";

      var newTr = document.createElement("div");
      
      newTr.className = "row";
      var currentTr = gridTable.appendChild(newTr);
      
     
      for (let i = 1; i < (startDate.getDay() || 7); i++) {
         let emptyDivCol = document.createElement("div");
         emptyDivCol.className = "col-1 empty-day";
         currentTr.appendChild(emptyDivCol);
         
      }

      var lastDay = new Date(currentDate.getFullYear(), currentDate.getMonth() + 1, 0);
      lastDay = lastDay.getDate();

      for (let i = 1; i <= lastDay; i++) {
         if (currentTr.children.length >= 7) {

            currentTr = gridTable.appendChild(addNewRow());
         }
         let currentDay = document.createElement("div"); 
         currentDay.className = "col-1";
         
        
         
         if (selectedDayBlock == null && i == currentDate.getDate() || selectedDate.toDateString() == new Date(currentDate.getFullYear(), currentDate.getMonth(), i).toDateString()) {
            selectedDate = new Date(currentDate.getFullYear(), currentDate.getMonth(), i);

            document.getElementById("eventDayName").innerHTML = selectedDate.toLocaleString("en-US", {
               month: "long",
               day: "numeric",
               year: "numeric"
            });

            selectedDayBlock = currentDay;
            setTimeout(() => {
               currentDay.classList.add("blue");
               currentDay.classList.add("lighten-3");
            }, 900);
         }
         currentDay.innerHTML = i;

         //show marks
         if (globalEventObj[new Date(currentDate.getFullYear(), currentDate.getMonth(), i).toDateString()]) {
            let eventMark = document.createElement("div");
            eventMark.className = "day-mark";
            currentDay.appendChild(eventMark);
         }

         currentTr.appendChild(currentDay);
      }

      for (let i = currentTr.getElementsByTagName("div").length; i < 7; i++) {
         let emptyDivCol = document.createElement("div");
         emptyDivCol.className = "col-1 empty-day";
         currentTr.appendChild(emptyDivCol);
      }

      if (side == "left") {
         gridTable.className = "animated fadeInLeft";
      } else {
         gridTable.className = "animated fadeInRight";
      }

      function addNewRow() {
         let node = document.createElement("div");
         node.className = "row";
         return node;
      }

   }, !side ? 0 : 270);
}

createCalendar(currentDate);

var todayDayName = document.getElementById("todayDayName");
// todayDayName.innerHTML = "Today is " + currentDate.toLocaleString("en-US", {
//    weekday: "long",
//    day: "numeric",
//    month: "short"
// });

var prevButton = document.getElementById("prev");
var nextButton = document.getElementById("next");

prevButton.onclick = function changeMonthPrev() {
   currentDate = new Date(currentDate.getFullYear(), currentDate.getMonth() - 1);
   createCalendar(currentDate, "left");
}
nextButton.onclick = function changeMonthNext() {
   currentDate = new Date(currentDate.getFullYear(), currentDate.getMonth() + 1);
   createCalendar(currentDate, "right");
}




var selectFullDate="";
gridTable.onclick = function (e) {

   if (!e.target.classList.contains("col-1") || e.target.classList.contains("empty-day")) {
      return;
   }

   if (selectedDayBlock) {
      if (selectedDayBlock.classList.contains("blue") && selectedDayBlock.classList.contains("lighten-3")) {
         selectedDayBlock.classList.remove("blue");
         selectedDayBlock.classList.remove("lighten-3");
      }
   }
   selectedDayBlock = e.target;
   selectedDayBlock.classList.add("blue");
   selectedDayBlock.classList.add("lighten-3");

   selectedDate = new Date(currentDate.getFullYear(), currentDate.getMonth(), parseInt(e.target.innerHTML));

    var selectDate=selectedDate.getDate();
  var selectMonth=selectedDate.getMonth()+1;
  var selectYear=selectedDate.getFullYear();
   selectFullDate=selectYear+'-'+selectMonth+'-'+selectDate; 
   showEvents(selectFullDate);

   document.getElementById("eventDayName").innerHTML = selectedDate.toLocaleString("en-US", {
      month: "long",
      day: "numeric",
      year: "numeric"
   });

}
$(document).ready(function(){   
    showEvents(selectFullDate);
    }); 
   

//var changeFormButton = document.getElementById("changeFormButton");
//var addForm = document.getElementById("addForm");
//changeFormButton.onclick = function (e) {
//   addForm.style.top = 0;
//}

var cancelAdd = document.getElementById("cancelAdd");
cancelAdd.onclick = function (e) {
   addForm.style.top = "100%";
   let inputs = addForm.getElementsByTagName("input");
   for (let i = 0; i < inputs.length; i++) {
      inputs[i].value = "";
   }
   let labels = addForm.getElementsByTagName("label");
   for (let i = 0; i < labels.length; i++) {
      labels[i].className = "";
   }
}

var addEventButton = document.getElementById("addEventButton");
addEventButton.onclick = function (e) {
   let title = document.getElementById("eventTitleInput").value.trim();
   let desc = document.getElementById("eventDescInput").value.trim();

   if (!title || !desc) {
      document.getElementById("eventTitleInput").value = "";
      document.getElementById("eventDescInput").value = "";
      let labels = addForm.getElementsByTagName("label");
      for (let i = 0; i < labels.length; i++) {
         labels[i].className = "";
      }
      return;
   }

   addEvent(title, desc);
   showEvents();

   if (!selectedDayBlock.querySelector(".day-mark")) {
      selectedDayBlock.appendChild(document.createElement("div")).className = "day-mark";
   }

   let inputs = addForm.getElementsByTagName("input");
   for (let i = 0; i < inputs.length; i++) {
      inputs[i].value = "";
   }
   let labels = addForm.getElementsByTagName("label");
   for (let i = 0; i < labels.length; i++) {
      labels[i].className = "";
   }
}
 
}

function showEvents(selectFullDate) { 
	   let eventDate = selectFullDate;
	let objWithDate ="";  
	$.ajax({
			type: "POST",
			traditional: true,
			dataType: 'json',
			url: 'getDateWiseCalenderEvents',  
			traditional: true,
			cache: false,
			data: {
				eventDate: eventDate
			},
	        success: function(response) {
		    stopLoader();
		    if(JSON.stringify(response) === JSON.stringify({})) {
			$("#sidebarEvents").html("<div class='empty-message'>Currently, no events to selected date</div>");
			}else {
			$("#sidebarEvents").html('');
			}
		    objWithDate = response;
		    
//   let sidebarEvents = document.getElementById("sidebarEvents");
//    objWithDate = globalEventObj[selectedDate.toDateString()];

//   sidebarEvents.innerHTML = "";

   if (objWithDate) {
      let eventsCount = 0;
      for (key in objWithDate) {
         let eventContainer = document.createElement("div");
         eventContainer.className = "eventCard";

         let eventHeader = document.createElement("div");
         eventHeader.className = "eventCard-header";

         let eventDescription = document.createElement("div");
         eventDescription.className = "eventCard-description";

        
         eventHeader.appendChild(document.createTextNode(key));
         eventContainer.appendChild(eventHeader);

         eventDescription.appendChild(document.createTextNode(objWithDate[key]));
         eventContainer.appendChild(eventDescription);

        

         let markWrapper = document.createElement("div");
         markWrapper.className = "eventCard-mark-wrapper";
         let mark = document.createElement("div");
         mark.classList = "eventCard-mark";
         markWrapper.appendChild(mark);
         eventContainer.appendChild(markWrapper);

         sidebarEvents.appendChild(eventContainer);

         eventsCount++;
      }
      let emptyFormMessage = document.getElementById("emptyFormTitle");
//      emptyFormMessage.innerHTML = `${eventsCount} events now`;
   } else {
      let emptyMessage = document.createElement("div");
      emptyMessage.className = "empty-message";
      emptyMessage.innerHTML = "Sorry, no events to selected date";
      sidebarEvents.appendChild(emptyMessage);
      let emptyFormMessage = document.getElementById("emptyFormTitle");
      emptyFormMessage.innerHTML = "No events now";
   }
   }
	});
}  
function addNewDateWiseEvent(){
	var result = '<div class="addForm" id="addForm">'
		+ '<div class="titleLabelSpanClass">'
		+ '<span>'
		+ '<label for="eventTitleInput" class="mainTitle">Title</label>'
		+ '</span>'
		+ '<span>'
		+ '<input id="eventTitleInputD" type="text" class="validate">'
		+ '</span>'
		+ '</div>'
		+ '<div class="timeLabelSpanClass">'
		+ '<span>'
		+ '<label for="eventDescInput">Time</label>'
		+ '</span>'
		+ '<span>'
		+ '<input id="eventDescInputT" type="time" class="validate">'
		+ '</span>'
		+ '</div>'
		+ '</div>';
	var modalObj = {
		title: labelObject['Message'] != null ? labelObject['Message'] : 'New Event', 
		body: result
	};
	var buttonArray = [
		{
			text: 'Add',
			click: function() {
				var title = $("#eventTitleInputD").val();
				var time = $("#eventDescInputT").val();
				if(time == null || time == undefined || time == ''){
					time = "7:48";
				}  
				addEvent(title, time);
				
			},
			isCloseButton: true
		} ,
		{
			text: 'Cancel',
			click: function() {
			},
			isCloseButton: true
		}   
	];
	modalObj['buttons'] = buttonArray;
	createModal("dataDxpSplitterValueNew", modalObj);
	$(".modal-dialog").addClass("modal-xs");
	$(".modal-dialog").addClass("newCalendarEventClass");
}
function addEvent(title, desc) {
   var dataObj = {};
   dataObj['EVENT_TITLE']=title;
   dataObj['EVENT_DESC']=desc;
   $.ajax({
			type: "POST",
			traditional: true,
			dataType: 'json',
			url: 'insertCalenderEvents',
			traditional: true,
			cache: false,
			data: {
				dataStr: JSON.stringify(dataObj)
			},
	        success: function(response) {
		    stopLoader();
		    var modalObj = {
		title: labelObject['Message'] != null ? labelObject['Message'] : 'Calender Message',
		body: response['result']
	};
	var buttonArray = [
		{
			text: 'Ok',
			click: function() { 
//				showEvents(desc); 
			},
			isCloseButton: true
		}   
	];
	modalObj['buttons'] = buttonArray;
	createModal("dataDxpSplitterValue", modalObj);
	$(".modal-dialog").addClass("modal-xs");
	
				
			}
		});
}