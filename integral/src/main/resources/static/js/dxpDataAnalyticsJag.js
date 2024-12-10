var HtmlEntities = {
	" ": "&nbsp;"
};

var unHtmlEntities = {
	"&nbsp;": " "
};
var dxpHtmlEntities = {
	"\"": "&dquo;",
	" ": "&nbsp;",
	"'": "&aqos;",
	"<": "&lt;",
	">": "&gt;",
	"¡": "&iexcl;",
	"¢": "&cent;",
	"£": "&pound;",
	"¤": "&curren;",
	"¥": "&yen;",
	"¦": "&brvbar;",
	"§": "&sect;",
	"¨": "&uml;",
	"©": "&copy;",
	"ª": "&ordf;",
	"«": "&laquo;",
	"¬": "&not;",
	"®": "&reg;",
	"¯": "&macr;",
	"°": "&deg;",
	"±": "&plusmn;",
	"²": "&sup2;",
	"³": "&sup3;",
	"´": "&acute;",
	"µ": "&micro;",
	"¶": "&para;",
	"·": "&middot;",
	"¸": "&cedil;",
	"¹": "&sup1;",
	"º": "&ordm;",
	"»": "&raquo;",
	"¼": "&frac14;",
	"½": "&frac12;",
	"¾": "&frac34;",
	"¿": "&iquest;",
	"À": "&Agrave;",
	"Á": "&Aacute;",
	"Â": "&Acirc;",
	"Ã": "&Atilde;",
	"Ä": "&Auml;",
	"Å": "&Aring;",
	"Æ": "&AElig;",
	"Ç": "&Ccedil;",
	"È": "&Egrave;",
	"É": "&Eacute;",
	"Ê": "&Ecirc;",
	"Ë": "&Euml;",
	"Ì": "&Igrave;",
	"Í": "&Iacute;",
	"Î": "&Icirc;",
	"Ï": "&Iuml;",
	"Ð": "&ETH;",
	"Ñ": "&Ntilde;",
	"Ò": "&Ograve;",
	"Ó": "&Oacute;",
	"Ô": "&Ocirc;",
	"Õ": "&Otilde;",
	"Ö": "&Ouml;",
	"×": "&times;",
	"Ø": "&Oslash;",
	"Ù": "&Ugrave;",
	"Ú": "&Uacute;",
	"Û": "&Ucirc;",
	"Ü": "&Uuml;",
	"Ý": "&Yacute;",
	"Þ": "&THORN;",
	"ß": "&szlig;",
	"à": "&agrave;",
	"á": "&aacute;",
	"â": "&acirc;",
	"ã": "&atilde;",
	"ä": "&auml;",
	"å": "&aring;",
	"æ": "&aelig;",
	"ç": "&ccedil;",
	"è": "&egrave;",
	"é": "&eacute;",
	"ê": "&ecirc;",
	"ë": "&euml;",
	"ì": "&igrave;",
	"í": "&iacute;",
	"î": "&icirc;",
	"ï": "&iuml;",
	"ð": "&eth;",
	"ñ": "&ntilde;",
	"ò": "&ograve;",
	"ó": "&oacute;",
	"ô": "&ocirc;",
	"õ": "&otilde;",
	"ö": "&ouml;",
	"÷": "&divide;",
	"ø": "&oslash;",
	"ù": "&ugrave;",
	"ú": "&uacute;",
	"û": "&ucirc;",
	"ü": "&uuml;",
	"ý": "&yacute;",
	"þ": "&thorn;",
	"ÿ": "&yuml;",
	"Œ": "&OElig;",
	"œ": "&oelig;",
	"Š": "&Scaron;",
	"š": "&scaron;",
	"Ÿ": "&Yuml;",
	"ƒ": "&fnof;",
	"ˆ": "&circ;",
	"˜": "&tilde;",
	"Α": "&Alpha;",
	"Β": "&Beta;",
	"Γ": "&Gamma;",
	"Δ": "&Delta;",
	"Ε": "&Epsilon;",
	"Ζ": "&Zeta;",
	"Η": "&Eta;",
	"Θ": "&Theta;",
	"Ι": "&Iota;",
	"Κ": "&Kappa;",
	"Λ": "&Lambda;",
	"Μ": "&Mu;",
	"Ν": "&Nu;",
	"Ξ": "&Xi;",
	"Ο": "&Omicron;",
	"Π": "&Pi;",
	"Ρ": "&Rho;",
	"Σ": "&Sigma;",
	"Τ": "&Tau;",
	"Υ": "&Upsilon;",
	"Φ": "&Phi;",
	"Χ": "&Chi;",
	"Ψ": "&Psi;",
	"Ω": "&Omega;",
	"α": "&alpha;",
	"β": "&beta;",
	"γ": "&gamma;",
	"δ": "&delta;",
	"ε": "&epsilon;",
	"ζ": "&zeta;",
	"η": "&eta;",
	"θ": "&theta;",
	"ι": "&iota;",
	"κ": "&kappa;",
	"λ": "&lambda;",
	"μ": "&mu;",
	"ν": "&nu;",
	"ξ": "&xi;",
	"ο": "&omicron;",
	"π": "&pi;",
	"ρ": "&rho;",
	"ς": "&sigmaf;",
	"σ": "&sigma;",
	"τ": "&tau;",
	"υ": "&upsilon;",
	"φ": "&phi;",
	"χ": "&chi;",
	"ψ": "&psi;",
	"ω": "&omega;",
	"ϑ": "&thetasym;",
	"ϒ": "&Upsih;",
	"ϖ": "&piv;",
	"–": "&ndash;",
	"—": "&mdash;",
	"‘": "&lsquo;",
	"’": "&rsquo;",
	"‚": "&sbquo;",
	"“": "&ldquo;",
	"”": "&rdquo;",
	"„": "&bdquo;",
	"†": "&dagger;",
	"‡": "&Dagger;",
	"•": "&bull;",
	"…": "&hellip;",
	"‰": "&permil;",
	"′": "&prime;",
	"″": "&Prime;",
	"‹": "&lsaquo;",
	"›": "&rsaquo;",
	"‾": "&oline;",
	"⁄": "&frasl;",
	"€": "&euro;",
	"ℑ": "&image;",
	"℘": "&weierp;",
	"ℜ": "&real;",
	"™": "&trade;",
	"ℵ": "&alefsym;",
	"←": "&larr;",
	"↑": "&uarr;",
	"→": "&rarr;",
	"↓": "&darr;",
	"↔": "&harr;",
	"↵": "&crarr;",
	"⇐": "&lArr;",
	"⇑": "&UArr;",
	"⇒": "&rArr;",
	"⇓": "&dArr;",
	"⇔": "&hArr;",
	"∀": "&forall;",
	"∂": "&part;",
	"∃": "&exist;",
	"∅": "&empty;",
	"∇": "&nabla;",
	"∈": "&isin;",
	"∉": "&notin;",
	"∋": "&ni;",
	"∏": "&prod;",
	"∑": "&sum;",
	"−": "&minus;",
	"∗": "&lowast;",
	"√": "&radic;",
	"∝": "&prop;",
	"∞": "&infin;",
	"∠": "&ang;",
	"∧": "&and;",
	"∨": "&or;",
	"∩": "&cap;",
	"∪": "&cup;",
	"∫": "&int;",
	"∴": "&there4;",
	"∼": "&sim;",
	"≅": "&cong;",
	"≈": "&asymp;",
	"≠": "&ne;",
	"≡": "&equiv;",
	"≤": "&le;",
	"≥": "&ge;",
	"⊂": "&sub;",
	"⊃": "&sup;",
	"⊄": "&nsub;",
	"⊆": "&sube;",
	"⊇": "&supe;",
	"⊕": "&oplus;",
	"⊗": "&otimes;",
	"⊥": "&perp;",
	"⋅": "&sdot;",
	"⌈": "&lceil;",
	"⌉": "&rceil;",
	"⌊": "&lfloor;",
	"⌋": "&rfloor;",
	"⟨": "&lang;",
	"⟩": "&rang;",
	"◊": "&loz;",
	"♠": "&spades;",
	"♣": "&clubs;",
	"♥": "&hearts;",
	"♦": "&diams;"
};

var unHtmlEntities = {
	"&nbsp;": " "
};
var dxpUnHtmlEntities = {
	"&nbsp;": " ",
	"&dquo;": "\"",
	"&aqos;": "'", "&lt;": "<", "&gt;": ">", "&nbsp;": " ", "&iexcl;": "¡", "&cent;": "¢", "&pound;": "£", "&curren;": "¤", "&yen;": "¥", "&brvbar;": "¦", "&sect;": "§", "&uml;": "¨", "&copy;": "©", "&ordf;": "ª", "&laquo;": "«", "&not;": "¬", "&reg;": "®", "&macr;": "¯", "&deg;": "°", "&plusmn;": "±", "&sup2;": "²", "&sup3;": "³", "&acute;": "´", "&micro;": "µ", "&para;": "¶", "&middot;": "·", "&cedil;": "¸", "&sup1;": "¹", "&ordm;": "º", "&raquo;": "»", "&frac14;": "¼", "&frac12;": "½", "&frac34;": "¾", "&iquest;": "¿", "&Agrave;": "À", "&Aacute;": "Á", "&Acirc;": "Â", "&Atilde;": "Ã", "&Auml;": "Ä", "&Aring;": "Å", "&AElig;": "Æ", "&Ccedil;": "Ç", "&Egrave;": "È", "&Eacute;": "É", "&Ecirc;": "Ê", "&Euml;": "Ë", "&Igrave;": "Ì", "&Iacute;": "Í", "&Icirc;": "Î", "&Iuml;": "Ï", "&ETH;": "Ð", "&Ntilde;": "Ñ", "&Ograve;": "Ò", "&Oacute;": "Ó", "&Ocirc;": "Ô", "&Otilde;": "Õ", "&Ouml;": "Ö", "&times;": "×", "&Oslash;": "Ø", "&Ugrave;": "Ù", "&Uacute;": "Ú", "&Ucirc;": "Û", "&Uuml;": "Ü", "&Yacute;": "Ý", "&THORN;": "Þ", "&szlig;": "ß", "&agrave;": "à", "&aacute;": "á", "&acirc;": "â", "&atilde;": "ã", "&auml;": "ä", "&aring;": "å", "&aelig;": "æ", "&ccedil;": "ç", "&egrave;": "è", "&eacute;": "é", "&ecirc;": "ê", "&euml;": "ë", "&igrave;": "ì", "&iacute;": "í", "&icirc;": "î", "&iuml;": "ï", "&eth;": "ð", "&ntilde;": "ñ", "&ograve;": "ò", "&oacute;": "ó", "&ocirc;": "ô", "&otilde;": "õ", "&ouml;": "ö", "&divide;": "÷", "&oslash;": "ø", "&ugrave;": "ù", "&uacute;": "ú", "&ucirc;": "û", "&uuml;": "ü", "&yacute;": "ý", "&thorn;": "þ", "&yuml;": "ÿ", "&OElig;": "Œ", "&oelig;": "œ", "&Scaron;": "Š", "&scaron;": "š", "&Yuml;": "Ÿ", "&fnof;": "ƒ", "&circ;": "ˆ", "&tilde;": "˜", "&Alpha;": "Α", "&Beta;": "Β", "&Gamma;": "Γ", "&Delta;": "Δ", "&Epsilon;": "Ε", "&Zeta;": "Ζ", "&Eta;": "Η", "&Theta;": "Θ", "&Iota;": "Ι", "&Kappa;": "Κ", "&Lambda;": "Λ", "&Mu;": "Μ", "&Nu;": "Ν", "&Xi;": "Ξ", "&Omicron;": "Ο", "&Pi;": "Π", "&Rho;": "Ρ", "&Sigma;": "Σ", "&Tau;": "Τ", "&Upsilon;": "Υ", "&Phi;": "Φ", "&Chi;": "Χ", "&Psi;": "Ψ", "&Omega;": "Ω", "&alpha;": "α", "&beta;": "β", "&gamma;": "γ", "&delta;": "δ", "&epsilon;": "ε", "&zeta;": "ζ", "&eta;": "η", "&theta;": "θ", "&iota;": "ι", "&kappa;": "κ", "&lambda;": "λ", "&mu;": "μ", "&nu;": "ν", "&xi;": "ξ", "&omicron;": "ο", "&pi;": "π", "&rho;": "ρ", "&sigmaf;": "ς", "&sigma;": "σ", "&tau;": "τ", "&upsilon;": "υ", "&phi;": "φ", "&chi;": "χ", "&psi;": "ψ", "&omega;": "ω", "&thetasym;": "ϑ", "&Upsih;": "ϒ", "&piv;": "ϖ", "&ndash;": "–", "&mdash;": "—", "&lsquo;": "‘", "&rsquo;": "’", "&sbquo;": "‚", "&ldquo;": "“", "&rdquo;": "”", "&bdquo;": "„", "&dagger;": "†", "&Dagger;": "‡", "&bull;": "•", "&hellip;": "…", "&permil;": "‰", "&prime;": "′", "&Prime;": "″", "&lsaquo;": "‹", "&rsaquo;": "›", "&oline;": "‾", "&frasl;": "⁄", "&euro;": "€", "&image;": "ℑ", "&weierp;": "℘", "&real;": "ℜ", "&trade;": "™", "&alefsym;": "ℵ", "&larr;": "←", "&uarr;": "↑", "&rarr;": "→", "&darr;": "↓", "&harr;": "↔", "&crarr;": "↵", "&lArr;": "⇐", "&UArr;": "⇑", "&rArr;": "⇒", "&dArr;": "⇓", "&hArr;": "⇔", "&forall;": "∀", "&part;": "∂", "&exist;": "∃", "&empty;": "∅", "&nabla;": "∇", "&isin;": "∈", "&notin;": "∉", "&ni;": "∋", "&prod;": "∏", "&sum;": "∑", "&minus;": "−", "&lowast;": "∗", "&radic;": "√", "&prop;": "∝", "&infin;": "∞", "&ang;": "∠", "&and;": "∧", "&or;": "∨", "&cap;": "∩", "&cup;": "∪", "&int;": "∫", "&there4;": "∴", "&sim;": "∼", "&cong;": "≅", "&asymp;": "≈", "&ne;": "≠", "&equiv;": "≡", "&le;": "≤", "&ge;": "≥", "&sub;": "⊂", "&sup;": "⊃", "&nsub;": "⊄", "&sube;": "⊆", "&supe;": "⊇", "&oplus;": "⊕", "&otimes;": "⊗", "&perp;": "⊥", "&sdot;": "⋅", "&lceil;": "⌈", "&rceil;": "⌉", "&lfloor;": "⌊", "&rfloor;": "⌋", "&lang;": "⟨", "&rang;": "⟩", "&loz;": "◊", "&spades;": "♠", "&clubs;": "♣", "&hearts;": "♥", "&diams;": "♦"
};

var drilldownChartFilters = {};


function showIntelliSenseSuggestions() {
	$(".leftFileUploads").hide();
	$(".visualizationMainDivwrapper").hide();
	$("#visualizeChartAndDataArea").css("width", "99%", "!important");
	switchSmartBiDesignTabs("li_autoSuggestionsView", "visionChartAutoSuggestionsViewId");
	showIntellisenseAutoSuggestions();
}
function showIntellisenseAutoSuggestions() {
	var labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {
	}
	var response = "<div id='visionChartsAutoSuggestionsOptionsId' class='visionChartsAutoSuggestionsOptionsClass'>"
		+ "<button type='button' value='Create Chart' class='autoSuggestionclass btn ' onclick=\"createAutoSuggestedChart()\">Create Chart</button>"
		+ "<button type='button' value='View Data' class='autoSuggestionclass btn ' onclick=\"showViewData()\">View Data</button>"
		+ "<button type='button' value='Show DashBoard' class='autoSuggestionclass btn ' onclick=\"ShowDashBoard()\">Show DashBoard</button>"
		+ "</div>";
	$("#dialog").html(response);
	$("#dialog").dialog({
		title: (labelObject["Auto Suggestions"] != null ? labelObject["Auto Suggestions"] : "Auto Suggestions"),
		modal: true,
		width: 400,
		height: 250,
		fluid: true,
		buttons: [{
			/*text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
			click: function() {
				$("#dialog").html("");
				$("#dialog").dialog("close");
				$("#dialog").dialog("destroy");
			}*/

		}],
		open: function() {
			$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
			$(".visionHeaderMain").css("z-index", "999");
			$(".visionFooterMain").css("z-index", "999");
			$(".ui-dialog").addClass("bicolumnPopUp");

		},
		beforeClose: function(event, ui) {
			$(".visionHeaderMain").css("z-index", "99999");
			$(".visionFooterMain").css("z-index", "99999");
		}
	});
}
function createAutoSuggestedChart() {
	$("#dialog").html("");
	$("#dialog").dialog("close");
	$("#dialog").dialog("destroy");
	$.ajax({
		type: "POST",
		url: "getAutoSuggestedChartTypes",
		cache: false,
		dataType: 'html',
		async: false,
		success: function(response) {
			if (response != null && response != '' && response != undefined) {
				response = JSON.parse(response);
				var result = response['result'];
				if (result != null && result != '' && result != undefined) {
					$("#visionChartsAutoSuggestionUserId").append(result);
				}
			}
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}
function ShowDashBoard() {
	$.ajax({
		type: "POST",
		url: "getColumnformStr",
		cache: false,
		dataType: 'html',
		async: false,
		success: function(response) {
			if (response != null && !jQuery.isEmptyObject(response)) {

			}
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}
function executeBIEditorScripts(tabId) {
	var script = "";
	var divId = "";
	var tabIndex = $("#" + tabId).jqxTabs("val");
	var content = $("#" + tabId).jqxTabs('getContentAt', parseInt(tabIndex));
	if (content != null) {
		var spliterIdDiv = content['0'];
		if (spliterIdDiv != null) {
			var spliterId = spliterIdDiv.id;
			console.log(spliterId);
			if (spliterId != null && spliterId != '') {
				divId = spliterId.replace("_splitter", "");
			}
		} else {
			var spliterId = content.id;
			console.log(spliterId);
			if (spliterId != null && spliterId != '') {
				divId = spliterId.replace("_splitter", "");
			}

		}

	}

	var sqlMainEditor = sqlMainEditor = ace.edit(divId);
	var script = sqlMainEditor.getSelectedText();
	if (script == "") {
		script = String(sqlMainEditor.getSession().getValue());
	}

	console.log("data:::" + script);
	if (script != null
		&& $.trim(script) != null
		&& $.trim(script) != ''
		&& $.trim(script) != 'null'
		&& $.trim(script.replace(/[\t\n]+/g, ' ')) != null
		&& $.trim(script.replace(/[\t\n]+/g, ' ')) != ''
		&& $.trim(script.replace(/[\t\n]+/g, ' ')) != 'null'
	) {
		var connectionName = $("#" + divId).attr("data-connction-name");
		$.ajax({
			type: "post",
			traditional: true,
			dataType: 'json',
			url: "executeBISQLQuery",
			cache: false,
			data: {
				script: script,
				connectionName: connectionName
			},
			success: function(response, status, xhr) {
				stopLoader();
				if (response != null && !jQuery.isEmptyObject(response)) {
					if (response['selectFlag']) {
						showBIExecutionResults(script, connectionName, response, divId);
					} else {
						$("#dialog").html(response['message']);
						$("#dialog").dialog({
							title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
							modal: true,
							height: 'auto',
							minWidth: 300,
							maxWidth: 'auto',
							fluid: true,
							buttons: [{
								text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
								click: function() {
									$(this).html("");
									//$(this).dialog("close");
									$(this).dialog("destroy");
								}
							}],
							open: function() {
								$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
								$(".visionHeaderMain").css("z-index", "999");
								$(".visionFooterMain").css("z-index", "999");
							},
							beforeClose: function(event, ui) {
								$(".visionHeaderMain").css("z-index", "99999");
								$(".visionFooterMain").css("z-index", "99999");
							}
						});
					}

				}
			},
			error: function(e) {
				console.log(e);
				sessionTimeout(e);
				stopLoader();
			}
		});
	} else {
		showMesg("No scripts/query to be run");
	}
}
function showBIExecutionResults(script, connectionName, response, divId) {
	if (response != null && !jQuery.isEmptyObject(response)) {
		var result = response['gridObject'];
		var dataFieldsArray = result['datafields'];
		var columnsArray = result['columns'];
		var columnsList = result['columnList'];
		var tableName = response['tableName'];
		var joinQueryFlag = response['joinQueryFlag'];
		var dataTypeCountObj = response['dataTypeCount'];
		var sqlScript = script;
		/*var regexTableName;*/
		for (var entitykey in dxpHtmlEntities) {
			var entity = dxpHtmlEntities[entitykey];
			var regex = new RegExp(entitykey, 'g');
			sqlScript = sqlScript.replace(regex, entity);
			/*regexTableName = tableName.replace(regex, entity);*/
		}
		var resultQuery;
		if (sqlScript.includes('\n')) {
			var sqlScriptArr = sqlScript.split('\n');
			var sqlScriptStr = "";
			for (var i = 0; i < sqlScriptArr.length; i++) {
				sqlScriptStr += sqlScriptArr[i];
			}
			if (sqlScriptStr.includes("'")) {
				sqlScriptStr = sqlScriptStr.replace(/'/g, "\\'");
			}
			if (sqlScriptStr.includes("\r")) {
				var stringToRemove = "\r";
				var sqlScriptStr = sqlScriptStr.replace(new RegExp(stringToRemove, 'g'), "&nbsp;");
			}
			resultQuery = sqlScriptStr;
		} else {
			resultQuery = sqlScript;
		}

		tableName = tableName.replace(/ /gi, ":");
		/*var divStr = "<div class='imgList'>";
		divStr += "<img src='images/Data-Analytics-icon.svg' style='width:28px;cursor:pointer;padding-left:3px;margin-right:5px;' onclick=showQueryCharts('" + resultQuery  + "','" + JSON.stringify(columnsList) + "','"+tableName+"','"+joinQueryFlag+"') title='ShowCharts'>"
		divStr += "</div>";*///commented by sai uday
		/* code by sai uday*/
		for (var i = 0; i < columnsList.length; i++) {
			columnsList[i] = columnsList[i].trim();
		}

		var columnsLis = JSON.stringify(columnsList);
		columnsLis = columnsLis.replace(/"/g, "'");
		columnsLis = columnsLis.replace(/\\/g, '"');
		columnsLis = columnsLis.toUpperCase();

		var dataTypeCountObj = JSON.stringify(dataTypeCountObj);
		dataTypeCountObj = dataTypeCountObj.replaceAll('"', '#');
		//		if ((/^[a-zA-Z]+$/.test(tableName))) {
		//			tableName = undefined;
		//		}
		var divStr = "<div class='imgList'>";
		divStr += "<img src='images/Data-Analytics-icon.svg' style='width:28px;cursor:pointer;padding-left:3px;margin-right:5px;' onclick=\"showQueryCharts('"
			+ resultQuery + "'," + columnsLis + ",'" + tableName + "','" + joinQueryFlag + "','Y','" + dataTypeCountObj + "','getSuggestedChartBasedonCols')\" title='ShowCharts'>";
		divStr += "</div>";

		/* code by sai uday*/
		$("#visionVisualizeQueryGridButtonsId").html(divStr);
		$("#visionVisualizeQueryGridDataBodyId").html("<div id = 'chartGridDataDiv' class = 'chartGridDataClass'></div>");
		var dataArray = response['dataArray'];
		var data = {
			dataFieldsArray: dataFieldsArray,
			columnsArray: columnsArray,
			query: script
		}

		var totalCount = response['totalCount'];

		var headerTooltipRenderer = function(element) {
			$(element).parent().jqxTooltip({
				position: 'mouse', theme: 'energyblue',
				position: 'bottom-right',
				showArrow: false, content: $(element).text()
			});
		}
		var source =
		{
			type: 'POST',
			datatype: "json",
			datafields: dataFieldsArray,
			data: data,
			url: 'getChartObjectData',
			cache: false,
			root: 'Rows',
			processdata: function(data) {
				showLoader();
				data['getOnlyDataArray'] = 'Y';
			},
			beforeSend: function() {
				//showLoader();

			}, loadError: function(xhr, status, error) {
			}, loadComplete: function(data) {
				//                               
				stopLoader();
			},
			beforeprocessing: function(data) {

				source.totalrecords = data[data.length - 1];
			},

		};
		//                        $("#chartGridDataDiv").jqxGrid({columns: columnsArray});
		/*window.allGridColumns["chartGridDataDiv"] = columnsList;*/
		var dataAdapter = new $.jqx.dataAdapter(source);
		$("#chartGridDataDiv").jqxGrid(
			{
				width: "99%",
				height: '427px',
				source: dataAdapter,
				theme: 'energyblue',
				pagesize: 50,
				sortable: true,
				pageable: true,
				autoheight: true,
				autoloadstate: false,
				autosavestate: false,
				columnsresize: true,
				columnsreorder: true,
				showfilterrow: true,
				filterable: true,
				selectionmode: 'checkbox',
				pagesizeoptions: [10, 50, 100, 1000],
				rendergridrows: function(params) {
					return params.data;
				},
				columnsresize: true,
				columns: columnsArray
			});


		$("#chartGridDataDiv").on('cellbeginedit', function(event) {

			var args = event.args;
			// column data field.
			var dataField = event.args.datafield;
			// row's bound index.
			var rowBoundIndex = event.args.rowindex;
			// cell value
			var value = args.value;
			// cell old value.
			var oldvalue = args.oldvalue;
			$('#' + gridId).jqxGrid('selectrow', rowBoundIndex);
			$("#last-edit-datafield").val(dataField);
			$("#last-edit-row").val(rowBoundIndex);
		});
		$("#chartGridDataDiv").on('cellendedit', function(event) {

			// event arguments.
			var args = event.args;
			// column data field.
			var dataField = event.args.datafield;
			// row's bound index.
			var rowBoundIndex = event.args.rowindex;
			// cell value
			var value = args.value;
			// cell old value.
			var oldvalue = args.oldvalue;
			// row's data.
			var rowData = args.row;



		});
		$("#chartGridDataDiv").on('cellvaluechanged', function(event) {
			var args = event.args;
			var dataField = args.datafield;
			var dataField1 = args.text;
			var rowIndex = args.rowindex;
			var cellValue = args.value;
			var column = $("#chartGridDataDiv").jqxGrid('getcolumn', event.args.datafield).text;
		});
		$("#chartGridDataDiv").on('celldoubleclick', function(event) {
			var args = event.args;
			var dataField = args.datafield;
			var dataField1 = args.text;
			var rowIndex = args.rowindex;
			var cellValue = args.value;
			var column = $("#chartGridDataDiv").jqxGrid('getcolumn', event.args.datafield).text;
			popupedit(column, cellValue);
		});



	}


}

function showQueryCharts(script, columnsList, tableName, joinQueryFlag, prependFlag, dataTypeCountObj, methodName, columnsListForComplexQueries, aiFlag) {

	var scriptForDecoding = script;
	for (var entitykey in dxpUnHtmlEntities) {
		var entity = dxpUnHtmlEntities[entitykey];
		var regex = new RegExp(entitykey, 'g');
		scriptForDecoding = scriptForDecoding.replace(regex, entity);
		script = script.replace(regex, entity);
	}


	var columnsList = JSON.stringify(columnsList);
	var columnsListForComplexQueries = JSON.stringify(columnsListForComplexQueries);
	var colLength;
	if (columnsList != null && columnsList != '' && columnsList != undefined) {
		columnsList = JSON.parse(columnsList);
	}
	if (columnsList != null && !jQuery.isEmptyObject(columnsList)) {
		colLength = columnsList.length;
	}
	dataTypeCountObj = dataTypeCountObj.replaceAll('#', '"');
	var containsTemp = columnsList.some(function(column) {
		return column.toLowerCase() === "temp";
	});
	if (containsTemp) colLength--;
	if (colLength != null && colLength != '' && colLength != undefined) {
		$.ajax({
			type: "post",
			traditional: true,
			dataType: 'json',
			url: "getSuggestedChartTypesBasedonColumns",
			cache: false,
			data: {
				script: script,
				colLength: colLength,
				columnsList: JSON.stringify(columnsList),
				tableName: tableName,
				joinQueryFlag: joinQueryFlag,
				prependFlag: prependFlag,
				dataTypeCountObj: dataTypeCountObj,
				methodName: methodName,
				columnsListForComplexQueries: columnsListForComplexQueries,
				scriptForDecoding: scriptForDecoding,
				aiFlag: aiFlag
			},
			success: function(response, status, xhr) {
				stopLoader();
				if (response != null && !jQuery.isEmptyObject(response)) {

					$("#dialog").html(response['result']);
					$("#dialog").dialog({
						title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
						modal: true,
						height: 250,
						width: 200,
						maxHeight: 'auto',
						fluid: true,
						buttons: [{
							/*text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
							click: function() {
								$(this).html("");
								//$(this).dialog("close");
								$(this).dialog("destroy");
							}*/
						}],
						open: function() {
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});


				}
			},
			error: function(e) {
				console.log(e);
				sessionTimeout(e);
				stopLoader();
			}
		});

	}

}


function showQueryAILensCharts(script, columnsList, tableName, joinQueryFlag, prependFlag, dataTypeCountObj, methodName, columnsListForComplexQueries, aiFlag) {

	var scriptForDecoding = script;
	for (var entitykey in dxpUnHtmlEntities) {
		var entity = dxpUnHtmlEntities[entitykey];
		var regex = new RegExp(entitykey, 'g');
		scriptForDecoding = scriptForDecoding.replace(regex, entity);
		script = script.replace(regex, entity);
	}


	var columnsList = JSON.stringify(columnsList);
	var columnsListForComplexQueries = JSON.stringify(columnsListForComplexQueries);
	var colLength;
	if (columnsList != null && columnsList != '' && columnsList != undefined) {
		columnsList = JSON.parse(columnsList);
	}
	if (columnsList != null && !jQuery.isEmptyObject(columnsList)) {
		colLength = columnsList.length;
	}
	dataTypeCountObj = dataTypeCountObj.replaceAll('#', '"');
	var containsTemp = columnsList.some(function(column) {
		return column.toLowerCase() === "temp";
	});
	if (containsTemp) colLength--;
	if (colLength != null && colLength != '' && colLength != undefined) {
		$.ajax({
			type: "post",
			traditional: true,
			dataType: 'json',
			url: "getAILensSuggestedChartTypesBasedonColumns",
			cache: false,
			data: {
				script: script,
				colLength: colLength,
				columnsList: JSON.stringify(columnsList),
				tableName: tableName,
				joinQueryFlag: joinQueryFlag,
				prependFlag: prependFlag,
				dataTypeCountObj: dataTypeCountObj,
				methodName: methodName,
				columnsListForComplexQueries: columnsListForComplexQueries,
				scriptForDecoding: scriptForDecoding,
				aiFlag: aiFlag
			},
			success: function(response, status, xhr) {
				stopLoader();
				if (response != null && !jQuery.isEmptyObject(response)) {
					var respColListStr = response['colListStr'];
					var respColSize = response['colSize'];
					var respTableName = response['tableName'];
					var respJoinQueryFlag = response['joinQueryFlag'];
					var respScript = response['script'];
					var respPrependFlag = response['prependFlag'];
					var respWhereCondition = response['whereCondition'];
					var respAiFlag = response['aiFlag'];
					if (respColSize != null && respColSize != '' && respColSize != undefined) {
						if (respColSize == 1) {
							getSuggestedChartBasedonCols(respColListStr, "indicator", respTableName, respJoinQueryFlag, respScript, respPrependFlag, respWhereCondition, respAiFlag);
						} else if (respColSize > 1) {
							getSuggestedChartBasedonCols(respColListStr, "bar", respTableName, respJoinQueryFlag, respScript, respPrependFlag, respWhereCondition, respAiFlag);
						}
					}


				}
			},
			error: function(e) {
				console.log(e);
				sessionTimeout(e);
				stopLoader();
			}
		});

	}

}

var aiLensDataId = 0;
function getSuggestedChartBasedonCols(columnsList, chartType, tableName, joinQueryFlag, script, prependFlag, whereCond, aiFlag) {
	tableName = tableName.replaceAll(':', ' ');
	columnsList = columnsList.replaceAll(':', ' ');
	script = script.replaceAll("@LT#", '<').replaceAll("@GT#", ">");
	for (var entitykey in dxpUnHtmlEntities) {
		var entity = dxpUnHtmlEntities[entitykey];
		var regex = new RegExp(entitykey, 'g');

		script = script.replace(regex, entity);

	}
	var createcount = 0;
	var axisColumns;
	if (columnsList != null && columnsList != '' && columnsList != undefined) {
		columnsList = JSON.parse(columnsList);
	}
	if (columnsList != null && !jQuery.isEmptyObject(columnsList)) {
		if (aiFlag != null && aiFlag != '' && aiFlag != undefined && aiFlag == 'AIFlag') {
			var divStr = "<div class=\"airesponseWrapperDiv\" data-id='" + aiLensDataId + "'>"
				+ "<div id='modalFileAILensChartsCard" + aiLensDataId + "' class='modalFileAILensChartsCardsClass row'></div>"
				+ "<div id='modalFileAILensCharts" + aiLensDataId + "' class='modalFileAILensChartsClass row'></div>"
				+ "</div>";
			$("#aiLensQuickInsightsDivId").append(divStr);
		} else {
			var divStr = "<div id='modalFileCharts' class='modalFileChartsClass row'></div>";
			if ($('#modalFileCharts').length) { }
			else {
				$("#visionVisualizationDataModalChartViewId").append(divStr);
			}
		}
		var tablesObj = [];
		var axisColumns = [];
		var valueColumn = [];
		var comboColumn = [];
		var columnName = columnsList[0];
		var columnObj = {};
		columnObj['tableName'] = tableName;
		if (!(joinQueryFlag != null && joinQueryFlag != '' && joinQueryFlag != undefined && joinQueryFlag != "undefined")) {
			columnObj['columnName'] = tableName + "." + columnName;
		} else {
			columnObj['columnName'] = columnName;
		}
		axisColumns.push(columnObj);
		if (!(tablesObj.indexOf(tableName) > -1)) {
			tablesObj.push(tableName);
		}
		var colLength;

		if (columnsList != null && !jQuery.isEmptyObject(columnsList)) {
			colLength = columnsList.length;
		}
		var containsTemp = columnsList.some(function(column) {
			return column.toLowerCase() === "temp";
		});
		if (containsTemp) colLength--;
		for (var i = 1; i < colLength; i++) {
			//var valueColumn = [];
			var numVal = columnsList[i];
			if (numVal != null && numVal != '' && numVal != undefined) {
				var columnName = numVal;
				if (columnName.indexOf("AS") > -1) {
					columnName = columnName.split("AS")[0];
					columnName = columnName.trim();
				}
				if (columnName.indexOf("(") > -1 && columnName.indexOf(")") > -1) {
					var aggColumnName = columnName.substr(0, columnName.indexOf("("));
					var colName = columnName.substr(columnName.indexOf("(") + 1, columnName.length);
					if (colName != null && colName != '' && colName !== undefined) {
						colName = colName.substr(0, colName.length - 1);
						if (!(colName.indexOf(".") > -1)) {
							if (!(joinQueryFlag != null && joinQueryFlag != '' && joinQueryFlag != undefined && joinQueryFlag != "undefined")) {
								columnName = aggColumnName + "(" + tableName + "." + colName + ")";
							} else {
								columnName = aggColumnName + "(" + colName + ")";
							}

						}
					}
					var valueColumnObj = {};
					var comboColumnObj = {};
					var axixColumnObj = {};
					if (chartType == "BarAndLine") {
						if (i > 1) {
							comboColumnObj['tableName'] = tableName;
							comboColumnObj['columnName'] = columnName;
							comboColumnObj['aggColumnName'] = aggColumnName;
							comboColumnObj['columnLabel'] = colName;
						} else {
							valueColumnObj['tableName'] = tableName;
							valueColumnObj['columnName'] = columnName;
							valueColumnObj['aggColumnName'] = aggColumnName;
							valueColumnObj['columnLabel'] = colName;
						}

					} else {
						valueColumnObj['tableName'] = tableName;
						valueColumnObj['columnName'] = columnName;
						valueColumnObj['aggColumnName'] = aggColumnName;
						valueColumnObj['columnLabel'] = colName;
					}


				} else {
					var valueColumnObj = {};
					var comboColumnObj = {};
					var axixColumnObj = {};
					valueColumnObj['tableName'] = tableName;
					if (!(columnName.indexOf(".") > -1)) {
						if (!(joinQueryFlag != null && joinQueryFlag != '' && joinQueryFlag != undefined && joinQueryFlag != "undefined")) {
							columnName = tableName + "." + columnName;
						} else {
							columnName = columnName;
						}


					}

					if (chartType == "BarAndLine") {
						if (i > 1) {
							comboColumnObj['tableName'] = tableName;
							comboColumnObj['columnName'] = columnName;
							comboColumnObj['aggColumnName'] = "";
							comboColumnObj['columnLabel'] = columnName.split(".")[1];
						} else {

							valueColumnObj['columnName'] = columnName;
							valueColumnObj['aggColumnName'] = "";
							valueColumnObj['columnLabel'] = columnName.split(".")[1];
						}

					} else if (chartType == "heatMap" || chartType == "treemap" || chartType == "sunburst") {

						valueColumnObj['columnName'] = columnName;
						valueColumnObj['aggColumnName'] = "";
						valueColumnObj['columnLabel'] = columnName.split(".")[1];
					} else if (chartType == 'sankey') {
						if (i < 2) {
							axixColumnObj['tableName'] = tableName;
							axixColumnObj['columnName'] = columnName;
						} else {
							valueColumnObj['columnName'] = columnName;
							valueColumnObj['aggColumnName'] = "";
							valueColumnObj['columnLabel'] = columnName.split(".")[1];

						}

					} else {
						valueColumnObj['columnName'] = "SUM(" + columnName + ")";
						valueColumnObj['aggColumnName'] = "SUM";
					}

				}
				if (Object.keys(axixColumnObj).length > 1) {
					axisColumns.push(axixColumnObj);
				}
				if (Object.keys(comboColumnObj).length > 1) {
					comboColumn.push(comboColumnObj);
				}
				if (Object.keys(valueColumnObj).length > 1) {
					valueColumn.push(valueColumnObj);
				}
			}
		}//FOR LOOP CLOSING STMT
		var dataObj = {};
		dataObj['axisColumns'] = JSON.stringify(axisColumns);
		dataObj['valuesColumns'] = JSON.stringify(valueColumn);
		dataObj['comboColumns'] = JSON.stringify(comboColumn);
		dataObj['tablesObj'] = JSON.stringify(tablesObj);
		dataObj['chartType'] = chartType;
		dataObj['axisColumnName'] = columnsList[0].split(".")[1];
		dataObj['chartCOnfigObjStr'] = JSON.stringify(getChartPropertiesEchart(chartType.toUpperCase(), ""));

		var colorsObj = {
			"clrs": ["#EAC117", "#347C2C", "#806517", "#E66C2C", "#5C3317", "#C11B17"]
		};
		dataObj['colorsObj'] = JSON.stringify(colorsObj);

		var number = (Math.random() + ' ').substring(2, 10) + (Math.random() + ' ').substring(2, 10);
		if (prependFlag != null && prependFlag != '' && prependFlag != undefined) {
			$("#modalFileCharts").prepend("<div id='visionVisualizeSuggestedQueryChart" + number + "' class='col-md-6 col-sm-6 col-lg-4 visionVisualizeSuggestedQueryChartClass visionVisualizeModalChartClass'><div id='visionVisualizeSuggestedQueryInnerChart" + number + "' class='visionVisualizeSuggestedQueryChartInnerClass'></div><div id='visionVisualizeSuggestedQueryInnerChart" + number + "_toolBox' class='visionVisualizeModalCharttoolBoxClass'><ul></ul></div><div id='visionVisualizeSuggestedQueryInnerChart" + number + "config' class='visionVisualizeSuggestedQueryChartConfigClass' style='display:none'></div></div>");
		} else {
			if (aiFlag != null && aiFlag != '' && aiFlag != undefined && aiFlag == 'AIFlag') {
				$("#modalFileAILensCharts" + aiLensDataId).append("<div id='visionVisualizeSuggestedQueryChart" + number + "' class='col-md-12 col-sm-12 col-lg-12 visionVisualizeSuggestedQueryChartClass'><div id='visionVisualizeSuggestedQueryInnerChart" + number + "' class='visionVisualizeSuggestedQueryChartInnerClass'></div><div id='visionVisualizeSuggestedQueryInnerChart" + number + "config' class='visionVisualizeSuggestedQueryChartConfigClass' style='display:none'></div></div>");
			} else {
				$("#modalFileCharts").append("<div id='visionVisualizeSuggestedQueryChart" + number + "' class='col-md-6 col-sm-6 col-lg-4 visionVisualizeSuggestedQueryChartClass visionVisualizeModalChartClass'><div id='visionVisualizeSuggestedQueryInnerChart" + number + "' class='visionVisualizeSuggestedQueryChartInnerClass'></div><div id='visionVisualizeSuggestedQueryInnerChart" + number + "_toolBox' class='visionVisualizeModalCharttoolBoxClass'><ul></ul></div><div id='visionVisualizeSuggestedQueryInnerChart" + number + "config' class='visionVisualizeSuggestedQueryChartConfigClass' style='display:none'></div></div>");
			}
		}
		var chartId = "visionVisualizeSuggestedQueryInnerChart" + number;
		dataObj['chartId'] = chartId;
		var configObj = chartFilterConfigObj[chartType];
		$("#" + chartId + "config").html(configObj);
		var chartOptAllObj = {};
		var chartConfigToggleStatus = {};
		var chartConfigPositionKeyObj = {};
		var errorMessageStr = "";
		var errorCount = 0;
		$("#" + chartId + "config ul li").each(function(i, ele) {
			var optColName = $(this).attr("data-column-name");
			var optKeyType = $(this).attr("data-key-type");
			if (optKeyType != null && optKeyType != '' && optKeyType != undefined) {
				chartConfigPositionKeyObj[optColName] = optKeyType;
			}
			var optName = $("#" + optColName).attr("data-opt-name");
			var optMan = $("#" + optColName).attr("data-man");
			var inputType = $("#" + optColName).attr("type");
			var optValue = $("#" + optColName).val();
			if (inputType == 'checkbox') {
				if ($("#" + optColName).is(':checked')) {
					optValue = true;
				} else {
					optValue = false;
				}
			}
			var isChartHoverActive = $("#toggleButtonForchartHover" + chartType.toUpperCase()).hasClass('active');
			if (!isChartHoverActive && optColName.includes('HOVERLABELDATA')) {
				optValue = 'none';
			}
			if (inputType == 'number') { //nested
				if (optValue != null && optValue != '' && optValue >= 1) {
					optValue = parseInt(optValue);
				}
			}
			var toggleBtnClasses = $(this).find('.toggle-btn').attr('class');
			if (toggleBtnClasses !== null && toggleBtnClasses !== '' && toggleBtnClasses !== undefined) {
				if (toggleBtnClasses.includes('active')) {
					chartConfigToggleStatus[optColName] = true;
				} else {
					chartConfigToggleStatus[optColName] = false;
				}
			}
			var isToggleActive = $(this).hasClass('active-filter');
			if (optValue != null && optValue != '' && isToggleActive) {
				chartOptAllObj[optColName] = optValue;
			} else if (optMan == 'M') {
				errorCount++;
				errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + " " + optName + "</span><b>:</b> Should not be null.</tr></td><br>";
			} else if (optColName.includes('SHOWLEGEND') && !isToggleActive) {
				chartOptAllObj[optColName] = false;
			}

		});

		var filteredchartOptAllObj = {};
		$.each(chartConfigPositionKeyObj, function(key, value) {
			var newKey = key.replace(/[0-9]/g, '');
			filteredchartOptAllObj[newKey] = value;
		});
		dataObj["chartPropObj"] = JSON.stringify(chartOptAllObj);
		dataObj["chartConfigPositionKeyStr"] = JSON.stringify(filteredchartOptAllObj);
		dataObj["chartConfigToggleStatus"] = JSON.stringify(chartConfigToggleStatus);

		dataObj["columnsKeys"] = JSON.stringify(columnsList);
		var sqlScript;
		for (var entitykey in dxpUnHtmlEntities) {
			var entity = dxpUnHtmlEntities[entitykey];
			var regex = new RegExp(entitykey, 'g');
			script = script.replace(regex, entity);
			/*regexTableName = tableName.replace(regex, entity);*/
		}
		sqlScript = script.replace(/\xA0/g, ' ');
		dataObj["script"] = sqlScript;

		$("#" + chartId).attr("dataObj", JSON.stringify(dataObj));
		$("#" + chartId + "config ul").remove();
		getModalChartSuggestions(chartId, dataObj, valueColumn, axisColumns, tablesObj, createcount,
			columnsList[0].split(".")[1], chartType, chartOptAllObj, filteredchartOptAllObj, chartConfigToggleStatus, "", aiFlag);
		createcount++;
		autoSuggestedChartCount++;
		$("span.visionAutoSuggestionChartCountSpan").text(autoSuggestedChartCount);
		closeDialogBox("#dialog");
		//		$("#dialog").html("");
		//		$("#dialog").dialog("close");
		//		$("#dialog").dialog("destroy");
		switchSmartBiDesignTabs('li_designView', 'visualizeArea');
		$('.visionVisualizationDataChartViewCLass').hide();
		aiLensDataId++;
	}

}

function executePythonBIEditorScripts(tabId) {
	var script = "";
	var divId = "";
	var tabIndex = $("#" + tabId).jqxTabs("val");
	var content = $("#" + tabId).jqxTabs('getContentAt', parseInt(tabIndex));
	if (content != null) {
		var spliterIdDiv = content['0'];
		if (spliterIdDiv != null) {
			var spliterId = spliterIdDiv.id;
			console.log(spliterId);
			if (spliterId != null && spliterId != '') {
				divId = spliterId.replace("_splitter", "");
			}
		} else {
			var spliterId = content.id;
			console.log(spliterId);
			if (spliterId != null && spliterId != '') {
				divId = spliterId.replace("_splitter", "");
			}

		}

	}

	var sqlMainEditor = sqlMainEditor = ace.edit(divId);
	var script = sqlMainEditor.getSelectedText();
	if (script == "") {
		script = String(sqlMainEditor.getSession().getValue());
	}

	console.log("data:::" + script);
	if (script != null
		&& $.trim(script) != null
		&& $.trim(script) != ''
		&& $.trim(script) != 'null'
		&& $.trim(script.replace(/[\t\n]+/g, ' ')) != null
		&& $.trim(script.replace(/[\t\n]+/g, ' ')) != ''
		&& $.trim(script.replace(/[\t\n]+/g, ' ')) != 'null'
	) {
		var connectionName = $("#" + divId).attr("data-connction-name");
		$.ajax({
			type: "post",
			traditional: true,
			dataType: 'json',
			url: "executeBIPythonQuery",
			cache: false,
			data: {
				script: script,
				connectionName: connectionName
			},
			success: function(response, status, xhr) {
				stopLoader();
				if (response != null && !jQuery.isEmptyObject(response)) {
					if (response['selectFlag']) {
						showPythonBIExecutionResults(script, connectionName, response, divId);
					} else {
						$("#dialog").html(response['message']);
						$("#dialog").dialog({
							title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
							modal: true,
							height: 'auto',
							minWidth: 300,
							maxWidth: 'auto',
							fluid: true,
							buttons: [{
								text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
								click: function() {
									$(this).html("");
									//$(this).dialog("close");
									$(this).dialog("destroy");
								}
							}],
							open: function() {
								$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
								$(".visionHeaderMain").css("z-index", "999");
								$(".visionFooterMain").css("z-index", "999");
							},
							beforeClose: function(event, ui) {
								$(".visionHeaderMain").css("z-index", "99999");
								$(".visionFooterMain").css("z-index", "99999");
							}
						});
					}

				}
			},
			error: function(e) {
				console.log(e);
				sessionTimeout(e);
				stopLoader();
			}
		});
	} else {
		showMesg("No scripts/query to be run");
	}
}
function showPythonBIExecutionResults(script, connectionName, response, divId) {
	if (response != null && !jQuery.isEmptyObject(response)) {
		var result = response['gridObject'];
		var dataFieldsArray = result['datafields'];
		var columnsArray = result['columns'];
		var columnsList = result['columnList'];
		var sqlScript;
		for (var entitykey in HtmlEntities) {
			var entity = HtmlEntities[entitykey];
			var regex = new RegExp(entitykey, 'g');
			sqlScript = script.replace(regex, entity);
		}
		var divStr = "<div class='imgList'>";
		divStr += "<img src='images/Data-Analytics-icon.svg' style='width:28px;cursor:pointer;padding-left:3px;margin-right:5px;' onclick=showQueryCharts('" + sqlScript + "','" + JSON.stringify(columnsList) + "') title='ShowCharts'>"
		divStr += "</div>";
		$("#visionVisualizeQueryGridButtonsId").html(divStr);
		$("#visionVisualizeQueryGridDataBodyId").html("<div id = 'chartGridDataDiv' class = 'chartGridDataClass'></div>");
		var dataArray = response['dataArray'];
		var data = {
			dataFieldsArray: dataFieldsArray,
			columnsArray: columnsArray,
			query: script
		}

		var totalCount = response['totalCount'];

		var headerTooltipRenderer = function(element) {
			$(element).parent().jqxTooltip({
				position: 'mouse', theme: 'energyblue',
				position: 'bottom-right',
				showArrow: false, content: $(element).text()
			});
		}
		var source =
		{
			type: 'POST',
			datatype: "json",
			datafields: dataFieldsArray,
			data: data,
			url: 'getPythonChartObjectData',
			cache: false,
			root: 'Rows',
			processdata: function(data) {
				showLoader();
				data['getOnlyDataArray'] = 'Y';
			},
			beforeSend: function() {
				//showLoader();

			}, loadError: function(xhr, status, error) {
			}, loadComplete: function(data) {
				//                               
				stopLoader();
			},
			beforeprocessing: function(data) {

				source.totalrecords = data[data.length - 1];
			},

		};
		//                        $("#chartGridDataDiv").jqxGrid({columns: columnsArray});
		/*window.allGridColumns["chartGridDataDiv"] = columnsList;*/
		var dataAdapter = new $.jqx.dataAdapter(source);
		$("#chartGridDataDiv").jqxGrid(
			{
				width: "99%",
				height: '427px',
				source: dataAdapter,
				theme: 'energyblue',
				pagesize: 50,
				sortable: true,
				pageable: true,
				autoheight: true,
				autoloadstate: false,
				autosavestate: false,
				columnsresize: true,
				columnsreorder: true,
				showfilterrow: true,
				filterable: true,
				selectionmode: 'checkbox',
				pagesizeoptions: [10, 50, 100, 1000],
				rendergridrows: function(params) {
					return params.data;
				},
				columnsresize: true,
				columns: columnsArray
			});


		$("#chartGridDataDiv").on('cellbeginedit', function(event) {

			var args = event.args;
			// column data field.
			var dataField = event.args.datafield;
			// row's bound index.
			var rowBoundIndex = event.args.rowindex;
			// cell value
			var value = args.value;
			// cell old value.
			var oldvalue = args.oldvalue;
			$('#' + gridId).jqxGrid('selectrow', rowBoundIndex);
			$("#last-edit-datafield").val(dataField);
			$("#last-edit-row").val(rowBoundIndex);
		});
		$("#chartGridDataDiv").on('cellendedit', function(event) {

			// event arguments.
			var args = event.args;
			// column data field.
			var dataField = event.args.datafield;
			// row's bound index.
			var rowBoundIndex = event.args.rowindex;
			// cell value
			var value = args.value;
			// cell old value.
			var oldvalue = args.oldvalue;
			// row's data.
			var rowData = args.row;



		});
		$("#chartGridDataDiv").on('cellvaluechanged', function(event) {
			var args = event.args;
			var dataField = args.datafield;
			var dataField1 = args.text;
			var rowIndex = args.rowindex;
			var cellValue = args.value;
			var column = $("#chartGridDataDiv").jqxGrid('getcolumn', event.args.datafield).text;
		});
		$("#chartGridDataDiv").on('celldoubleclick', function(event) {
			var args = event.args;
			var dataField = args.datafield;
			var dataField1 = args.text;
			var rowIndex = args.rowindex;
			var cellValue = args.value;
			var column = $("#chartGridDataDiv").jqxGrid('getcolumn', event.args.datafield).text;
			popupedit(column, cellValue);
		});



	}


}


function getVoiceRply() {
	$('#voiceTextPopover').remove();
	$(".voiceIcon").append("<div id='voiceTextPopover'></div>")
	$("#voiceTextPopover").html("<div class='homeSearchIcon'><textarea id='voiceTextBox'></textarea><img src='images/search_blue1.png' style='width:25px' onclick='showingChartOnVoiceResponse()'/></div>");
	$("#voiceTextPopover").jqxPopover({
		width: 300,
		height: 80,
		showArrow: true,
		position: 'bottom',
		selector: $(".voiceIcon")

	});
	$(".voiceIcon").find("img").attr("src", "images/voiceLoader.gif");
	$("#voiceTextPopover").jqxPopover('open');
	if ('SpeechRecognition' in window || 'webkitSpeechRecognition' in window) {
		const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
		const recognition = new SpeechRecognition();
		recognition.start();
		recognition.lang = 'en-US';
		$("#voicLoader").show();

		recognition.onresult = function(event) {
			const transcript = event.results[0][0].transcript;
			$("#voiceTextBox").val(transcript);
			$(".voiceIcon").find("img").attr("src", "images/Mike-OutLine-Icon-01.png");
			$("#voicLoader").hide();
		};

		recognition.onerror = function(event) {
			console.error('Speech recognition error:', event.error);
		};
	} else {
		alert('Speech recognition is not supported in your browser.');
	}
}
function showingChartOnVoiceResponse() {
	showLoader();
	var inputText = $("#voiceTextBox").val();

	$.ajax({
		type: "POST",
		dataType: 'json',
		traditional: true,
		url: 'getVoiceResponse',
		cache: false,
		data: {
			'inputText': inputText,

		},
		success: function(response) {
			stopLoader();
			if (response != null && response != undefined && response != '' && !jQuery.isEmptyObject(response)) {
				closeDialogBox("#dialog");
				getQueryForVoiceSearch(response["result"], "getVoiceSuggestedChartBasedonCols");


			}
			else {
				showMesgModelPopup("<b>Please Try Again...</b>")

			}

		}
	});

}


function getQueryForVoiceSearch(response, methodName) {
	if (response != null && !jQuery.isEmptyObject(response)) {
		var query = response["query"];
		var tableName = response['tableName']
		var columnsList = response['columnsList'];
		if (query != null && query != '' && query != undefined) {
			var connectionName = "Current_V10";
			showLoader();
			$.ajax({
				type: "POST",
				url: "executeBISQLQuery",
				cache: false,
				dataType: 'json',
				async: false,
				data: {
					script: query,
					connectionName: connectionName,
					tableName: tableName,
					columnsList: JSON.stringify(columnsList)
				},
				success: function(response) {
					if (response != null && !jQuery.isEmptyObject(response)) {
						if (response['selectFlag']) {
							var result = response['gridObject'];
							var columnsList = result['columnList'];
							var tableName = response['tableName'];
							var joinQueryFlag = response['joinQueryFlag'];
							var dataTypeCountObj = response['dataTypeCount'];
							var columnsListForComplexQueries;
							if (response['columnsListForComplexQueries'] != null && response['columnsListForComplexQueries'] != undefined && !jQuery.isEmptyObject(response['columnsListForComplexQueries'])) {
								columnsListForComplexQueries = response['columnsListForComplexQueries'];
							}
							tableName = tableName.replace(/ /gi, ":");
							var sqlScript = query;
							/*var regexTableName;*/
							for (var entitykey in dxpHtmlEntities) {
								var entity = dxpHtmlEntities[entitykey];
								var regex = new RegExp(entitykey, 'g');
								sqlScript = sqlScript.replace(regex, entity);
								/*regexTableName = tableName.replace(regex, entity);*/
							}
							var resultQuery;
							if (sqlScript.includes('\n')) {
								var sqlScriptArr = sqlScript.split('\n');
								var sqlScriptStr = "";
								for (var i = 0; i < sqlScriptArr.length; i++) {
									sqlScriptStr += sqlScriptArr[i];
								}
								if (sqlScriptStr.includes("'")) {
									sqlScriptStr = sqlScriptStr.replace(/'/g, "\\'");
								}
								if (sqlScriptStr.includes("\r")) {
									var stringToRemove = "\r";
									var sqlScriptStr = sqlScriptStr.replace(new RegExp(stringToRemove, 'g'), "&nbsp;");
								}
								resultQuery = sqlScriptStr;
							} else {
								resultQuery = sqlScript;
							}
							for (var i = 0; i < columnsList.length; i++) {
								columnsList[i] = columnsList[i].trim();
							}

							/*var columnsLis = JSON.stringify(columnsList);
							columnsLis = columnsLis.replace(/"/g, "'");
							columnsLis = columnsLis.replace(/\\/g, '"');
							columnsLis = columnsLis.toUpperCase();*/
							var dataTypeCountObj = JSON.stringify(dataTypeCountObj);
							dataTypeCountObj = dataTypeCountObj.replaceAll('"', '#');
							showQueryCharts(resultQuery, columnsList, tableName, joinQueryFlag, "Y", dataTypeCountObj, methodName, columnsListForComplexQueries);

						} else {
							stopLoader();
							$("#dialog").html(response['message']);
							$("#dialog").dialog({
								title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
								modal: true,
								height: 'auto',
								minWidth: 300,
								maxWidth: 'auto',
								fluid: true,
								buttons: [{
									text: (labelObject['Ok'] != null ? labelObject['Ok'] : 'Ok'),
									click: function() {
										$(this).html("");
										//$(this).dialog("close");
										$(this).dialog("destroy");
									}
								}],
								open: function() {
									$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
									$(".visionHeaderMain").css("z-index", "999");
									$(".visionFooterMain").css("z-index", "999");
								},
								beforeClose: function(event, ui) {
									$(".visionHeaderMain").css("z-index", "99999");
									$(".visionFooterMain").css("z-index", "99999");
								}
							});
						}

					}
				},
				error: function(e) {
					console.log(e);
					sessionTimeout(e);
					stopLoader();
				}
			});
		}
	}
}

function displayChart(data) {

	var chartData = data['result'];
	var dataTypeCountObj = chartData['dataypes'];
	var axisColName = chartData['axisColName'];
	delete chartData['dataypes'];
	delete chartData['axisColName'];
	var title = $("#voiceTextBox").val();
	var colLength = Object.keys(chartData).length;

	if (colLength != null && colLength != '' && colLength != undefined) {
		$.ajax({
			type: "post",
			traditional: true,
			dataType: 'json',
			url: "getVoiceSuggestedChartsBasedonColumns",
			cache: false,
			data: {
				colLength: colLength,
				columnsList: JSON.stringify(chartData),
				dataTypeCountObj: JSON.stringify(dataTypeCountObj),
				axisColName: axisColName,
				title: title

			},
			success: function(response, status, xhr) {
				stopLoader();
				if (response != null && !jQuery.isEmptyObject(response)) {

					$("#dialog").html(response['result']);
					$("#dialog").dialog({
						title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
						modal: true,
						height: 250,
						width: 200,
						maxHeight: 'auto',
						fluid: true,
						buttons: [{
						}],
						open: function() {
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});


				}
			},
			error: function(e) {
				console.log(e);
				sessionTimeout(e);
				stopLoader();
			}
		});

	}

}



function viewChartBasedOnType(dataObj, chartType, axisColumnName, title) {
	closeDialogBox("#dialog1");
	if (dataObj != null && dataObj != '' && dataObj != undefined) {
		dataObj = dataObj.replaceAll("#", " ");
	}
	if (title != null && title != '' && title != undefined) {
		title = title.replaceAll("#", " ");
	}
	var chartId = 'voiceChartId';
	var htmlStr = "<div id='voiceChartId' class='voiceChartClass'></div>"
	$("#dialog1").html(htmlStr);
	$("#dialog1").dialog({
		title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
		modal: true,
		height: 450,
		width: 500,
		maxHeight: 'auto',
		fluid: true,
		buttons: [{
		}],
		open: function() {
			$("#Loader").css("display", "none");
			$("body").css("pointer-events", "auto");
			if (dataObj != null && dataObj != '' && dataObj != undefined) {
				dataObj = JSON.parse(dataObj);
				var chartDataObj = dataObj;
				var data = [];
				var layout = {};
				if (chartType !== null && chartType !== '' && chartType !== undefined && chartType === 'pie') {
					$.each(chartDataObj, function(key) {

						var traceObj = {};
						var colorObj = {};
						if (key !== axisColumnName) {
							traceObj['labels'] = chartDataObj[axisColumnName];
							traceObj['values'] = chartDataObj[key];
							traceObj['type'] = chartType;
							traceObj['name'] = 'value';
							traceObj['marker'] = colorObj;

						}
						if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
							data.push(traceObj);
						}
					});
				} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'donut') {
					$.each(chartDataObj, function(key) {

						var traceObj = {};
						var colorObj = {};
						if (key !== axisColumnName) {
							traceObj['labels'] = chartDataObj[axisColumnName];
							traceObj['values'] = chartDataObj[key];
							traceObj['hole'] = 0.4;
							traceObj['type'] = 'pie';
							traceObj['name'] = 'value';
							traceObj['marker'] = colorObj;

						}
						if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
							data.push(traceObj);
						}
					});
				} else if (chartType != null && chartType != '' && chartType != undefined && (chartType == 'bar'
					|| chartType == 'waterfall' || chartType == 'histogram')) {
					$.each(chartDataObj, function(keyName) {

						var traceObj = {};
						if (keyName !== axisColumnName) {
							if (chartType == 'waterfall') {
								traceObj['x'] = chartDataObj[axisColumnName];
								traceObj['y'] = chartDataObj[keyName];
								traceObj['orientation'] = 'v';
							} else if (chartType == 'histogram') {
								//traceObj['y'] = chartDataObj[axisColumnName];
								traceObj['x'] = chartDataObj[keyName];
							} else {
								traceObj['x'] = chartDataObj[axisColumnName];
								traceObj['y'] = chartDataObj[keyName];
							}
							traceObj['type'] = chartType;

						}
						if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
							data.push(traceObj);
						}
					});
				} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'column') {
					$.each(chartDataObj, function(keyName) {
						var traceObj = {};
						if (keyName !== axisColumnName) {
							traceObj['x'] = chartDataObj[axisColumnName];
							traceObj['y'] = chartDataObj[keyName];
							traceObj['type'] = "bar";

						}
						if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
							data.push(traceObj);
						}
					});
				} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'lines') {
					$.each(chartDataObj, function(keyName, val) {
						var traceObj = {};
						if (keyName !== axisColumnName) {
							traceObj['x'] = chartDataObj[axisColumnName];
							traceObj['y'] = chartDataObj[keyName];
							traceObj['type'] = chartType;

						}

						if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
							data.push(traceObj);
						}
					});
				} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'scatter') {
					$.each(chartDataObj, function(keyName, val) {

						var traceObj = {};
						var colorObj = {};
						if (keyName !== axisColumnName) {
							traceObj['x'] = chartDataObj[axisColumnName];
							traceObj['y'] = chartDataObj[keyName];
							traceObj['type'] = chartType;
							traceObj['mode'] = 'markers';
							traceObj['marker'] = colorObj;
						}
						if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
							data.push(traceObj);
						}
					});
				} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'indicator') {
					var traceObj = {};
					var domainObj = {};
					var domainArr = [];
					domainArr.push(0);
					domainArr.push(1);
					domainObj["x"] = domainArr;
					domainObj["y"] = domainArr;
					traceObj['domain'] = domainObj;
					traceObj['value'] = chartDataObj[0];
					traceObj['type'] = chartType;
					traceObj['mode'] = "gauge+number";
					if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
						data.push(traceObj);
					}
				}
				else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'funnel') {
					$.each(chartDataObj, function(key) {
						var traceObj = {};
						if (key !== axisColumnName) {
							traceObj['y'] = chartDataObj[axisColumnName];
							traceObj['x'] = chartDataObj[key];
							traceObj['type'] = chartType;
							//                                traceObj['orientation'] = 'h';
							traceObj['name'] = 'value';

						}
						if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
							data.push(traceObj);
						}
					});
				} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'scatterpolar') {
					$.each(chartDataObj, function(keyName) {
						var traceObj = {};
						if (keyName !== axisColumnName) {
							traceObj['r'] = chartDataObj[keyName];
							traceObj['theta'] = chartDataObj[axisColumnName];
							traceObj['type'] = chartType;
							traceObj['fill'] = 'toself';

						}
						if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
							data.push(traceObj);
						}
					});
				} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'BarAndLine') {
					$(".fileChartsBorder").css("max-width", "100%", "!important");
					getBarAndLineChart(chartId, chartDataObj, 0, chartType);
					return;
				}
				else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'treemap') {
					getTreeMapChart(chartId, chartDataObj, 0, chartType);
					return;



				} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'sunburst') {
					getSunburstChart(chartId, chartDataObj, 0, chartType);
					return;

				}
				else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'heatMap') {
					$(".fileChartsBorder").css("max-width", "100%");
					getEchartHeatMap(chartId, chartDataObj, 0);
					return;
				}
				else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'candlestick') {


					$(".fileChartsBorder").css("max-width", "100%", "!important");
					getCandlestickChart(chartId, chartDataObj, 0, chartType);
					return;
				}

				var margin = {};
				if (chartType != null && chartType != '' && chartType != undefined && chartType == 'treemap') {
					margin = {
						l: 0,
						r: 0,
						b: 0,
						t: 30,
						pad: 0
					};
				} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'column') {
					margin = {
						l: 50,
						r: 50,
						b: 100,
						t: 50
					};

				} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'bar') {
					margin = {
						l: 150,
						r: 50,
						b: 50,
						t: 50
					};

				} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'lines') {
					margin = {
						l: 50,
						r: 50,
						b: 100,
						t: 50
					};

				} else {
					margin = {
						l: 30,
						r: 50,
						b: 50,
						t: 50
					};
				}
				layout = {
					margin: margin,
					height: 300,
					dragmode: false,
					font: {
						size: 9
					},
					modebar: {
						orientation: 'v',
						color: '#0b4a99',
						activecolor: '#9ED3CD'
					},

					title: {
						text: title,
						font: {
							family: '"Open Sans", verdana, arial, sans-serif',
							size: 12
						},
						xref: 'paper',
						x: 0.1,
					}
				};





				Plotly.newPlot(chartId, data, layout);
			}
			$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
			$(".visionHeaderMain").css("z-index", "999");
			$(".visionFooterMain").css("z-index", "999");
		},
		beforeClose: function(event, ui) {
			$(".visionHeaderMain").css("z-index", "99999");
			$(".visionFooterMain").css("z-index", "99999");
		}
	});


}





function getVoiceSuggestedChartBasedonCols(columnsList, chartType, tableName, joinQueryFlag, script, prependFlag) {
	tableName = tableName.replace(/[^a-zA-Z0-9_]/g, "_").replace(/_+/g, "_");
	columnsList = columnsList.replaceAll(':', ' ');
	var createcount = 0;
	var axisColumns;
	if (columnsList != null && columnsList != '' && columnsList != undefined) {
		columnsList = JSON.parse(columnsList);
	}
	if (columnsList != null && !jQuery.isEmptyObject(columnsList)) {

		closeDialogBox("#dialog1");
		var number = (Math.random() + ' ').substring(2, 10) + (Math.random() + ' ').substring(2, 10);
		$("#dialog1").html("<div id='visionVisualizeSuggestedQueryChart" + number + "' class='col-md-6 col-sm-6 col-lg-4 visionVisualizeSuggestedQueryChartClass fileVoiceChartsBorder'><div id='visionVisualizeSuggestedQueryInnerChart" + number + "' class='visionVisualizeSuggestedQueryChartInnerClass'></div><div id='visionVisualizeSuggestedQueryInnerChart" + number + "config' class='visionVisualizeSuggestedQueryChartConfigClass' style='display:none'></div></div>");
		$("#dialog1").dialog({
			title: (labelObject['Chart'] != null ? labelObject['Chart'] : 'Chart'),
			width: 650,
			maxWidth: 650,
			height: 400,
			maxHeight: 1000,
			fluid: true,
			buttons: [],
			open: function() {

				var tablesObj = [];
				var axisColumns = [];
				var valueColumn = [];
				var comboColumn = [];
				var columnName = columnsList[0];
				var columnObj = {};
				columnObj['tableName'] = tableName;
				if (!(joinQueryFlag != null && joinQueryFlag != '' && joinQueryFlag != undefined && joinQueryFlag != "undefined")) {
					columnObj['columnName'] = tableName + "." + columnName;
				} else {
					columnObj['columnName'] = columnName;
				}
				axisColumns.push(columnObj);
				if (!(tablesObj.indexOf(tableName) > -1)) {
					tablesObj.push(tableName);
				}
				for (var i = 1; i < columnsList.length; i++) {
					//var valueColumn = [];
					var numVal = columnsList[i];
					if (numVal != null && numVal != '' && numVal != undefined) {
						var columnName = numVal;
						if (columnName.indexOf("AS") > -1) {
							columnName = columnName.split("AS")[0];
							columnName = columnName.trim();
						}
						if (columnName.indexOf("(") > -1 && columnName.indexOf(")") > -1) {
							var aggColumnName = columnName.substr(0, columnName.indexOf("("));
							var colName = columnName.substr(columnName.indexOf("(") + 1, columnName.length);
							if (colName != null && colName != '' && colName !== undefined) {
								colName = colName.substr(0, colName.length - 1);
								if (!(colName.indexOf(".") > -1)) {
									if (!(joinQueryFlag != null && joinQueryFlag != '' && joinQueryFlag != undefined && joinQueryFlag != "undefined")) {
										columnName = aggColumnName + "(" + tableName + "." + colName + ")";
									} else {
										columnName = aggColumnName + "(" + colName + ")";
									}

								}
							}
							var valueColumnObj = {};
							var comboColumnObj = {};
							var axixColumnObj = {};
							if (chartType == "BarAndLine") {
								if (i > 1) {
									comboColumnObj['tableName'] = tableName;
									comboColumnObj['columnName'] = columnName;
									comboColumnObj['aggColumnName'] = aggColumnName;
									comboColumnObj['columnLabel'] = colName;
								} else {
									valueColumnObj['tableName'] = tableName;
									valueColumnObj['columnName'] = columnName;
									valueColumnObj['aggColumnName'] = aggColumnName;
									valueColumnObj['columnLabel'] = colName;
								}

							} else {
								valueColumnObj['tableName'] = tableName;
								valueColumnObj['columnName'] = columnName;
								valueColumnObj['aggColumnName'] = aggColumnName;
								valueColumnObj['columnLabel'] = colName;
							}


						} else {
							var valueColumnObj = {};
							var comboColumnObj = {};
							var axixColumnObj = {};
							valueColumnObj['tableName'] = tableName;
							if (!(columnName.indexOf(".") > -1)) {
								if (!(joinQueryFlag != null && joinQueryFlag != '' && joinQueryFlag != undefined && joinQueryFlag != "undefined")) {
									columnName = tableName + "." + columnName;
								} else {
									columnName = columnName;
								}


							}

							if (chartType == "BarAndLine") {
								if (i > 1) {
									comboColumnObj['tableName'] = tableName;
									comboColumnObj['columnName'] = columnName;
									comboColumnObj['aggColumnName'] = "";
									comboColumnObj['columnLabel'] = columnName.split(".")[1];
								} else {

									valueColumnObj['columnName'] = columnName;
									valueColumnObj['aggColumnName'] = "";
									valueColumnObj['columnLabel'] = columnName.split(".")[1];
								}

							} else if (chartType == "heatMap" || chartType == "treemap" || chartType == "sunburst") {

								valueColumnObj['columnName'] = columnName;
								valueColumnObj['aggColumnName'] = "";
								valueColumnObj['columnLabel'] = columnName.split(".")[1];
							} else if (chartType == 'sankey') {
								if (i < 2) {
									axixColumnObj['tableName'] = tableName;
									axixColumnObj['columnName'] = columnName;
								} else {
									valueColumnObj['columnName'] = columnName;
									valueColumnObj['aggColumnName'] = "";
									valueColumnObj['columnLabel'] = columnName.split(".")[1];

								}

							} else {
								valueColumnObj['columnName'] = "SUM(" + columnName + ")";
								valueColumnObj['aggColumnName'] = "SUM";
							}

						}
						if (Object.keys(axixColumnObj).length > 1) {
							axisColumns.push(axixColumnObj);
						}
						if (Object.keys(comboColumnObj).length > 1) {
							comboColumn.push(comboColumnObj);
						}
						if (Object.keys(valueColumnObj).length > 1) {
							valueColumn.push(valueColumnObj);
						}
					}
				}//FOR LOOP CLOSING STMT
				var dataObj = {};
				dataObj['axisColumns'] = JSON.stringify(axisColumns);
				dataObj['valuesColumns'] = JSON.stringify(valueColumn);
				dataObj['comboColumns'] = JSON.stringify(comboColumn);
				dataObj['tablesObj'] = JSON.stringify(tablesObj);
				dataObj['chartType'] = chartType;
				dataObj['axisColumnName'] = columnsList[0].split(".")[1];


				var chartId = "visionVisualizeSuggestedQueryInnerChart" + number;
				dataObj['chartId'] = chartId;
				var configObj = chartFilterConfigObj[chartType];
				$("#" + chartId + "config").html(configObj);
				var chartOptAllObj = {};
				var chartConfigToggleStatus = {};
				var chartConfigPositionKeyObj = {};
				var errorMessageStr = "";
				var errorCount = 0;
				$("#" + chartId + "config ul li").each(function(i, ele) {
					var optColName = $(this).attr("data-column-name");
					var optKeyType = $(this).attr("data-key-type");
					if (optKeyType != null && optKeyType != '' && optKeyType != undefined) {
						chartConfigPositionKeyObj[optColName] = optKeyType;
					}
					var optName = $("#" + optColName).attr("data-opt-name");
					var optMan = $("#" + optColName).attr("data-man");
					var inputType = $("#" + optColName).attr("type");
					var optValue = $("#" + optColName).val();
					if (inputType == 'checkbox') {
						if ($("#" + optColName).is(':checked')) {
							optValue = true;
						} else {
							optValue = false;
						}
					}
					var isChartHoverActive = $("#toggleButtonForchartHover" + chartType.toUpperCase()).hasClass('active');
					if (!isChartHoverActive && optColName.includes('HOVERLABELDATA')) {
						optValue = 'none';
					}
					if (inputType == 'number') { //nested
						if (optValue != null && optValue != '' && optValue >= 1) {
							optValue = parseInt(optValue);
						}
					}
					var toggleBtnClasses = $(this).find('.toggle-btn').attr('class');
					if (toggleBtnClasses !== null && toggleBtnClasses !== '' && toggleBtnClasses !== undefined) {
						if (toggleBtnClasses.includes('active')) {
							chartConfigToggleStatus[optColName] = true;
						} else {
							chartConfigToggleStatus[optColName] = false;
						}
					}
					var isToggleActive = $(this).hasClass('active-filter');
					if (optValue != null && optValue != '' && isToggleActive) {
						chartOptAllObj[optColName] = optValue;
					} else if (optMan == 'M') {
						errorCount++;
						errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + " " + optName + "</span><b>:</b> Should not be null.</tr></td><br>";
					} else if (optColName.includes('SHOWLEGEND') && !isToggleActive) {
						chartOptAllObj[optColName] = false;
					}

				});

				var filteredchartOptAllObj = {};
				$.each(chartConfigPositionKeyObj, function(key, value) {
					var newKey = key.replace(/[0-9]/g, '');
					filteredchartOptAllObj[newKey] = value;
				});
				dataObj["chartPropObj"] = JSON.stringify(chartOptAllObj);
				dataObj["chartConfigPositionKeyStr"] = JSON.stringify(filteredchartOptAllObj);
				dataObj["chartConfigToggleStatus"] = JSON.stringify(chartConfigToggleStatus);

				dataObj["columnsKeys"] = JSON.stringify(columnsList);
				var sqlScript;
				for (var entitykey in HtmlEntities) {
					var entity = HtmlEntities[entitykey];
					var regex = new RegExp(entitykey, 'g');
					sqlScript = script.replace(regex, entity);
					/*regexTableName = tableName.replace(regex, entity);*/
				}
				sqlScript = sqlScript.replace(/\xA0/g, ' ');
				dataObj["script"] = sqlScript;

				$("#" + chartId).attr("dataObj", JSON.stringify(dataObj));
				getVoiceModalChartSuggestions(chartId, dataObj, valueColumn, axisColumns, tablesObj, createcount,
					columnsList[0].split(".")[1], chartType, chartOptAllObj, filteredchartOptAllObj, chartConfigToggleStatus);





				$("#Loader").css("display", "none");
				$("body").css({ "pointer-events": "auto" });
				$(".visionVisualizationDragColumns").addClass('visionVisualizationDragFilterColumns');
				$("#dialog1").addClass('filterPopUp');
				$(".ui-dialog").addClass('homePageDDSlicer');
				$(".ui-dialog").css("z-index", "9999"); //jaggu 
			},
			beforeClose: function(event, ui) {

			}
		});


	}

}




function getVoiceModalChartSuggestions(chartId, dataObj, valueColumns, axisColumns, tablesObj, createcount,
	axisColName, chartType, chartOptAllObj, filteredchartOptAllObj, chartConfigToggleStatus, flag) {
	var tchartId = chartId;
	if (chartType != null && chartType != '' && chartType == 'card') {
		$.ajax({
			url: 'fetchCardFromQuestion',
			type: "POST",
			data: dataObj,
			dataType: 'json',
			traditional: true,
			cache: false,
			success: function(response) {
				stopLoader();

				if (response != null && !jQuery.isEmptyObject(response)) {
					var result = response['value'];
					var chartId = response['chartId'];
					$('.fileVoiceChartsBorder').addClass("voiceModelup");
					var parentChartId = $("#" + chartId).parent().attr('id');
					$("#" + parentChartId).addClass("fileChartsBorder");
					var chartTitle = $("#myInput").val() ?? $("#voiceTextBox").val();
					if (!(chartTitle != null && chartTitle != '' && chartTitle != undefined
						&& chartTitle != "null")) {
						var title = "";
						$.each(valueColumns, function(index, value) {
							var valueColName = value.columnName;
							var valueName = "";
							if (valueColName != null && valueColName != '' && valueColName != undefined && valueColName.indexOf(".") > -1) {
								valueName = value.columnName.split(".")[1];
							} else {
								valueName = value.columnName;
							}

							var openIndex = valueName.indexOf('(', 0);
							var closeIndex = valueName.indexOf(')', openIndex);

							if (openIndex !== -1 && closeIndex !== -1) {
								valuesColumnName = valueName.substring(openIndex + 1, closeIndex).trim();

							}

							//						valuesColumnName = valueName.replace(/[()]/g, "");
							valuesColumnName = valuesColumnName.replace(/_/g, " ");
							defaultLegendNames[valuesColumnName] = valuesColumnName;
							var legendLabels = value['userProvidedLegendLabel'];
							if (legendLabels !== '' && legendLabels !== undefined && legendLabels !== null) {
								userProvidedLegendNames[valuesColumnName] = legendLabels;
							}
							title += valuesColumnName;
							if (index < (valueColumns.length - 1)) {
								title += " and ";
							}
						});
						$.each(axisColumns, function(index, value) {
							var axisName = value.columnName.split(".")[1];
							if (!(axisName != null && axisName != '' && axisName != undefined))
								axisName = value.columnName.split(".")[1];
							if (axisName == null || axisName == '' || axisName == undefined) {
								axisName = value.columnName;

								var openIndex = axisName.indexOf('(', 0);
								var closeIndex = axisName.indexOf(')', openIndex);

								if (openIndex !== -1 && closeIndex !== -1) {
									axisColumnName = axisName.substring(openIndex + 1, closeIndex).trim();

								}
							}
							axisColumnName = axisName.replace(/[()]/g, "");
							axisColumnName = axisColumnName.replace(/_/g, " ");
							if (title != null && title != '' && title != undefined) {
								title += " by " + axisColumnName;
							} else {
								title += axisColumnName;
							}


						});

						var initTitle = "";
						if (title != null && title != '' && title != undefined) {
							title = title.split(' ');
							for (var chr = 0; chr < title.length; chr++) {
								initTitle += title[chr].substring(0, 1).toUpperCase() + title[chr].substring(1, title[chr].length).toLowerCase() + ' '
							}
						}
						chartTitle = initTitle;

					}


					$("#" + chartId).html(`<div class='showCardTitleAndResultFromVoiceCLS'><div class='showCardTitleFromVoiceCLS' id='showCardTitleFromVoiceID'>${chartTitle}</div><div class='showCardFromQuestionsCLS' id='showCardFromQuestionID'>${result}</div></div>`);
					$(".showCardFromVoiceCLS").css('width', '100%', '!important');
					$(".showCardFromVoiceCLS").css('height', '100%', '!important');



				}

			}, error: function(e) {
				console.log("The Error Message is:::" + e.message);
				sessionTimeout(e);
			}
		});
	}
	else {
		$.ajax({
			url: 'fetchModalChartData',
			type: "POST",
			data: dataObj,
			dataType: 'json',
			traditional: true,
			cache: false,
			success: function(response) {
				$("#Loader").css("display", "none");
				$("body").css("pointer-events", "auto");
				$("#modalDataDialog").addClass("modalChartsPopup");
				if (response['flag'] == 'Y' || (response != null && !jQuery.isEmptyObject(response) && response['data'] != null && !jQuery.isEmptyObject(response['data']))) {
					var resultObj = response;
					var chartDataObj = resultObj['data'];
					var dataPropObject = resultObj['dataPropObject'];
					var chartId = resultObj['chartId'];
					var layoutObj = resultObj['layout'];
					var number = resultObj['number'];
					var data = [];
					var layout = {};
					var axisColumnName;
					var valuesColumnName;
					var defaultLegendNames = {};
					var userProvidedLegendNames = {};
					var title = "";
					if (chartId == undefined) {
						chartId = tChartId;
					}
					if (response['flag'] == 'Y' || (chartDataObj != null && chartDataObj != '' && !jQuery.isEmptyObject(chartDataObj))) {
						$.each(valueColumns, function(index, value) {
							var valueColName = value.columnName;
							var valueName = "";
							if (valueColName != null && valueColName != '' && valueColName != undefined && valueColName.indexOf(".") > -1) {
								valueName = value.columnName.split(".")[1];
							} else {
								valueName = value.columnName;
							}

							var openIndex = valueName.indexOf('(', 0);
							var closeIndex = valueName.indexOf(')', openIndex);

							if (openIndex !== -1 && closeIndex !== -1) {
								valuesColumnName = valueName.substring(openIndex + 1, closeIndex).trim();

							}

							//						valuesColumnName = valueName.replace(/[()]/g, "");
							valuesColumnName = valuesColumnName.replace(/_/g, " ");
							defaultLegendNames[valuesColumnName] = valuesColumnName;
							var legendLabels = value['userProvidedLegendLabel'];
							if (legendLabels !== '' && legendLabels !== undefined && legendLabels !== null) {
								userProvidedLegendNames[valuesColumnName] = legendLabels;
							}
							if (valuesColumnName != null && valuesColumnName != '' && valuesColumnName != undefined &&
								valuesColumnName != 'TEMP') {
								title += valuesColumnName;
							}
							else {
								title = title.substring(0, title.length - 5);
							}
							if (index < (valueColumns.length - 1)) {
								title += " and ";
							}
						});
						$.each(axisColumns, function(index, value) {
							var axisName = value.columnName.split(".")[1];
							if (!(axisName != null && axisName != '' && axisName != undefined))
								axisName = value.columnName.split(".")[1];
							if (axisName == null || axisName == '' || axisName == undefined) {
								axisName = value.columnName;

								var openIndex = axisName.indexOf('(', 0);
								var closeIndex = axisName.indexOf(')', openIndex);

								if (openIndex !== -1 && closeIndex !== -1) {
									axisColumnName = axisName.substring(openIndex + 1, closeIndex).trim();

								}
							}
							axisColumnName = axisName.replace(/[()]/g, "");
							axisColumnName = axisColumnName.replace(/_/g, " ");
							defaultLegendNames[axisColumnName] = axisColumnName;
							title += " by " + axisColumnName;

						});

						var initTitle = "";
						if (title != null && title != '' && title != undefined) {
							title = title.split(' ');
							for (var chr = 0; chr < title.length; chr++) {
								initTitle += title[chr].substring(0, 1).toUpperCase() + title[chr].substring(1, title[chr].length).toLowerCase() + ' '
							}
						}
						var item = $("#voiceTextBox").val();
						if (item != null && item != "" && item != undefined) {
							initTitle = item;
						}
						response['chartTitle'] = initTitle;
						var upperChartType = chartType.toUpperCase();
						upperChartType = upperChartType + "CHARTTITLE";
						$("#" + chartId + "config").find("#" + upperChartType).val(initTitle);
						var dataObjStr = $("#" + chartId).attr("dataObj");
						var dataObject = '';
						if (dataObj !== null && dataObj !== ''
							&& dataObj !== undefined) {
							dataObject = JSON.parse(dataObjStr);
						}
						var chartConfigurationPropStr = dataObject['chartPropObj'];
						if (chartConfigurationPropStr !== null && chartConfigurationPropStr !== ''
							&& chartConfigurationPropStr !== undefined) {
							var chartConfigurationPropObject = JSON.parse(chartConfigurationPropStr);
							chartConfigurationPropObject[upperChartType] = initTitle;
							dataObject['chartPropObj'] = JSON.stringify(chartConfigurationPropObject);
							$("#" + chartId).attr("dataObj", JSON.stringify(dataObject));
						}
						var colorArray = [];
						var deleteicon = {
							'height': 512,
							'width': 448,
							'path': 'M135.2 17.69C140.6 6.848 151.7 0 163.8 0H284.2C296.3 0 307.4 6.848 312.8 17.69L320 32H416C433.7 32 448 46.33 448 64C448 81.67 433.7 96 416 96H32C14.33 96 0 81.67 0 64C0 46.33 14.33 32 32 32H128L135.2 17.69zM31.1 128H416V448C416 483.3 387.3 512 352 512H95.1C60.65 512 31.1 483.3 31.1 448V128zM111.1 208V432C111.1 440.8 119.2 448 127.1 448C136.8 448 143.1 440.8 143.1 432V208C143.1 199.2 136.8 192 127.1 192C119.2 192 111.1 199.2 111.1 208zM207.1 208V432C207.1 440.8 215.2 448 223.1 448C232.8 448 240 440.8 240 432V208C240 199.2 232.8 192 223.1 192C215.2 192 207.1 199.2 207.1 208zM304 208V432C304 440.8 311.2 448 320 448C328.8 448 336 440.8 336 432V208C336 199.2 328.8 192 320 192C311.2 192 304 199.2 304 208z',
							'color': 'rgb(31,119,180)'
						};

						var saveChart = {
							'height': 512,
							'width': 512,
							'id': chartId,
							'path': 'M384 160C366.3 160 352 145.7 352 128C352 110.3 366.3 96 384 96H544C561.7 96 576 110.3 576 128V288C576 305.7 561.7 320 544 320C526.3 320 512 305.7 512 288V205.3L342.6 374.6C330.1 387.1 309.9 387.1 297.4 374.6L191.1 269.3L54.63 406.6C42.13 419.1 21.87 419.1 9.372 406.6C-3.124 394.1-3.124 373.9 9.372 361.4L169.4 201.4C181.9 188.9 202.1 188.9 214.6 201.4L320 306.7L466.7 159.1L384 160z',
							'color': 'rgb(31,119,180)'
						};
						var EditIcon = {
							'height': 512,
							'width': 512,
							'id': chartId,
							'path': 'M490.3 40.4C512.2 62.27 512.2 97.73 490.3 119.6L460.3 149.7L362.3 51.72L392.4 21.66C414.3-.2135 449.7-.2135 471.6 21.66L490.3 40.4zM172.4 241.7L339.7 74.34L437.7 172.3L270.3 339.6C264.2 345.8 256.7 350.4 248.4 353.2L159.6 382.8C150.1 385.6 141.5 383.4 135 376.1C128.6 370.5 126.4 361 129.2 352.4L158.8 263.6C161.6 255.3 166.2 247.8 172.4 241.7V241.7zM192 63.1C209.7 63.1 224 78.33 224 95.1C224 113.7 209.7 127.1 192 127.1H96C78.33 127.1 64 142.3 64 159.1V416C64 433.7 78.33 448 96 448H352C369.7 448 384 433.7 384 416V319.1C384 302.3 398.3 287.1 416 287.1C433.7 287.1 448 302.3 448 319.1V416C448 469 405 512 352 512H96C42.98 512 0 469 0 416V159.1C0 106.1 42.98 63.1 96 63.1H192z',
							'color': 'rgb(31,119,180)'
						};
						var AssignUser = {
							'height': 512,
							'width': 512,
							'path': 'M424.1 287c-15.13-15.12-40.1-4.426-40.1 16.97V352H336L153.6 108.8C147.6 100.8 138.1 96 128 96H32C14.31 96 0 110.3 0 128s14.31 32 32 32h80l182.4 243.2C300.4 411.3 309.9 416 320 416h63.97v47.94c0 21.39 25.86 32.12 40.99 17l79.1-79.98c9.387-9.387 9.387-24.59 0-33.97L424.1 287zM336 160h47.97v48.03c0 21.39 25.87 32.09 40.1 16.97l79.1-79.98c9.387-9.391 9.385-24.59-.0013-33.97l-79.1-79.98c-15.13-15.12-40.99-4.391-40.99 17V96H320c-10.06 0-19.56 4.75-25.59 12.81L254 162.7L293.1 216L336 160zM112 352H32c-17.69 0-32 14.31-32 32s14.31 32 32 32h96c10.06 0 19.56-4.75 25.59-12.81l40.4-53.87L154 296L112 352z',
							'color': 'rgb(31,119,180)'
						};
						var config = {
							responsive: true,
							displayModeBar: true,
							downloadImage: true,
							displaylogo: false,
							dragmode: false,
							modeBarButtonsToAdd: [
								/*{
									name: 'Delete', icon: deleteicon, click: function() {
										deleteModalChart(chartId);
									}
								}, 
								{
									name: 'Save', icon: saveChart, click: function(event) {
										saveModalChart(chartId)
									}
								}, 
								{
									name: 'Edit', icon: EditIcon, click: function(event) {
										getModalChartSetting(chartId, chartType, layout, data, createcount, event, "", JSON.stringify(chartConfigToggleStatus));
									}
								}, {
									name: 'Chart Types', icon: AssignUser, click: function(event) {
										changeModalGraph(event, chartId, chartType, layout, data, createcount);
									}
								}*/

							],
							modeBarButtonsToRemove: ['zoomin', 'resetViews', 'resetScale2d', 'zoomout', 'toImage', 'pan2d', 'sendDataToCloud', 'hoverClosestCartesian', 'autoScale2d', 'lasso2d', 'select2d', 'zoom2d']
						};

						if (chartType !== null && chartType !== '' && chartType !== undefined && chartType === 'pie') {
							$.each(chartDataObj, function(key) {

								var traceObj = {};
								var colorObj = {};
								if (key !== axisColumnName) {
									traceObj['labels'] = chartDataObj[axisColumnName];
									traceObj['values'] = chartDataObj[key];
									traceObj['type'] = chartType;
									traceObj['name'] = 'value';
									traceObj['marker'] = colorObj;
									$.each(dataPropObject, function(key, val) {
										traceObj[key] = val;
									});
								}
								if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
									data.push(traceObj);
								}
							});
						} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'donut') {
							$.each(chartDataObj, function(key) {

								var traceObj = {};
								var colorObj = {};
								if (key !== axisColumnName) {
									traceObj['labels'] = chartDataObj[axisColumnName];
									traceObj['values'] = chartDataObj[key];
									traceObj['hole'] = 0.4;
									traceObj['type'] = 'pie';
									traceObj['name'] = 'value';
									traceObj['marker'] = colorObj;
									$.each(dataPropObject, function(key, val) {
										traceObj[key] = val;
									});
								}
								if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
									data.push(traceObj);
								}
							});
						} else if (chartType != null && chartType != '' && chartType != undefined && (chartType == 'bar'
							|| chartType == 'waterfall' || chartType == 'histogram')) {
							var colorCount = 0;
							colorArray = ['#1864ab', '#fd7e14', '#0b7285', '#ff6b6b'];
							$.each(chartDataObj, function(keyName) {

								var traceObj = {};
								var colorObj = {};
								if (keyName !== axisColumnName) {
									if (chartType == 'waterfall') {
										traceObj['x'] = chartDataObj[axisColumnName];
										traceObj['y'] = chartDataObj[keyName];
										traceObj['orientation'] = 'v';
									} else if (chartType == 'histogram') {
										//traceObj['y'] = chartDataObj[axisColumnName];
										traceObj['x'] = chartDataObj[keyName];
									} else {
										traceObj['x'] = chartDataObj[axisColumnName];
										traceObj['y'] = chartDataObj[keyName];
									}
									traceObj['type'] = chartType;




									var keys = keyName.split("ASCOL");
									keyName = keys[0];
									traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
									$.each(dataPropObject, function(key, val) {
										if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
											var colorsArray = val['colors'];
											if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
												colorObj['color'] = colorsArray;
											} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
												colorObj['color'] = colorsArray[colorCount++];
											} else {
												colorObj['color'] = colorArray[colorCount++];
											}
											traceObj[key] = colorObj;
										} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
											colorObj['color'] = colorArray[colorCount++];
											traceObj[key] = colorObj;
										} else {
											traceObj[key] = val;
										}
									});
								}
								if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
									data.push(traceObj);
								}
							});
						} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'column') {
							$.each(chartDataObj, function(keyName) {
								var traceObj = {};
								var colorObj = {};
								if (keyName !== axisColumnName) {
									traceObj['x'] = chartDataObj[axisColumnName];
									traceObj['y'] = chartDataObj[keyName];
									traceObj['type'] = "bar";
									var keys = keyName.split("ASCOL");
									keyName = keys[0];
									traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
									$.each(dataPropObject, function(key, val) {
										if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
											var colorsArray = val['colors'];
											if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
												colorObj['color'] = colorsArray;
											} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
												colorObj['color'] = colorsArray[colorCount++];
											} else {
												colorObj['color'] = colorArray[colorCount++];
											}
											traceObj[key] = colorObj;
										} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
											colorObj['color'] = colorArray[colorCount++];
											traceObj[key] = colorObj;
										} else {
											traceObj[key] = val;
										}
									});
								}
								if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
									data.push(traceObj);
								}
							});
						} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'lines') {
							var colorCount = 0;
							var lineColorCount = 0;
							colorArray = ['#1864ab', '#fd7e14', '#0b7285', '#ff6b6b'];
							$.each(chartDataObj, function(keyName, val) {
								var traceObj = {};
								var colorObj = {};
								if (keyName !== axisColumnName) {
									traceObj['x'] = chartDataObj[axisColumnName];
									traceObj['y'] = chartDataObj[keyName];
									traceObj['type'] = chartType;
									var keys = keyName.split("ASCOL");
									keyName = keys[0];
									traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
									$.each(dataPropObject, function(key, val) {
										if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
											var colorsArray = val['color'];
											if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
												colorObj['color'] = colorsArray;
											} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
												colorObj['color'] = colorsArray[colorCount++];
											} else {
												colorObj['color'] = colorArray[colorCount++];
											}
											colorObj['size'] = val['size'];
											traceObj[key] = colorObj;
										} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
											colorObj['color'] = colorArray[colorCount++];
											traceObj[key] = colorObj;
										} else if (key === 'line' && !jQuery.isEmptyObject(val) && val !== null) {
											var lineObject = Object.assign({}, val);
											var colorsArray = lineObject['color'];
											if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
												lineObject['color'] = colorsArray;
											} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
												lineObject['color'] = colorsArray[lineColorCount++];
											} else {
												lineObject['color'] = colorArray[lineColorCount++];
											}
											traceObj[key] = lineObject;
										} else {
											traceObj[key] = val;
										}
									});
								}

								if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
									data.push(traceObj);
								}
							});
						} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'scatter') {
							var colorCount = 0;
							colorArray = ['#1864ab', '#fd7e14', '#0b7285', '#ff6b6b'];
							$.each(chartDataObj, function(keyName, val) {

								var traceObj = {};
								var colorObj = {};
								if (keyName !== axisColumnName) {
									traceObj['x'] = chartDataObj[axisColumnName];
									traceObj['y'] = chartDataObj[keyName];
									traceObj['type'] = chartType;
									traceObj['mode'] = 'markers';
									traceObj['marker'] = colorObj;
									var keys = keyName.split("ASCOL");
									keyName = keys[0];
									traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
									$.each(dataPropObject, function(key, val) {
										if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
											var colorsArray = val['color'];
											if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
												colorObj['color'] = colorsArray;
											} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
												colorObj['color'] = colorsArray[colorCount++];
											} else {
												colorObj['color'] = colorArray[colorCount++];
											}
											colorObj['size'] = val['size'];
											traceObj[key] = colorObj;
										} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
											colorObj['color'] = colorArray[colorCount++];
											traceObj[key] = colorObj;
										} else {
											traceObj[key] = val;
										}
									});
								}
								if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
									data.push(traceObj);
								}
							});
						} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'indicator') {
							var traceObj = {};
							var domainObj = {};
							var domainArr = [];
							domainArr.push(0);
							domainArr.push(1);
							domainObj["x"] = domainArr;
							domainObj["y"] = domainArr;
							traceObj['domain'] = domainObj;
							traceObj['value'] = chartDataObj[0];
							traceObj['type'] = chartType;
							traceObj['mode'] = "gauge+number";
							traceObj['gauge'] = resultObj['gauge'];
							if (dataPropObject != null && !jQuery.isEmptyObject(dataPropObject)) {
								$.each(dataPropObject, function(key, val) {
									traceObj[key] = val;
								});
							}
							if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
								data.push(traceObj);
							}
						}
						else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'funnel') {
							var colorCount = 0;
							colorArray = ['#1864ab', '#fd7e14', '#0b7285', '#ff6b6b'];
							$.each(chartDataObj, function(key) {
								var traceObj = {};
								var colorObj = {};
								if (key !== axisColumnName) {
									traceObj['y'] = chartDataObj[axisColumnName];
									traceObj['x'] = chartDataObj[key];
									traceObj['type'] = chartType;
									//                                traceObj['orientation'] = 'h';
									traceObj['name'] = 'value';
									$.each(dataPropObject, function(key, val) {
										if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
											var colorsArray = val['colors'];
											if (colorsArray !== undefined && colorsArray !== null && colorsArray.length !== null) {
												colorObj['color'] = colorsArray[colorCount++];
											} else {
												colorObj['color'] = colorArray[colorCount++];
											}
											traceObj[key] = colorObj;
										} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
											colorObj['color'] = colorArray[colorCount++];
											traceObj[key] = colorObj;
										} else {
											traceObj[key] = val;
										}
									});
								}
								if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
									data.push(traceObj);
								}
							});
						} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'scatterpolar') {
							var colorCount = 0;
							colorArray = ['#1864ab', '#fd7e14', '#0b7285', '#ff6b6b'];
							$.each(chartDataObj, function(keyName) {
								var traceObj = {};
								var colorObj = {};
								if (keyName !== axisColumnName) {
									traceObj['r'] = chartDataObj[keyName];
									traceObj['theta'] = chartDataObj[axisColumnName];
									traceObj['type'] = chartType;
									traceObj['fill'] = 'toself';
									var keys = keyName.split("ASCOL");
									keyName = keys[0];
									traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
									$.each(dataPropObject, function(key, val) {
										if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
											var colorsArray = val['colors'];
											if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
												colorObj['color'] = colorsArray;
											} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
												colorObj['color'] = colorsArray[colorCount++];
											} else {
												colorObj['color'] = colorArray[colorCount++];
											}
											traceObj[key] = colorObj;
										} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
											colorObj['color'] = colorArray[colorCount++];
											traceObj[key] = colorObj;
										} else {
											traceObj[key] = val;
										}
									});
								}
								if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
									data.push(traceObj);
								}
							});
						} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'BarAndLine') {
							getBarAndLineChart(chartId, response, count, chartType);
							return;
						}
						else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'treemap') {
							getTreeMapChart(chartId, response, count, chartType);
							return;



						} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'sunburst') {
							getSunburstChart(chartId, response, count, chartType);
							return;

						}
						else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'heatMap') {
							getEchartHeatMap(chartId, response, count);
							return;
						}
						else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'candlestick') {


							getCandlestickChart(chartId, response, count, chartType);
							return;
						}
						else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'BasicAreaChart') {
							getBasicAreaChart(chartId, response, count, chartType);
							return;

						} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'StackedAreaChart') {
							getStackedAreaChart(chartId, response, count, chartType);
							return;

						}
						else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'GradStackAreaChart') {
							getGradientStackedAreaChart(chartId, response, count, chartType);
							return;

						}
						else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'AreaPiecesChart') {
							getAreaPiecesChart(chartId, response, count, chartType);
							return;

						}
						else if (chartType != null && chartType != '' && chartType == 'sankey') {
							getSankeyChart(chartId, response, count, chartType);
							return;
						}

						var margin = {};
						if (chartType != null && chartType != '' && chartType != undefined && chartType == 'treemap') {
							margin = {
								l: 0,
								r: 0,
								b: 0,
								t: 30,
								pad: 0
							};
						} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'column') {
							margin = {
								l: 50,
								r: 50,
								b: 100,
								t: 50
							};

						} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'bar') {
							margin = {
								l: 100,
								r: 50,
								b: 50,
								t: 50
							};

						} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'lines') {
							margin = {
								l: 50,
								r: 50,
								b: 100,
								t: 50
							};

						} else {
							margin = {
								l: 30,
								r: 50,
								b: 50,
								t: 50
							};
						}
						layout = {
							margin: margin,
							height: 300,
							dragmode: false,
							font: {
								size: 9
							},
							modebar: {
								orientation: 'v',
								color: '#0b4a99',
								activecolor: '#9ED3CD'
							},

							title: {
								text: initTitle,
								font: {
									family: '"Open Sans", verdana, arial, sans-serif',
									size: 12
								},
								xref: 'paper',
								x: 0.1,
							}
						};
						var legend = {
							"x": 0.2,
							"y": 0.2,
							"orientation": "h"
						};

						if (layoutObj != null && !jQuery.isEmptyObject(layoutObj)) {
							$.each(layoutObj, function(key, val) {
								layout[key] = val;
							});
						}


						Plotly.newPlot(chartId, data, layout, config);


						$("#" + chartId + " .svg-container").append("<div class='xAxisLabelTooltip'></div>");
						var currentChartXaxisLabelSelector = $("#" + chartId).find(".xaxislayer-above").children();
						currentChartXaxisLabelSelector.each(function(index, element) {
							var labelTitle = $(this).children().text();
							$("#" + chartId + " .xAxisLabelTooltip").append('<span class="xlabelTooltipText">' + labelTitle + "</span>");
						});
						$("#" + chartId + " .xtick").unbind("mouseenter").mouseenter(function(e) {
							var cssTransformProp = $(this).children().attr("transform");
							var firstIndexOfTransformProp = cssTransformProp.split(",")[0];
							var indexOfTransformOpenPar = firstIndexOfTransformProp.indexOf("(");
							var transformHorStr = firstIndexOfTransformProp.substring(indexOfTransformOpenPar + 1, cssTransformProp.length);
							var transformHorVal = parseInt(transformHorStr) - 15;
							showAxisLabelsTooltipOnHover($(this), "xAxisLabelTooltip", chartId, transformHorVal, 0);
						});
						$("#" + chartId + " .xtick").unbind("mouseleave").mouseleave(function(e) {
							hideAxisLabelsTooltipOnHover($(this), "xAxisLabelTooltip", chartId);
						});

						$("#" + chartId + " .svg-container").append("<div class='yAxisLabelTooltip'></div>");
						var currentChartXaxisLabelSelector = $("#" + chartId).find(".yaxislayer-above").children();
						currentChartXaxisLabelSelector.each(function(index, element) {
							var labelTitle = $(this).children().text();
							$("#" + chartId + " .yAxisLabelTooltip").append('<span class="ylabelTooltipText">' + labelTitle + "</span>");
						});
						$("#" + chartId + " .ytick").unbind("mouseenter").mouseenter(function(e) {
							var cssTransformProp = $(this).children().attr("transform");
							var firstIndexOfTransformProp = cssTransformProp.split(",")[1];
							var transformVerStr = firstIndexOfTransformProp.substring(0, firstIndexOfTransformProp.length - 1);
							var transformVerVal = parseInt(transformVerStr) - 230;
							showAxisLabelsTooltipOnHover($(this), "yAxisLabelTooltip", chartId, 0, transformVerVal);
						});
						$("#" + chartId + " .ytick").unbind("mouseleave").mouseleave(function(e) {
							hideAxisLabelsTooltipOnHover($(this), "yAxisLabelTooltip", chartId);
						});


					} else {
						chartId = chartId.replace("Inner", "");
						$("#" + chartId).remove();

					}
				} else {
					chartId = response['chartId'];
					$("#" + chartId).parent().remove();
				}

			}, error: function(e) {
				console.log("The Error Message is:::" + e.message);
				sessionTimeout(e);
			}
		});
	}
}


var insightsData = {};
var insightsResponseData = {};
function getInsightsDataView(tableName) {
	/*ajaxStart();*/
	showLoader();
	$.ajax({
		url: 'getInsightsView',
		type: "POST",
		dataType: 'json',
		traditional: true,
		cache: false,
		async: true,
		data: {
			flag: true,
			tableName: tableName
		},
		success: function(response) {
			stopLoader();
			if (response != null && !jQuery.isEmptyObject(response)) {
				$("#visionChartsAutoSuggestionUserId1").empty();
				$("#visionInsightsVisualizationChartId").empty();
				$("#visionInsightsVisualizationChartDataId").empty();
				var errorMsg = response['error'];
				if (errorMsg != null && errorMsg != '' && errorMsg != undefined) {
					showStr("Error", errorMsg);
					return;
				}
				var insightsMainDiv = response['insightsMainDiv'];
				var insightListMap = response['apiDataObj'];
				insightsData = insightListMap;
				insightsResponseData = response;
				$("#visionChartsAutoSuggestionUserId1").html(insightsMainDiv);
				$("#accordionInsightsId").accordion({
					collapsible: true
				});
				var firstDivClick = $("#accordionInsightsId h3:first").attr("onclick");
				setTimeout(function() {
					eval(firstDivClick);
				}, 2000);

				switchSmartBiDesignTabs('li_autoSuggestionsView', 'visionChartAutoSuggestionsViewId');

			}
		}, error: function(e) {
			console.log("The Error Message is:::" + e.message);
			sessionTimeout(e);
		}
	});

}

function showInsightsSummaryData(divId, insightsType, tableName) {
	var insightListMap = insightsData[insightsType];
	if (insightListMap != null && !jQuery.isEmptyObject(insightListMap)) {
		var insightList = insightListMap['query'];
		var querysMapTemp = insightsData[insightsType];
		var querysMap = querysMapTemp['query'];
		var columnsListMap = querysMapTemp.column;
		if (insightList != null && !jQuery.isEmptyObject(insightList) &&
			querysMap != null && !jQuery.isEmptyObject(querysMap)) {
			getInsightsView(tableName, insightList, querysMap, columnsListMap, divId);
		} else {
			var modalObj = {
				title: 'Message',
				body: "There is no insights for this table."
			};
			var buttonArray = [
				{
					text: 'Close',
					click: function() {

					},
					isCloseButton: true
				}
			];
			modalObj['buttons'] = buttonArray;
			createModal("dataDxpSplitterValue", modalObj);
			$(".modal-dialog").addClass("opacity-animate3");

		}
	}
}

var insightsQueryMapArr = {};
var insightsColumnMapArr = {};
function getInsightsView(tableName, insightsData, queryMap, columnsListMap, divId) {
	insightsQueryMapArr[tableName] = queryMap;
	insightsColumnMapArr[tableName] = columnsListMap;
	/*var ulStr = "<ul> <h6>Insights</h6>";
	$.each(insightsData,function(i,val)
	{
		ulStr += "<li onclick='showInsightsChartAndData(\""+i+"\",\""+tableName+"\")'>"+i+"</li>";
	});
	ulStr += "</ul>";*/

	var ulStr = '<ul> <h6>Insights</h6>';
	$.each(insightsData, function(i, val) {
		ulStr += '<li onclick=\"showInsightsChartAndData(\'' + i.replaceAll(/'/g, "\\'") + '\',\'' + tableName + '\')\">' + i + '</li>';
	});
	ulStr += '</ul>';

	$("#" + divId).html(ulStr);
}

function showInsightsChartAndData(question, tableName) {
	var queryMap = insightsQueryMapArr[tableName];
	var columnsListObj = insightsColumnMapArr[tableName];
	var response = {};
	response['tableName'] = tableName;
	if (question != null && question != '' && question != undefined) {
		if (queryMap != null && !jQuery.isEmptyObject(queryMap)) {
			var query = queryMap[question];
			var columnsListArr = columnsListObj[question];
			response['query'] = query;
			response['columnsList'] = columnsListArr;
		}
	}
	getQueryForVoiceSearch(response, "getInsightsSuggestedChartBasedonCols");

}

function getInsightsSuggestedChartBasedonCols(columnsList, chartType, tableName, joinQueryFlag, script, prependFlag, whereCondition) {
	if (whereCondition != null && whereCondition != undefined) {
		whereCondition = whereCondition.replace(/&aqos;/g, "'").replaceAll(":", " ").replaceAll("@LT#", '<').replaceAll("@GT#", ">");
	}
	closeDialogBox("#dialog");
	tableName = tableName.replaceAll(':', ' ');
	columnsList = columnsList.replaceAll(':', ' ').replaceAll('#', '"');
	script = script.replaceAll("@LT#", '<').replaceAll("@GT#", ">");
	for (var entitykey in dxpUnHtmlEntities) {
		var entity = dxpUnHtmlEntities[entitykey];
		var regex = new RegExp(entitykey, 'g');
		script = script.replace(regex, entity);
	}
	var columnsListForComplexQuery = columnsList;
	var createcount = 0;
	var axisColumns;
	if (columnsList != null && columnsList != '' && columnsList != undefined) {
		columnsList = JSON.parse(columnsList);
	}
	if (columnsList != null && !jQuery.isEmptyObject(columnsList)) {

		var number = (Math.random() + ' ').substring(2, 10) + (Math.random() + ' ').substring(2, 10);
		var divId = "<div id='visionVisualizeSuggestedQueryChart" + number + "' class='col-md-6 col-sm-6 col-lg-4 visionVisualizeSuggestedQueryChartClass fileVoiceChartsBorder'><div id='visionVisualizeSuggestedQueryInnerChart" + number + "' class='visionVisualizeSuggestedQueryChartInnerClass'></div><div id='visionVisualizeSuggestedQueryInnerChart" + number + "config' class='visionVisualizeSuggestedQueryChartConfigClass' style='display:none'></div></div>";
		$("#visionInsightsVisualizationChartId").html(divId);
		var tablesObj = [];
		var axisColumns = [];
		var valueColumn = [];
		var comboColumn = [];
		var columnName = columnsList[0];
		var columnObj = {};
		columnObj['tableName'] = tableName;
		if (!(joinQueryFlag != null && joinQueryFlag != '' && joinQueryFlag != undefined && joinQueryFlag != "undefined")) {
			columnObj['columnName'] = tableName + "." + columnName;
		} else {
			columnObj['columnName'] = columnName;
		}
		axisColumns.push(columnObj);
		if (!(tablesObj.indexOf(tableName) > -1)) {
			tablesObj.push(tableName);
		}
		for (var i = 1; i < columnsList.length; i++) {
			//var valueColumn = [];
			var numVal = columnsList[i];
			if (numVal != null && numVal != '' && numVal != undefined) {
				var columnName = numVal;
				if (columnName.indexOf("AS") > -1) {
					columnName = columnName.split("AS")[0];
					columnName = columnName.trim();
				}
				if (columnName.indexOf("(") > -1 && columnName.indexOf(")") > -1) {
					var aggColumnName = columnName.substr(0, columnName.indexOf("("));
					var colName = columnName.substr(columnName.indexOf("(") + 1, columnName.length);
					if (colName != null && colName != '' && colName !== undefined) {
						colName = colName.substr(0, colName.length - 1);
						if (!(colName.indexOf(".") > -1)) {
							if (!(joinQueryFlag != null && joinQueryFlag != '' && joinQueryFlag != undefined && joinQueryFlag != "undefined")) {
								columnName = aggColumnName + "(" + tableName + "." + colName + ")";
							} else {
								columnName = aggColumnName + "(" + colName + ")";
							}

						}
					}
					var valueColumnObj = {};
					var comboColumnObj = {};
					var axixColumnObj = {};
					if (chartType == "BarAndLine") {
						if (i > 1) {
							comboColumnObj['tableName'] = tableName;
							comboColumnObj['columnName'] = columnName;
							comboColumnObj['aggColumnName'] = aggColumnName;
							comboColumnObj['columnLabel'] = colName;
						}
						else if (chartType == "sankey") {
							if (i != columnsList.length - 1) {
								axixColumnObj['tableName'] = tableName;
								axixColumnObj['columnName'] = columnName;
							} else {
								valueColumnObj['columnName'] = columnName;
								valueColumnObj['aggColumnName'] = aggColumnName;
								valueColumnObj['columnLabel'] = columnName.split(".")[1];
							}

						}


						else {
							valueColumnObj['tableName'] = tableName;
							valueColumnObj['columnName'] = columnName;
							valueColumnObj['aggColumnName'] = aggColumnName;
							valueColumnObj['columnLabel'] = colName;
						}

					} else {
						valueColumnObj['tableName'] = tableName;
						valueColumnObj['columnName'] = columnName;
						valueColumnObj['aggColumnName'] = aggColumnName;
						valueColumnObj['columnLabel'] = colName;
					}


				} else {
					var valueColumnObj = {};
					var comboColumnObj = {};
					var axixColumnObj = {};
					valueColumnObj['tableName'] = tableName;
					if (!(columnName.indexOf(".") > -1)) {
						if (!(joinQueryFlag != null && joinQueryFlag != '' && joinQueryFlag != undefined && joinQueryFlag != "undefined")) {
							columnName = tableName + "." + columnName;
						} else {
							columnName = columnName;
						}


					}

					if (chartType == "BarAndLine") {
						if (i > 1) {
							comboColumnObj['tableName'] = tableName;
							comboColumnObj['columnName'] = columnName;
							comboColumnObj['aggColumnName'] = "";
							comboColumnObj['columnLabel'] = columnName.split(".")[1];
						} else {

							valueColumnObj['columnName'] = columnName;
							valueColumnObj['aggColumnName'] = "";
							valueColumnObj['columnLabel'] = columnName.split(".")[1];
						}

					} else if (chartType == "heatMap" || chartType == "treemap" || chartType == "sunburst") {

						valueColumnObj['columnName'] = columnName;
						valueColumnObj['aggColumnName'] = "";
						valueColumnObj['columnLabel'] = columnName.split(".")[1];
					} else if (chartType == 'sankey') {
						if (i < 2) {
							axixColumnObj['tableName'] = tableName;
							axixColumnObj['columnName'] = columnName;
						} else {
							valueColumnObj['columnName'] = columnName;
							valueColumnObj['aggColumnName'] = "";
							valueColumnObj['columnLabel'] = columnName.split(".")[1];

						}

					} else {
						valueColumnObj['columnName'] = "SUM(" + columnName + ")";
						valueColumnObj['aggColumnName'] = "SUM";
					}

				}
				if (Object.keys(axixColumnObj).length > 1) {
					axisColumns.push(axixColumnObj);
				}
				if (Object.keys(comboColumnObj).length > 1) {
					comboColumn.push(comboColumnObj);
				}
				if (Object.keys(valueColumnObj).length > 1) {
					valueColumn.push(valueColumnObj);
				}
			}
		}//FOR LOOP CLOSING STMT
		var dataObj = {};
		dataObj['axisColumns'] = JSON.stringify(axisColumns);
		dataObj['valuesColumns'] = JSON.stringify(valueColumn);
		dataObj['comboColumns'] = JSON.stringify(comboColumn);
		dataObj['tablesObj'] = JSON.stringify(tablesObj);
		dataObj['chartType'] = chartType;
		dataObj['axisColumnName'] = columnsList[0].split(".")[1];
		dataObj['columnsListForComplexQuery'] = columnsListForComplexQuery;
		dataObj['whereCondition'] = whereCondition;
		var colorsObj = {
			"clrs": ["#EAC117", "#347C2C", "#806517", "#E66C2C", "#5C3317", "#C11B17"]
		};
		dataObj['colorsObj'] = JSON.stringify(colorsObj);


		var chartId = "visionVisualizeSuggestedQueryInnerChart" + number;
		dataObj['chartId'] = chartId;
		var configObj = chartFilterConfigObj[chartType];
		$("#" + chartId + "config").html(configObj);
		var chartOptAllObj = {};
		var chartConfigToggleStatus = {};
		var chartConfigPositionKeyObj = {};
		var errorMessageStr = "";
		var errorCount = 0;
		$("#" + chartId + "config ul li").each(function(i, ele) {
			var optColName = $(this).attr("data-column-name");
			var optKeyType = $(this).attr("data-key-type");
			if (optKeyType != null && optKeyType != '' && optKeyType != undefined) {
				chartConfigPositionKeyObj[optColName] = optKeyType;
			}
			var optName = $("#" + optColName).attr("data-opt-name");
			var optMan = $("#" + optColName).attr("data-man");
			var inputType = $("#" + optColName).attr("type");
			var optValue = $("#" + optColName).val();
			if (inputType == 'checkbox') {
				if ($("#" + optColName).is(':checked')) {
					optValue = true;
				} else {
					optValue = false;
				}
			}
			var isChartHoverActive = $("#toggleButtonForchartHover" + chartType.toUpperCase()).hasClass('active');
			if (!isChartHoverActive && optColName.includes('HOVERLABELDATA')) {
				optValue = 'none';
			}
			if (inputType == 'number') { //nested
				if (optValue != null && optValue != '' && optValue >= 1) {
					optValue = parseInt(optValue);
				}
			}
			var toggleBtnClasses = $(this).find('.toggle-btn').attr('class');
			if (toggleBtnClasses !== null && toggleBtnClasses !== '' && toggleBtnClasses !== undefined) {
				if (toggleBtnClasses.includes('active')) {
					chartConfigToggleStatus[optColName] = true;
				} else {
					chartConfigToggleStatus[optColName] = false;
				}
			}
			var isToggleActive = $(this).hasClass('active-filter');
			if (optValue != null && optValue != '' && isToggleActive) {
				chartOptAllObj[optColName] = optValue;
			} else if (optMan == 'M') {
				errorCount++;
				errorMessageStr += "<tr><td>  " + '<p class="visionGenericTabStatusDialog">' + " " + '<span style="color:blue;">' + " " + optName + "</span><b>:</b> Should not be null.</tr></td><br>";
			} else if (optColName.includes('SHOWLEGEND') && !isToggleActive) {
				chartOptAllObj[optColName] = false;
			}

		});

		var filteredchartOptAllObj = {};
		$.each(chartConfigPositionKeyObj, function(key, value) {
			var newKey = key.replace(/[0-9]/g, '');
			filteredchartOptAllObj[newKey] = value;
		});
		dataObj["chartPropObj"] = JSON.stringify(chartOptAllObj);
		dataObj["chartConfigPositionKeyStr"] = JSON.stringify(filteredchartOptAllObj);
		dataObj["chartConfigToggleStatus"] = JSON.stringify(chartConfigToggleStatus);

		dataObj["columnsKeys"] = JSON.stringify(columnsList);
		var sqlScript;
		for (var entitykey in HtmlEntities) {
			var entity = HtmlEntities[entitykey];
			var regex = new RegExp(entitykey, 'g');
			sqlScript = script.replace(regex, entity);
			/*regexTableName = tableName.replace(regex, entity);*/
		}
		sqlScript = sqlScript.replace(/\xA0/g, ' ');
		dataObj["script"] = sqlScript;
		script = script.replaceAll("@LT#", '<').replaceAll("@GT#", ">");
		$("#" + chartId).attr("dataObj", JSON.stringify(dataObj));
		getVoiceModalChartSuggestions(chartId, dataObj, valueColumn, axisColumns, tablesObj, createcount,
			columnsList[0].split(".")[1], chartType, chartOptAllObj, filteredchartOptAllObj, chartConfigToggleStatus);


		$("#Loader").css("display", "none");
		$("body").css({ "pointer-events": "auto" });
		$("#visionInsightsVisualizationChartDataId.visionInsightsVisualizationChartDataClass").css("display", "block");
		getDataFromInsightsQuery(dataObj);



	}

}


function getDataFromInsightsQuery(response) {
	if (response != null && !jQuery.isEmptyObject(response)) {
		var query = response["script"];
		if (query != null && query != '' && query != undefined) {
			var connectionName = "Current_V10";
			$.ajax({
				type: "POST",
				url: "executeInsightsSQLQuery",
				cache: false,
				dataType: 'json',
				async: false,
				data: {
					script: query,
					connectionName: connectionName
				},
				success: function(response) {
					if (response != null && !jQuery.isEmptyObject(response)) {

						var tableStr = response["tableStr"];
						$("#visionInsightsVisualizationChartDataId").html(tableStr);
					}
				},
				error: function(e) {
					console.log(e);
					sessionTimeout(e);
					stopLoader();
				}
			});
		}
	}
}

function Showdata(...args) {
	setTimeout(function() {
		var divStr = "<div class='visionInsightsShowArgsClass' style=>";
		for (let arg of args) {
			divStr += "<span class='visionInsightsShowArgsSpanClass' style='display: block; width: 100%;font-size:16px;'>" + arg + "</span>";
		}
		divStr += "</div>";
		showStr("Message", divStr);
		$('.ui-dialog-content').css('height', 'auto');
		$('.ui-dialog-content').css('max-height', '200px');
	}, 1000);

}


function onchangeCompareFilter(chartDropDownVal, chartType) {
	var items = {};
	var itemsList1 = [];
	var itemsList2 = [];
	$('#visionVisualizeHomeChartOneFiltersValues div').each(function(event) {
		let value = $(this).text();
		let id = $(this).attr('id');
		if (value != null && value != '' && value != undefined) {
			value = $.trim(value);
		}
		itemsList1.push(value);


	});
	$('#visionVisualizeHomeChartOneFiltersValues div').each(function(event) {
		let value = $(this).text();
		let id = $(this).attr('id');
		if (value != null && value != '' && value != undefined) {
			value = $.trim(value);
		}
		itemsList2.push(value);
	});
	items['chart1'] = itemsList1;
	items['chart2'] = itemsList2;
	updateHomeCompareFilterData(items);
	getHomeCompareChartFilterData("visionDashBoardHomeCompareFilterId", chartDropDownVal, JSON.stringify(items));
	getCompareChart(id, chartType);
	$('#dialog1').append($('#visionDashBoardHomeCompareFilterId'));
}


function showAINotification() {
	showaiLoader();
	$('#stopResponsingID').show(1000);
	$("#aiTypedValue").attr('readonly', true);
	$.ajax({
		type: "POST",
		url: 'showAILensNotificationsData',
		data: {
			notificationFlag: "Y",
		},
		traditional: true,
		cache: false,
		success: function(response) {
			stopaiLoader();
			aiAutoScrollContainer();
			const notificationJson = [];
			$("#aiTypedValue").val('');
			if (response != null && response != undefined && response != "") {
				var responseObj = JSON.parse(response);
				$(".typed-cursor").hide();
				var length = Object.keys(responseObj).length;
				for (var i = 0; i < length; i++) {
					var dataObj = {};
					var messagePopupCreation = "<div class='aiLensResultDataClass'>" + responseObj[(i + 1) + 'row'] + "</div>";

					dataObj['id'] = i + 1;
					dataObj['notif'] = messagePopupCreation;
					notificationJson.push(dataObj);
				}
				const notificationStrings = notificationJson.map((elem) => elem.notif);
				$(".typed-cursor").hide();
				animateListItem(0);
				function animateListItem(index) {
					if (index < notificationJson.length) {
						const listItem = $(`<div class='airesponseWrapperDiv' data-id='${notificationJson[index].id}'><div class='aiLensMainResultBoxClass' id='aiLensMainResultBoxClass'>`
							+ `<div class='listItemsText'></div>`
							+ `<div class='aiLensImageDataClass' id='aiLensImageDataClass'>`
							+ `<span id='aiResulBoxLikeClassId` + aiResultBoxCount + `'><img src=\"images/like_blue.png\" title='Like' style='width:20px;cursor:pointer;'></span>`
							+ `<span id='aiResulBoxDisLikeClassId` + aiResultBoxCount + `'><img src=\"images/dislike_blue.png\" title='Dislike' style='width:20px;cursor:pointer;'></span>`
							+ `<span id='aiResulBoxCopyClassId` + aiResultBoxCount + `'><img src=\"images/aiCopy.png\" title='Copy' style='width:20px;cursor:pointer;'></span>`
							+ `<span id='aiResulBoxDownloadClassId` + aiResultBoxCount + `'><img src=\"images/aiDownload.png\" title='Download' style='width:20px;cursor:pointer;'></span>`
							+ `</div>`
							+ `</div>`);
						$(".aiChatgptResponseContainer").append(listItem);
						const typed = new Typed(listItem.find('.listItemsText')[0], {
							strings: [notificationStrings[index]],
							typeSpeed: 50,
							onComplete: function() {
								console.log('Animation completed for', notificationJson[index].id);
								$(".typed-cursor").hide();
								$("#stopResponsingID").hide();
								$("#aiTypedValue").attr('readonly', false);
								aiResultBoxCount++;
								animateListItem(index + 1);
							}
						});
						$('#stopResponsingID').click(function() {
							typed.stop();
							$("#aiTypedValue").removeAttr("readonly");
							$('#stopResponsingID').hide();
						});
					}
				}
				stopaiLoader();
			}
		}
	});
}

function getAILensDataCard(chartDataObj, axisColumnName, chartType) {
	if (chartDataObj != null && !jQuery.isEmptyObject(chartDataObj)) {
		if (Object.keys(chartDataObj).length == 2) {
			var aiLensDataIdCnt = aiLensDataId - 1;
			var colorArr = ["#e2db93;", "#879bf5;", "#e2cfe6;", "#5cb3aa;", "#ef7760;", "#eea86c;"];
			var divHtml = "";
			var axisArray = [];
			var valuesArray = [];
			$.each(chartDataObj, function(keyName) {
				if (keyName !== axisColumnName) {
					axisArray = chartDataObj[axisColumnName];
					valuesArray = chartDataObj[keyName];
				}
			});
			if (axisArray != null && !jQuery.isEmptyObject(axisArray) &&
				valuesArray != null && !jQuery.isEmptyObject(valuesArray) &&
				Object.keys(axisArray).length == Object.keys(valuesArray).length) {
				divHtml += "<div class=\"niicExploreData\">";
				var j = 0;
				$.each(axisArray, function(i, val) {
					divHtml += "<a href=\"#\" class=\"niicAILensAnalyticsCardClass\" style=\"border-left: 4px solid " + colorArr[j] + "\">"
						+ "<span class=\"rightExploreData\">"
						+ "<span>"
						+ "<p>" + valuesArray[i] + "</p>"
						+ "<p>" + axisArray[i] + "</p>"
						+ "</span>"
						+ "</span>"
						+ "</a>";
					if (i != 0 && j != 0 && i === j) {
						j = 0;
					} else {
						j++;
					}
				});
				divHtml += " </div>";
				$("#modalFileAILensChartsCard" + aiLensDataIdCnt + "").html(divHtml);
			}

		} else if (Object.keys(chartDataObj).length > 2) {

		}
	}
}


function getAILensInsights(tables, aiFlag) {
	var tableDiv = "";
	if (tables != null && !jQuery.isEmptyObject(tables)) {
		if (Object.keys(tables).length == 1) {
			showSummarizeInsightsonColumns(tables[0]);
			return;
		}
		var randomNumber1 = generateRandomNumber();
		tableDiv = "<div id='userTableNamesAILensDivId' class='userTableNamesAILensDivClass text-right'>"
			+ "<div class=\"search AILensInsightsClass\">"
			+ "<input type=\"text\" placeholder=\"search\" id='data-AILensInsightsTablessearch'/>" + "</div>"
			+ "<div id='userAILensInsightsTableNamesDivId' class='userAILensInsightsTableNamesDivClass'>";
		$.each(tables, function(i, val) {
			tableDiv += "<div id='" + val
				+ "_table' class='userTableNameAILensClass' onclick=getAILensInsightsSelectedTableName('"
				+ val + "','visionConversationalAI" + randomNumber1 + "') data-AILensInsightsTablefilter-item data-filter-name=\"" + val
				+ "\">" + val + "</div>";
		});
		tableDiv += "</div>" + "</div>";

		var divId = "<p class='AILensInsightsClass'>Please Select Table</p>";
		var randomNumber = generateRandomNumber();
		var msgDiv = `<div id='visionConversationalAI${randomNumber}' class='convai-message'>
                         <div class='convai-left-message'>${divId}</div>
                         </div>`;
		$('.aiChatgptResponseContainer').append(msgDiv);


		var msgDiv1 = `<div id='visionConversationalAI${randomNumber1}' class='convai-message summarizeSaleClasssId'>
                         <div class='convai-right-message'>${tableDiv}</div>
                         </div>`;
		$('.aiChatgptResponseContainer').append(msgDiv1);


		$('#data-AILensInsightsTablessearch').on('keyup', function() {
			var searchVal = $(this).val();
			var filterItems = $('[data-AILensInsightsTablefilter-item]');

			if (searchVal != '') {
				filterItems.addClass('AilensInsightsTableshidden');
				$('[data-filter-name*="' + searchVal.toUpperCase() + '"]').removeClass('AilensInsightsTableshidden');
			} else {
				filterItems.removeClass('AilensInsightsTableshidden');
			}
		});

	}

}

function getAILensInsightsSelectedTableName(TableName, divId) {
	$("#" + divId).remove();
	var randomNumber = generateRandomNumber();
	var msgDiv = `<div id='visionConversationalAI${randomNumber}' class='convai-message'>
                         <div class='convai-right-message'>${TableName}</div>
                         </div>`;
	$('.aiChatgptResponseContainer').append(msgDiv);
	showSummarizeInsightsonColumns(TableName);

}

function getAILensInsightsSelectedDataTableName(tableName) {
	showSummarizeInsightsonColumns(tableName);
}
function showSummarizeInsightsonColumns(TableName) {
	var randomNumber = generateRandomNumber();
	var msgText = "<p>Do you want Visualize Insights on Total Columns or Selected Columns.</p>";
	var msgDiv = `<div id='visionConversationalAI${randomNumber}' class='convai-message'>
                         <div class='convai-left-message'>${msgText}</div>
                         </div>`;
	$('.aiChatgptResponseContainer').append(msgDiv);

	var randomNumber1 = generateRandomNumber();
	var msgText1 = `<button onclick=getAILensInsightsColumnsBasedonTable('${TableName}','${randomNumber1}','TotalColumns')>Total Columns</button>
	                 <button onclick=getAILensInsightsColumnsBasedonTable('${TableName}','${randomNumber1}','SelectedColumns')>Select Columns</button>`;
	var msgDiv1 = `<div id='visionConversationalAI${randomNumber1}' class='convai-message tableClumnsClassId'>
                         <div class='convai-right-message'>${msgText1}</div>
                         </div>`;
	$('.aiChatgptResponseContainer').append(msgDiv1);
}
function getAILensInsightsColumnsBasedonTable(TableName, divNumber, columnTypes) {
	$("#visionConversationalAI" + divNumber).remove();
	var randomNum = generateRandomNumber();
	var msgDiv = `<div id='visionConversationalAI${randomNum}' class='convai-message'>
                         <div class='convai-right-message'>${columnTypes}</div>
                         </div>`;
	$('.aiChatgptResponseContainer').append(msgDiv);
	$.ajax({
		type: "POST",
		url: "showAITypedValueAnalyticsResults",
		data: {
			aiTypedValue: TableName,
			tableNameFlag: true
		},
		traditional: true,
		cache: false,
		success: function(response) {
			stopaiLoader();
			if (response != null && response != undefined && !jQuery.isEmptyObject(response)) {
				var replyObj = JSON.parse(response);
				if (replyObj != null && !jQuery.isEmptyObject(replyObj) &&
					replyObj['type'] != null && replyObj['type'] != '' && replyObj['type'] != undefined) {
					var columns = replyObj['chartResult'];
					if (columnTypes != null && columnTypes != '' && columnTypes != undefined && columnTypes == 'TotalColumns') {
						var values = columns[TableName];
						getAILensInsightsonColumns(TableName, columnTypes, values)
					}
					else {
						var textMsg = "<p class='AILensInsightsClass'>Please Select Columns for Insights</p>";
						var randomNumber = generateRandomNumber();
						var msgDiv = `<div id='visionConversationalAI${randomNumber}' class='convai-message'>
                         <div class='convai-left-message'>${textMsg}</div>
                         </div>`;
						$('.aiChatgptResponseContainer').append(msgDiv);

						$.each(columns, function(key, val) {
							var msgDiv = `<div id='visionConversationalAI${randomNumber}' class='convai-message'>
                         <div class='convai-right-message'><div id='ailensInsights${key}Id' class='aiLensInsightsColumnsClass'></div>
                         <div id ='ailensInsights${key}ButtonId' class='aiLensInsightsColumnsButtonClass'><button onclick="getAILensInsightsonColumns('${key}','${columnTypes}')">Submit</button></div>
                         </div>
                         </div>`;
							$('.aiChatgptResponseContainer').append(msgDiv);
							$("#ailensInsights" + key + "Id").jqxListBox({
								source: val,
								theme: 'energyblue',
								width: '200px',
								height: '30px',
								filterable: true,
								checkboxes: true,
								searchMode: "containsignorecase",
								width: 200,
								height: 200
							});
						});
					}
				}

			}




		},
		error: function() {
			stopLoader();
		}
	});
}

function getAILensInsightsonColumns(tableName, columnTypes, values) {
	if (columnTypes != null && columnTypes != '' && columnTypes != undefined && columnTypes == 'SelectedColumns') {
		var checkValues = $("#ailensInsights" + tableName + "Id").jqxListBox('getCheckedItems');
		values = [];
		var items = checkValues;
		$.each(items, function(index) {
			var value = this.value;
			//If single value has mutiple commma separating replace them to create as single string and changing to orginal state 
			value = value.replace(/ /g, "#"); // Replace spaces with #
			value = value.replace(/,/g, "$"); // Replace commas with $
			values.push(value);
		});
	}
	if (values != null && !jQuery.isEmptyObject(values)) {
		values = values.toString();
	}
	if (!($("#leftFileUploadMainDivwrapperID").length)) {
		loadVisuvalization();
	}

	$.ajax({
		type: "POST",
		url: "showAILensInsightsResults",
		data: {
			tableName: tableName,
			columns: values
		},
		traditional: true,
		cache: false,
		success: function(response) {
			stopaiLoader();
			if (response != null && response != undefined && !jQuery.isEmptyObject(response)) {
				var response = JSON.parse(response);
				if (response != null && !jQuery.isEmptyObject(response)) {
					$("#visionChartsAutoSuggestionUserId1").empty();
					$("#visionInsightsVisualizationChartId").empty();
					$("#visionInsightsVisualizationChartDataId").empty();
					var insightsMainDiv = response['insightsMainDiv'];
					var insightListMap = response['apiDataObj'];
					if (insightsMainDiv != null && insightsMainDiv != null && insightsMainDiv != undefined &&
						insightListMap != null && !jQuery.isEmptyObject(insightListMap)) {
						insightsData = insightListMap;
						insightsResponseData = response;
						$("#visionChartsAutoSuggestionUserId1").html(insightsMainDiv);
						$("#accordionInsightsId").accordion({
							collapsible: true
						});
						var firstDivClick = $("#accordionInsightsId h3:first").attr("onclick");
						setTimeout(function() {
							eval(firstDivClick);
						}, 2000);

						switchSmartBiDesignTabs('li_autoSuggestionsView', 'visionChartAutoSuggestionsViewId');
						closeAINavigationforInsights();
						getModalFileColumns(event, tableName, "AIFlag");
					} else {
						var modalObj = {
							title: 'Message',
							body: "There is no insights for this table."
						};
						var buttonArray = [
							{
								text: 'Close',
								click: function() {

								},
								isCloseButton: true
							}
						];
						modalObj['buttons'] = buttonArray;
						createModal("dataDxpSplitterValue", modalObj);
						$(".modal-dialog").addClass("opacity-animate3");

					}
				}

			}




		},
		error: function() {
			stopLoader();
		}
	});
}

function closeAINavigationforInsights() {
	$('#myBtnContainer').empty();
	$('.intelliSenseChartVisualizeDivClass').empty();
	$(".aiLensImgSrcAppend").attr('src', '');
	$("#myNav").removeClass('pannelAIwrapper');
	//    $("#myNav").resizable('destroy');
	$("#myNav").css('left', 'inherit');
	document.getElementById("myNav").style.width = "0%";
	//    $(".aiChatContainer").html('');
	$(".aiNotificationsResultClass").html('');
	$("#stopResponsingID").hide(1000);
	$("#aiTypedValue").attr('readonly', false);


}

function showInsightsCompareData(divId, insightsType, tableName) {
	var insightListStr = insightsResponseData[insightsType];
	if (insightListStr != null && insightListStr != '' && insightListStr != undefined) {
		$("#" + divId).html(insightListStr);

	}
}

function getAILensInsightsColumnNames($this, selectBoxId, appendId, tableName) {
	var selectedVal = $("#" + selectBoxId).val();
	if (selectedVal != null && selectedVal != '' && selectedVal != undefined && selectedVal != 'Select') {
		var selectId = selectBoxId.replace("_colDdwId", "");
		$("#" + appendId).empty();
		$("#" + appendId).addClass("aiLensInsightSelectedClassId");
		$("#" + selectId + "_subColDropDownBodyId").empty();
		$("#" + selectId + "_valDropDownBodyId").empty();
		if (!($("#" + selectId + "_colDropDownId").hasClass("ailensInsightsColEmptySpaceClass"))) {
			$("#" + selectId + "_colDropDownId").addClass("ailensInsightsColEmptySpaceClass")
		}
		if (!($("#" + selectId + "_valDropDownId").hasClass("ailensInsightsColEmptySpaceClass"))) {
			$("#" + selectId + "_valDropDownId").addClass("ailensInsightsColEmptySpaceClass")
		}
		var nextSelectBoxId = selectId + "_" + selectedVal;
		var nextSelectBoxValues = $("#" + nextSelectBoxId).val();
		if (nextSelectBoxValues != null && nextSelectBoxValues != '' && nextSelectBoxValues != undefined) {
			nextSelectBoxValues = JSON.parse(nextSelectBoxValues);
			if (nextSelectBoxValues != null && !jQuery.isEmptyObject(nextSelectBoxValues)) {
				$("#" + selectId + "_subColDdwId").empty();
				$.each(nextSelectBoxValues, function(i, val) {
					$("#" + selectId + "_subColDdwId").append($('<option></option>').attr("value", val).text(val));
				});
				$("#" + selectId + "_subColDdwId").css("height", "50px", "!important");

			}
		}
		$("#" + appendId).html("<div class='aiLensInsightsSelectColBodyClass'>" + selectedVal + "</div>");
	}

}


function getAILensInsightsSubColumnNames($this, selectBoxId, appendId, tableName) {
	var selectedVal = $("#" + selectBoxId + " :selected").map(function(i, el) {
		return $(el).val();
	}).get();
	if (selectedVal != null && selectedVal != '' && selectedVal != undefined && selectedVal != 'Select') {
		$("#" + appendId).empty();
		$("#" + appendId).addClass("aiLensInsightSelectedClassId");
		var selectId = selectBoxId.replace("_subColDdwId", "");
		$("#" + selectId + "_valDropDownBodyId").empty();
		$("#" + selectId + "_valDropDownBodyId").css("margin-top", "30px;", "!important");
		$.each(selectedVal, function(i, val) {
			var primarySelectId = selectBoxId.replace("_subColDdwId", "_colDdwId");
			var primarySelectedVal = $("#" + primarySelectId).val();
			var nextSelectBoxId = selectId + "_" + primarySelectedVal + "_" + val;
			var nextSelectBoxValues = $("#" + nextSelectBoxId).val();
			var randomNumber = generateRandomNumber();
			if (nextSelectBoxValues != null && nextSelectBoxValues != '' && nextSelectBoxValues != undefined) {
				nextSelectBoxValues = JSON.parse(nextSelectBoxValues);
				if (nextSelectBoxValues != null && !jQuery.isEmptyObject(nextSelectBoxValues)) {
					$("#" + selectId + "_valDropDownBodyId").append("<div id='valDropDownId" + randomNumber + "' class='valDropdownIdAIlensClass' style='margin-bottom:30px'></div>");
					$.each(nextSelectBoxValues, function(j, value) {
						$("#valDropDownId" + randomNumber).append("<div class='aiLensInsightsSelectColBodyClass'>" + value + "</div>")
					});

				} else {
					$("#" + selectId + "_valDropDownBodyId").append("<div id='valDropDownEmptyId" + randomNumber + "' class='valDropdownIdAIlensClass' style='height:35px'></div>");
				}
			} else {
				$("#" + selectId + "_valDropDownBodyId").append("<div id='valDropDownEmptyId" + randomNumber + "' class='valDropdownIdAIlensClass' style='height:35px'></div>");
			}
			var divHeight = $("#valDropDownId" + randomNumber).height();
			divHeight = (divHeight != null && divHeight != '' && divHeight != undefined) ? (divHeight + 10) : "35";
			var nestedSelectBoxId = nextSelectBoxId + "_query";
			$("#" + appendId).append("<div class='aiLensInsightsSelectColBodyClass' style='height:" + divHeight + "px'><a href='#' onclick=getAILensInsightsCompareChart('" + nestedSelectBoxId + "','" + tableName + "','" + val + "')>" + val + "</a></div>");
		})

	}
}


function getAILensInsightsCompareChart(queryId, tableName, value) {
	var response = {};
	var query = $("#" + queryId).val();
	if (query != null && query != '' && query != undefined) {
		query = query.replaceAll("&&", "'");
		var resultObj = insightsData['Compare'];
		if (resultObj != null && !jQuery.isEmptyObject(resultObj)) {
			var column = resultObj['column'];
			if (column != null && !jQuery.isEmptyObject(column)) {
				var columnsList = column[value];
				response['columnsList'] = columnsList;
				response['query'] = query;
				response['tableName'] = tableName;
				getQueryForVoiceSearch(response, "getInsightsSuggestedChartBasedonCols");
			}
		}
	}
}

function getDataLineageView(tableName) {
	$.ajax({
		type: "POST",
		url: "showDataLineageResults",
		data: {
			tableName: tableName,
		},
		traditional: true,
		cache: false,
		success: function(response) {
			stopaiLoader();
			if (response != null && !jQuery.isEmptyObject(response)) {
				var resultDivStr = response['resultDivStr'];
				if (resultDivStr != null && resultDivStr != null && resultDivStr != undefined) {
					var modalObj = {
						title: 'Data Lineage',
						body: resultDivStr
					};
					var buttonArray = [
						{
							text: 'Close',
							click: function() {

							},
							isCloseButton: true
						}
					];
					modalObj['buttons'] = buttonArray;
					createModal("dataDxpSplitterValue", modalObj);
					$(".modal-dialog").addClass("opacity-animate3");
					$(".modal-dialog").addClass("dataLineageClassID");
				} else {
					var modalObj = {
						title: 'Message',
						body: "No operation has been performed on this table"
					};
					var buttonArray = [
						{
							text: 'Close',
							click: function() {

							},
							isCloseButton: true
						}
					];
					modalObj['buttons'] = buttonArray;
					createModal("dataDxpSplitterValue", modalObj);
					$(".modal-dialog").addClass("opacity-animate3");

				}
			} else {
				var modalObj = {
					title: 'Message',
					body: "No operation has been performed on this table"
				};
				var buttonArray = [
					{
						text: 'Close',
						click: function() {

						},
						isCloseButton: true
					}
				];
				modalObj['buttons'] = buttonArray;
				createModal("dataDxpSplitterValue", modalObj);
				$(".modal-dialog").addClass("opacity-animate3");

			}






		},
		error: function() {
			stopLoader();
		}
	});
}



function getAIForecastData(chartId, expandChartId) {
	$.ajax({
		type: 'post',
		traditional: true,
		dataType: 'html',
		cache: false,
		url: 'getChartFilterData',
		async: false,
		data: {
			chartId: chartId,
			flag: "N"
		},
		success: function(response) {
			if (response != null && !jQuery.isEmptyObject(response)) {
				var result = JSON.parse(response);
				var dataarr = result['dataarr'];
				if (dataarr != null && dataarr != '' && dataarr != undefined) {
					var count = 0;
					for (var i = 0; i < dataarr.length; i++) {
						var XAxix = dataarr[i]['xAxix'];
						var yAxix = dataarr[i]['yAxix'];
						var type = dataarr[i]['type'];
						var table = dataarr[i]['table'];
						var id = dataarr[i]['chartid'];
						var Lebel = dataarr[i]['Lebel'];
						var aggColumnName = dataarr[i]['aggColumnName'];
						var chartPropObj = dataarr[i]['chartPropObj'];
						var chartConfigObj = dataarr[i]['chartConfigObj'];
						var labelLegend = dataarr[i]['labelLegend'];
						var filterCondition = dataarr[i]['filterCondition'];
						var colorsObj = dataarr[i]['colorsObj'];
						var chartConfigToggleStatus = dataarr[i]['chartConfigToggleStatus'];
						var homeFilterColumn = dataarr[i]['homeFilterColumn'];
						var fetchQuery = dataarr[i]['fetchQuery'];
						var radioButtons = dataarr[i]['radioButtons'];
						var comboValue = dataarr[i]['comboValue'];
						var currencyConversionStrObject = dataarr[i]['currencyConversionStrObject'];
						var paramCardDateObj = dataarr[i]['paramCardDateObj'];
						var cardType = dataarr[i]['cardType'];
						var cardTrendType = dataarr[i]['cardTrendType'];
						var cardTrend = dataarr[i]['cardTrend'];
						var zAxix = dataarr[i]['zAxis'];
						var query = dataarr[i]['query'];
						var resizeData = dataarr[i]['resizeData'];
						if ((XAxix != null && XAxix != '' && yAxix != null && yAxix != '' && type != 'Card') || (query != null && query != '' && query != undefined)) {
							var chartid = id;
							if (expandChartId != null && expandChartId != '' && expandChartId != undefined) {
								var chartExtendPropObj = $("#homepageChartDiv_options").val();
								if (chartExtendPropObj != null && chartExtendPropObj != '' && chartExtendPropObj != undefined) {
									chartPropObj = chartExtendPropObj;
								}
								$("#homepageChartDiv_filter").val(filterCondition);
								getExpandVisualizeChart(expandChartId, chartType, XAxix, yAxix, table, aggColumnName, filterCondition, chartPropObj, chartConfigObj, count, '', expandChartId, colorsObj);
							} else {
								var chartExtendPropObj = $("#" + chartid + "_options").val();
								if (chartExtendPropObj != null && chartExtendPropObj != '' && chartExtendPropObj != undefined) {
									chartPropObj = chartExtendPropObj;
								}
								var slicerFilter = [];
								var chartFilter = $("#" + id + "_homePageFilter").val();
								if (chartFilter != null && chartFilter != '' && chartFilter != undefined) {
									chartFilter = JSON.parse(chartFilter);
									if (chartFilter != null && !jQuery.isEmptyObject(chartFilter)) {
										for (var key in chartFilter) {
											var paramObj = chartFilter[key];
											slicerFilter.push(paramObj);
										}
									}
								}
								var slicerFilterCondition = $("#" + id + "_filter").val();
								if (slicerFilterCondition != null && slicerFilterCondition != '' && slicerFilterCondition != undefined) {
									slicerFilterCondition = JSON.parse(slicerFilterCondition);
									if (slicerFilterCondition != null && !jQuery.isEmptyObject(slicerFilterCondition) && slicerFilterCondition.length > 0) {
										for (var key in slicerFilterCondition) {
											var paramObj = slicerFilterCondition[key];
											slicerFilter.push(paramObj);
										}
									}
								}
								if (filterCondition != null && filterCondition != '' && filterCondition != undefined) {
									filterCondition = JSON.parse(filterCondition);
									if (filterCondition != null && !jQuery.isEmptyObject(filterCondition)) {
										for (var key in filterCondition) {
											var paramObj = filterCondition[key];
											slicerFilter.push(paramObj);
										}
									}
								}
								if (slicerFilter != null && !jQuery.isEmptyObject(slicerFilter)) {
									slicerFilter = JSON.stringify(slicerFilter);
								}
								if (query != null && query != undefined && query != "") {
									getQueryVisualizeChart(chartid, type, slicerFilter, chartPropObj, chartConfigObj, count, labelLegend, colorsObj, comboValue, chartConfigToggleStatus, "", "", fetchQuery, radioButtons, currencyConversionStrObject, query, table);

								} else {
									var foreCastFlag = "Y";
									getVisualizeChart(chartid, type, XAxix, yAxix, table, aggColumnName, slicerFilter, chartPropObj, chartConfigObj, count, labelLegend, colorsObj, comboValue, chartConfigToggleStatus, "", "", fetchQuery, radioButtons, currencyConversionStrObject, zAxix, foreCastFlag);
								}

							}

						}

					}
				}


			}
		},
		error: function(e) {
			sessionTimeout(e);
		}
	});
}

function getAILensAnalyticsQuestions() {
	var tablesList = $("#userAIlensAnalyticsExistTableNamesDivId").find('input:checked').map(function() {
		return $(this).val();
	}).get();
	if (tablesList.length == 0) {
		showAnalyticsMsg("Error", "Please select at least one table", "noTableSelectedInAILensAnalaytics");
		return;
	}
	var tableName = tablesList.join(',');
	var randomNumber = generateRandomNumber();
	var msgDiv = `<div id='visionConversationalAI${randomNumber}' class='convai-message'>
                                     <div class='convai-left-message'>${tableName}</div>
                                     </div>`;
	$('#aiLensQuickAnalyticsContentDivId').append(msgDiv);
	var userName = $("#rsUserName").val();
	var [lang, sessionId] = getChatbotParams();
	showaiLoader();
	aiAutoScrollContainer();
	$.ajax({
		type: "POST",
		url: "getAILensInsightsAnalyticsQuestions",
		cache: false,
		data: {
			userName: userName,
			sessionId: sessionId,
			tableName: tableName
		},
		success: function(response) {
			//TODO: Need to add Loader
			stopaiLoader();

			aiAutoScrollContainer();
			if (response != null && !jQuery.isEmptyObject(response)) {
				var questionsDivStr = response['questionsDivStr'];
				$("#aiLensQuickAnalyticsQuestionsDivId").html(questionsDivStr);
				$(".defultShowAIDiv").addClass("quickAnalyticsQuestionsEnables");
				showAnimatedBubbleSequnce();
				$('.quickInsightsAnalyticsQuestionsClass').on('click', function() {
					showaiLoader();
					var value = $(this).text();
					getAILensAnalyticsQuestionsData(value, '', tableName);
					if ($('.quickInsightsAnalyticsQuestionsToggleicon i').hasClass('fa-angle-double-up')) {
						quickInsightsAnalyticsQuestionsToggleicon();
					}

				});
				/*$('#data-quickInsightsAnalyticsQuestions').slick({
									  dots: false,
									  infinite: true,
									  speed: 500,
									  slidesToShow: 4,
									  slidesToScroll: 1,
									  autoplay: true,
									  autoplaySpeed: 2000,
									  arrows: false,
									  vertical:true,

							   });*/



			}

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});

}

function getAILensAnalyticsQuestionsData(question, voiceFlag, tableName, appendId) {

	var randomNumber = generateRandomNumber();
	var msgDiv = `<div id='visionConversationalAI${randomNumber}' class='convai-message'>
                             <div class='convai-right-message'>${question}</div>
                             <div class='convai-left-message convai-left-description' style="display:none"><div id='aiLensAnalyticsQuestionData${randomNumber}' style="width:100%"></div><div id='aiLensAnalyticsQuestionChart${randomNumber}'  style="
         width: 330px;
        height: 330px;

    "></div>
                             </div>`;
	if (appendId != null && appendId != '' && appendId != undefined) {
		$('#' + appendId).append(msgDiv);
	} else {
		$('#aiLensQuickAnalyticsContentDivId').append(msgDiv);
	}
	//$(".aiLensAnalyticsDivClass .quickInsightsAnalyticsQuestionsToggleicon").css("bottom", "8.5%");
	$("#aiLensQuickAnalyticsDivId").removeClass("quickAnalyticsQuestionsClass");
	quickInsightsAnalyticsQuestionsToggleicon();
	$(".defultShowAIDiv").removeClass("quickAnalyticsQuestionsEnables");   // 4136
	//var userName = $("#rsUserName").val();
	var [lang, sessionId] = getChatbotParams();
	var dashbordname;
	var shareUserName;
	if ($('#OptionDropdownData').length > 0 && $('#sharedUserNamesDropdownData').length > 0) {
		var item = $("#OptionDropdownData").jqxDropDownList('getSelectedItem');
		if (item != null) {
			dashbordname = item.value;
		}
		var shareUserNameitem = $("#sharedUserNamesDropdownData").jqxDropDownList('getSelectedItem');
		if (shareUserNameitem != null) {
			shareUserName = shareUserNameitem.value;    
		}
	}
	showaiLoader();
	$.ajax({
		type: "POST",
		url: "getAILensInsightsAnalyticsQuestionsData",
		cache: false,
		data: {
			//userName: userName,
			sessionId: sessionId,
			question: question,
			voiceFlag: voiceFlag,
			tableName: tableName,
			dashboardName: dashbordname,
			shareUserName: shareUserName
		},
		success: function(response) {
			$("#aiTypedValue").val("");
			if (response != null && !jQuery.isEmptyObject(response)) {
				if (response.hasOwnProperty('resultDataStr')) {
					if (response.hasOwnProperty('blobData')) {
						showAudioFileForAnalytics(response['blobData']);
					}
					var resultObj = JSON.parse(response['resultDataStr']);
					setTimeout(function() {
						if (resultObj.hasOwnProperty('ANSWER') && response.hasOwnProperty('isJson')) {
							resultObj['isJson'] = response['isJson'];
							createTableFromJsonObj(resultObj, 'aiLensAnalyticsQuestionData' + randomNumber);
							getAILensAnalyticsChart(resultObj, 'aiLensAnalyticsQuestionChart' + randomNumber);
							stopaiLoader();
						}
					}, 10000);
				}

			} else {
				stopaiLoader();
			}
			aiAutoScrollContainer();


		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
			stopaiLoader();
		}
	});

}
function getAILensAnalyticsChart(resultObj, chartId) {
	if (resultObj.hasOwnProperty('ANSWER') && resultObj.hasOwnProperty('isJson') && resultObj['isJson']) {
		var ansObj = resultObj['ANSWER'];
		var keys = Object.keys(ansObj);
		if (keys != null && !jQuery.isEmptyObject(keys) && Object.keys(keys).length >= 2) {
			var [xAxis, yAxis] = keys;
			var trace1 = {
				x: ansObj[xAxis],
				y: ansObj[yAxis],
				type: 'bar'
			};

			var data = [trace1];


			var layout = {
				title: `${yAxis} by ${xAxis}`,
				xaxis: { title: xAxis },
				yaxis: { title: yAxis },
				height: '330px',
				width: '350px'
			};
			var config = {
				responsive: true,
				displayModeBar: true,
				downloadImage: true,
				displaylogo: false,
				dragmode: false,
				modeBarButtonsToAdd: [],
				modeBarButtonsToRemove: ['toImage', 'zoomin', 'resetViews', 'resetScale2d', 'zoomout', 'pan2d', 'sendDataToCloud', 'hoverClosestCartesian', 'autoScale2d', 'lasso2d', 'select2d', 'zoom2d']
			};
			var ExpandIcon = {
				'height': 512,
				'width': 448,
				'path': 'M447.1 319.1v135.1c0 13.26-10.75 23.1-23.1 23.1h-135.1c-12.94 0-24.61-7.781-29.56-19.75c-4.906-11.1-2.203-25.72 6.937-34.87l30.06-30.06L224 323.9l-71.43 71.44l30.06 30.06c9.156 9.156 11.91 22.91 6.937 34.87C184.6 472.2 172.9 479.1 160 479.1H24c-13.25 0-23.1-10.74-23.1-23.1v-135.1c0-12.94 7.781-24.61 19.75-29.56C23.72 288.8 27.88 288 32 288c8.312 0 16.5 3.242 22.63 9.367l30.06 30.06l71.44-71.44L84.69 184.6L54.63 214.6c-9.156 9.156-22.91 11.91-34.87 6.937C7.798 216.6 .0013 204.9 .0013 191.1v-135.1c0-13.26 10.75-23.1 23.1-23.1h135.1c12.94 0 24.61 7.781 29.56 19.75C191.2 55.72 191.1 59.87 191.1 63.1c0 8.312-3.237 16.5-9.362 22.63L152.6 116.7l71.44 71.44l71.43-71.44l-30.06-30.06c-9.156-9.156-11.91-22.91-6.937-34.87c4.937-11.95 16.62-19.75 29.56-19.75h135.1c13.26 0 23.1 10.75 23.1 23.1v135.1c0 12.94-7.781 24.61-19.75 29.56c-11.1 4.906-25.72 2.203-34.87-6.937l-30.06-30.06l-71.43 71.43l71.44 71.44l30.06-30.06c9.156-9.156 22.91-11.91 34.87-6.937C440.2 295.4 447.1 307.1 447.1 319.1z',
				'color': 'rgb(31,119,180)'
			};
			var tempExpand = {
				name: 'Expand', icon: ExpandIcon, click: function() {
					expandChart('bar', layout, data, chartId, 0, "", "", "", "");
				}
			};
			config.modeBarButtonsToAdd.push(tempExpand);
			Plotly.newPlot(chartId, data, layout, config);
			$('#' + chartId).hide();




		} else {
			var dataObj = resultObj['ANSWER'];
			var keys = Object.keys(dataObj);
			var [xAxis] = keys;
			var value = parseInt(dataObj[xAxis]);
			var rangeVal = value +100;
			const data = [{
				type: "indicator",
				mode: "gauge+number",
				value: value,
				gauge: {
					axis: { range: [0, rangeVal], tickwidth: 1, tickcolor: "darkblue" },
					bar: { color: "darkblue" },
					bgcolor: "white",
					borderwidth: 2,
					bordercolor: "gray",
					//            steps: [
					//                            { range: [0, 50], color: "lightgray" },
					//                            { range: [50, 100], color: "gray" }
					//                        ],
					threshold: {
						line: { color: "red", width: 4 },
						thickness: 0.75,
						value: value
					}
				}
			}];

			const layout = {
				margin: { t: 25, r: 25, l: 25, b: 25 },
				paper_bgcolor: "lavender",
				font: { color: "darkblue", family: "Arial" }
			};

			Plotly.newPlot(chartId, data, layout);
			console.log();
			$('#' + chartId).hide();
		}



	}
	else {
		if (resultObj.hasOwnProperty('ANSWER')
			&& resultObj.hasOwnProperty('isJson') && !resultObj['isJson']) {
			var dataObj = resultObj['ANSWER'];
			var value = dataObj[0];
            const data = [{
				type: "indicator",
				mode: "gauge+number",
				value: value,
				gauge: {
					axis: { range: [0, value+1000], tickwidth: 1, tickcolor: "darkblue" }, 
					bar: { color: "darkblue" },
					bgcolor: "white",
					borderwidth: 2,
					bordercolor: "gray",
					//            steps: [
					//                            { range: [0, 50], color: "lightgray" },
					//                            { range: [50, 100], color: "gray" }
					//                        ],
					threshold: {
						line: { color: "red", width: 4 },
						thickness: 0.75,
						value: value
					}
				}
			}];

			const layout = {
				margin: { t: 25, r: 25, l: 25, b: 25 },
				paper_bgcolor: "lavender",
				font: { color: "darkblue", family: "Arial" }
			};

			Plotly.newPlot(chartId, data, layout);
			console.log();
			$('#' + chartId).hide();
		}
	}
}
function createTableFromJsonObj(resultObj, tableId) {
	if (resultObj.hasOwnProperty('ANSWER') && resultObj.hasOwnProperty('isJson') && resultObj['isJson']) {
		var dataObj = resultObj['ANSWER'];
		var keys = Object.keys(dataObj);
		var values = Object.values(dataObj);
		var maxLength = Math.max(...values.map(arr => arr.length));
		var table = $('<table style="width:100%"></table>');
		var thead = $('<thead></thead>');
		var headerRow = $('<tr></tr>');
		var chartId = tableId.replace('Data', 'Chart');
		keys.forEach((key, index) => {
			var th = $('<th style="text-align:center;color: #0b4a99;"></th>').text(key);
			if (index === keys.length - 1) {
				const icon = $(`<img onclick="toggleChartInAnalayticsTab('${chartId}')" src="images/Column.svg" style="margin-left: 20px;width: 15px;margin-bottom: 5px;height: 15px;">`);
				th.append(icon);
			}
			headerRow.append(th);
		});
		thead.append(headerRow);
		table.append(thead);


		var tbody = $('<tbody></tbody>');
		for (let i = 0; i < maxLength; i++) {
			var row = $('<tr></tr>');
			keys.forEach(key => {
				var td = $('<td style="border:0;font-size:12px; font-weight:600;color:#000;"></td>').text(dataObj[key][i] || '');
				row.append(td);
			});
			tbody.append(row);
		}
		table.append(tbody);
		$('#' + tableId).parent().show();
		$('#' + tableId).empty().append(table);
	} else {
		if (resultObj.hasOwnProperty('ANSWER')
			&& resultObj.hasOwnProperty('isJson') && !resultObj['isJson']) {
			var dataObj = resultObj['ANSWER'];
			var value = dataObj[0];
			var chartId = tableId.replace('Data', 'Chart');
			$('#' + tableId).append(`<div style="display:flex"><div id='singleValueAnalyticsTable'>${value}</div>
								<img onclick="toggleChartInAnalayticsTab('${chartId}')" src="images/Column.svg" style="margin-left: 20px;width: 15px;margin-bottom: 5px;height: 15px;"></div>`);
			$('#' + tableId).parent().show();
		}
	}
	aiAutoScrollContainer();
}

function toggleChartInAnalayticsTab(chartId) {
	aiAutoScrollContainer();
	$('#' + chartId).toggle();
}

function getAILensAnalyticsExistingTables(domain, responseStr) {
	showaiLoader();
	var randomNumber = generateRandomNumber();
	var msgText, msgDiv, msg;
	if (responseStr != null && responseStr != '' && responseStr != undefined) {
		for (var entitykey in dxpUnHtmlEntities) {
			var entity = dxpUnHtmlEntities[entitykey];
			var regex = new RegExp(entitykey, 'g');
			responseStr = responseStr.replace(regex, entity);
		}
		resultObj = JSON.parse(responseStr);
		msgText = resultObj['ANSWER'];
		msg = resultObj['QUESTION']
	} else {
		msgText = "<p>You have below Files/Tables, Choose one from the list.</p>";
	}
	if (!(msgText != null && msgText != undefined && msgText.trim() != '')) {
		msgText = "<p>You have below Files/Tables, Choose one from the list.</p>";
	}
	msgDiv = `<div id='visionConversationalAI${randomNumber}' class='convai-message'>
                                   <div class='convai-right-message'>${msg}</div>
                                 <div class='convai-left-message'>${msgText}</div>
                                 </div>`;

	var userName = $("#rsUserName").val();
	$.ajax({
		type: "POST",
		url: "getAILensAnalyticsUserExistTableNamesData",
		cache: false,
		data: {
			userName: userName,


		},
		success: function(response) {
			stopaiLoader();
			aiAutoScrollContainer();
			if (response != null && !jQuery.isEmptyObject(response)) {
				var tableDiv = response['tableDiv'];
				if (tableDiv != null && tableDiv != '' && tableDiv != undefined) {
					$('#aiLensQuickAnalyticsContentDivId').append(msgDiv);
					var msgText = "<div id='userAIlensAnalyticsTablesDivId' class='userAILensAnalyticsTablesDivClass'>"
						+ tableDiv
						+ "<button class=\"userAIlensAnalyticsExistTableNamesButton userAIlensAnalyticsTableButton\" onclick=\"getAILensAnalyticsQuestions()\">AI suggest questions to ask</button>"
						+ "<button class=\"userAIlensAnalyticsExistTableNamesButton userAIlensAnalyticsTableButton\" onclick=\"getAILensAnalyticsInsights()\">Insights</button>"
						+ "</div>";
					$("#aiLensQuickAnalyticsContentDivId").append(msgText);
					showAnimatedBubbleSequnce();
					$('#data-AnalyticsExistTablessearch').on('keyup', function() {
						var searchVal = $(this).val();
						var filterItems = $('[data-AIlensAnalyticsExistTablefilter-item]');

						if (searchVal != '') {
							filterItems.addClass('aiLensAnalyticsExistTableshidden');
							$('[data-AILensAnalyticsExistfilter-name*="' + searchVal.toUpperCase() + '"]').removeClass('aiLensAnalyticsExistTableshidden');
						} else {
							filterItems.removeClass('aiLensAnalyticsExistTableshidden');
						}
					});
				} else {
					randomNumber = generateRandomNumber();
					msg = "You don't have any existing tables or files";
					msgDiv = `<div id='visionConversationalAI${randomNumber}' class='convai-message'>
                                   <div class='convai-right-message'>${msg}</div>
                                 </div>`;
					$('#aiLensQuickAnalyticsContentDivId').append(msgDiv);
				}
			}

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});

}
function toggleAIlensTableSelectCheckbox(tableName) {
	$(".checkDivSingleCheckBoxClass").find("input[type='checkbox']:checked").each(function(i, checkbox) {
		checkbox.checked = false;
	});
	var checkbox = $("#userAIlensAnalyticsCheckbox_" + tableName);
	checkbox.prop('checked', !checkbox.prop('checked'));
}
function getDecompositionTreeData() {

	$(".searchMainWrap").show();
	$(".languageSelectionBox").hide();
	$(".settingheaderImage").hide();
	$("#pageBodyContent").remove();
	$("#pageBody").append('<div class="page-body-content" id="pageBodyContent"><div id ="tree-container" class="decompositionTree-Visualize-page-body"></div></div></div>');
	showLoader();
	$.ajax({
		url: 'getKnowThePulseTweetDecompositionTreeBox',
		type: "POST",
		dataType: 'json',
		traditional: true,
		cache: false,
		async: true,
		data: {
			level: 0,
		},
		success: function(response) {
			stopLoader();
			if ($("#tree-container").length > 0) {
				$("#tree-container").html(response['main_div']);
			}
			gridTreeBoxes('', response['TreeBoxObj'])
		}
	});

}

function getEchartsDecompositionTreeData() {
	$(".searchMainWrap").show();
	$(".languageSelectionBox").hide();
	$(".settingheaderImage").hide();
	$("#pageBodyContent").remove();
	$("#pageBody").append('<div class="page-body-content" id="pageBodyContent"><div id ="echartsTreeContainer" class="decompositionTree-Visualize-page-body"></div></div></div>');
	showLoader();
	$.ajax({
		url: 'getEchartsDecompositionTreeData',
		type: "POST",
		dataType: 'json',
		traditional: true,
		cache: false,
		async: true,
		data: {
			level: 0,
		},
		success: function(response) {
			stopLoader();
			if (response != null && response != undefined && !jQuery.isEmptyObject(response)) {
				getDecompositionTreeEcharts(response, 'echartsTreeContainer');
			}
		},
		error: function(err) {
			console.err(err);
		}
	});
}
function getDecompositionTreeEcharts(response, treeId, count, chartType, saveType, filterCondition) {
	$("#visionVisualizeChartId" + count).remove();
	var basicAreaChartId = "visionVisualizeChart" + count;
	var basicAreaChartHomeId = "visionVisualizeChartHome" + count;
	$("#" + basicAreaChartId).append("<div id='visionVisualizeChartId" + count + "' type='" + chartType + "' count='" + count + "' class='visionVisualChartBoxClass visionVisualChartBoxSelected'></div>");
	$("#" + treeId + "_echarts_toolBox_filterCond").val(filterCondition);
	if ($("#" + treeId).parents().hasClass('homeChartWrapDiv')) {
		$('#' + treeId).parent('.visionVisualizeChartBoxClass').css('width', '820px', '!important');
		$('#' + treeId).parent('.visionVisualizeChartBoxClass').css('height', '70VH', '!important');
		$('#' + treeId).css('width', '100%', '!important');
		$('#' + treeId).css('height', '70VH', '!important');
	} else {
		$('#' + treeId).parent('.visionVisualizeChartBoxClass').css('width', '95%', '!important');
		$('#' + treeId).parent('.visionVisualizeChartBoxClass').css('height', '400px', '!important');
		$('#' + treeId).css('width', '94%', '!important');
		$('#' + treeId).css('height', '400px', '!important');
		$("#" + basicAreaChartId).css('width', '93%', '!important');
		$("#" + basicAreaChartId).css('height', '410px', '!important');
	}
	var dom = document.getElementById(treeId);
	var myChart = echarts.init(dom, null, {
		renderer: 'canvas',
		useDirtyRect: false
	});
	var chartEditoptions, bigChartType = chartType.toUpperCase(), chartTitle, chartLayout = 'orthogonal', chartOrientation = 'LR', makerSymbol = 'circle', markerColor = '#CCC', lineDash = 'line', lineColor = '#CCC', lineWidth = 1;
	if (response['chartCOnfigObjStr'] != null && response['chartCOnfigObjStr'] != undefined) {
		chartEditoptions = JSON.parse(response['chartCOnfigObjStr']);

		chartTitle = chartEditoptions[bigChartType + 'CHARTTITLE'] || chartEditoptions[bigChartType + 'CHARTTITLE' + count] || "";

		chartLayout = chartEditoptions[bigChartType + 'LAYOUT'] || chartEditoptions[bigChartType + 'LAYOUT' + count] || "orthogonal"

		chartOrientation = chartEditoptions[bigChartType + 'ORIENT'] || chartEditoptions[bigChartType + 'ORIENT' + count] || "LR";

		makerSymbol = chartEditoptions[bigChartType + 'SYMBOL'] || chartEditoptions[bigChartType + 'SYMBOL' + count] || "circle";

		markerColor = chartEditoptions[bigChartType + 'MARKERCOLORS'] || chartEditoptions[bigChartType + 'MARKERCOLORS' + count] || "#CCC";

		lineDash = chartEditoptions[bigChartType + 'LINEDASH'] || chartEditoptions[bigChartType + 'LINEDASH' + count] || "line";

		lineWidth = chartEditoptions[bigChartType + 'LINEWIDTH'] || chartEditoptions[bigChartType + 'LINEWIDTH' + count] || "1";


		lineColor = chartEditoptions[bigChartType + 'LINECOLORS'] || chartEditoptions[bigChartType + 'LINECOLORS' + count] || "#CCC";

	}
	if (lineWidth != null && lineWidth != undefined && typeof lineWidth === 'string') {
		lineWidth = parseInt(lineWidth);
	}
	if (response.hasOwnProperty('data')) {
		var data = response['data'];
		var tableName = response['tableName'];
		function setCollapsedFalse(node) {
			node.collapsed = false;
			if (node.children) {
				node.children.forEach(setCollapsedFalse);
			}
		}
		setCollapsedFalse(data);
		function formatNumber(value) {
			if (value >= 1000000000) {
				return (value / 1000000000).toFixed(1) + 'B';
			} else if (value >= 1000000) {
				return (value / 1000000).toFixed(1) + 'M';
			} else if (value >= 1000) {
				return (value / 1000).toFixed(1) + 'K';
			} else {
				return value;
			}
		}
		var tooltip = {
			trigger: 'item',
			triggerOn: 'mousemove',
			formatter: function(params) {
				var formatNumberValue = formatNumber(params.value);
				if (formatNumberValue != null && formatNumberValue != undefined && formatNumberValue != '') {
					return params.name + '-> ' + formatNumber(params.value);
				}
				return params.name;
			}
		};
		var label = {
			show: true,
			position: 'left',
			verticalAlign: 'middle',
			align: 'right',
			//fontSize: 9 + lineWidth,
			//distance: 0,
			fontSize: 12,
			distance: 10,
			rotate: 0,
			color: "black",
			formatter: function(params) {
				const labelText = params.name;
				const maxWidth = 10;
				if (labelText.length > maxWidth) {
					return labelText.substring(0, maxWidth) + '...';
				}
				return labelText;
			}
		};
		var leavesLabel = {
			position: 'right',
			verticalAlign: 'middle',
			align: 'left',
			fontSize: 12
		};
		var series = [
			{
				type: 'tree',
				data: [data],
				top: '7%',
				/*left: '7%',
				bottom: '1%',
				right: '20%',*/
				left: '10%',
				bottom: '1%',
				right: '15%',
				distance: 20,
				symbolSize: 9 + lineWidth,
				label: label,
				leaves: {
					label: leavesLabel
				},
				emphasis: {
					focus: 'descendant'
				},
				lineStyle: {
					color: lineColor,
					width: lineWidth,
					type: lineDash,
				},
				itemStyle: {
					color: markerColor,
				},
				orient: chartOrientation,
				symbol: makerSymbol,
				layout: chartLayout,
				expandAndCollapse: true,
				animationDuration: 550,
				animationDurationUpdate: 750
			}
		];
		var option = {
			title: {
				text: chartTitle,
				textStyle: {
					fontSize: 14,
					fontWeight: '500',
					color: '#0b4a99',
					fontFamily: 'segeo-ui, -apple-system, Roboto, Helvetica Neue,sans-serif',
				},
				left: 'center',
				top: '10px',
			},
			tooltip: tooltip,
			series: series
		};


		if (option && typeof option === 'object') {
			myChart.setOption(option);
			$("#" + treeId).attr("echartOption", JSON.stringify(option));

		}
		if (!(saveType != null && saveType != '' && saveType != undefined)) {
			getToolBox(treeId, chartType, tableName, "", response, count, 0);
		}
		showEchartsExpandAndModebarButtons(treeId, chartType, count, tableName, response, '');
		$(`#${treeId}_toolBox li[title^='Data on Flip']`).hide();
		$(`#${treeId}_toolBox li[title^='Chart Types']`).hide();
		$(`#${treeId}_toolBox li[title^='Change Colors']`).hide();
		handleEchartsResize(treeId);
		echartHandleDragAndDropEvent();
	}
}
function getKpiChart(response, chartId, count, chartType, saveType) {
	$("#visionVisualizeChartId" + count).remove();
	var basicAreaChartId = "visionVisualizeChart" + count;
	var basicAreaChartHomeId = "visionVisualizeChartHome" + count;
	var chartEditoptions;
	var tableName = response['tableName'];
	$("#" + basicAreaChartId).append("<div id='visionVisualizeChartId" + count + "' type='" + chartType + "' count='" + count + "' class='visionVisualChartBoxClass visionVisualChartBoxSelected kpiChartMainDiv'></div>");
	if ($("#" + chartId).parents().hasClass('homeChartWrapDiv')) {
		$('#' + chartId).parent('.visionVisualizeChartBoxClass').css('width', '820px', '!important');
		$('#' + chartId).parent('.visionVisualizeChartBoxClass').css('height', '70VH', '!important');
		$('#' + chartId).css('width', '100%', '!important');
		$('#' + chartId).css('height', '70VH', '!important');
	} else {
		$('#' + chartId).parent('.visionVisualizeChartBoxClass').css('width', '95%', '!important');
		$('#' + chartId).parent('.visionVisualizeChartBoxClass').css('height', '410px', '!important');
		$('#' + chartId).css('width', '94%', '!important');
		$('#' + chartId).css('height', '400px', '!important');

	}

	var title = $("#" + chartType.toUpperCase() + "TITLE" + count).val();
	if (response.hasOwnProperty('dataDiv')) {
		var dataDiv = response['dataDiv'];
		$("#" + chartId).html("<div id='kpiChartTitleId" + count + "' class='kpiChartTitleClass'></div>" + dataDiv);
		$('#' + chartId).find(".progress").each(function() {
			let target = $(this).data('target');
			let actual = $(this).data('actual');
			let percentage = (actual / target) * 100;
			$(this).text(percentage.toFixed(2) + '%');
			$(this).css('width', ((percentage > 100) ? 100 : percentage) + '%');

		});
		$("#" + chartId).find("#kpiChartTitleId" + count).html(title);


	}
	getToolBox(chartId, chartType, tableName, chartEditoptions, response, count);
	showEchartsExpandAndModebarButtons(chartId, chartType, count, tableName, response, '');
}

function getDecompositionTreeData() {

	$(".searchMainWrap").show();
	$(".languageSelectionBox").hide();
	$(".settingheaderImage").hide();
	$("#pageBodyContent").remove();
	$("#pageBody").append('<div class="page-body-content" id="pageBodyContent"><div id ="tree-container" class="decompositionTree-Visualize-page-body"></div></div></div>');
	showLoader();
	$.ajax({
		url: 'getKnowThePulseTweetDecompositionTreeBox',
		type: "POST",
		dataType: 'json',
		traditional: true,
		cache: false,
		async: true,
		data: {
			level: 0,
		},
		success: function(response) {
			stopLoader();
			if ($("#tree-container").length > 0) {
				$("#tree-container").html(response['main_div']);
			}
			gridTreeBoxes('', response['TreeBoxObj'])
		}
	});

}
function addOrRemoveColumnDecompositionTree(id, count, level, operation) {
	var nextLevel = parseInt(level) + 1;
	if (operation == 'ADD') {
		//var level = $('#'+id+count).attr('level');
		var r = $(`.visionVisualizeDecomposeTreeClass_${level} #${id}${count}`).siblings('button.fa-plus').hide();
		var appendDiv = `<div class='visionVisualizeDecomposeTreeClass_${nextLevel}'>
                        <span class="visionVisualizeChartValues">Level ${nextLevel}</span>
                        <div style="display:flex"><div id="${id + count}" level=${nextLevel} class="visionVisualizeChartClass ui-droppable" style="width:100%"></div>
                        <button class="fa fa-plus addOrRemoveButtonColumnDecompositionTree" onclick="addOrRemoveColumnDecompositionTree('visionVisualizeChartValuesId_','0',${nextLevel},'ADD')" fdprocessedid="hwwbs"></button>
                        <button class="fa fa-minus addOrRemoveButtonColumnDecompositionTree" onclick="addOrRemoveColumnDecompositionTree('visionVisualizeChartValuesId_','0',${nextLevel},'REMOVE')" fdprocessedid="9ic2pp"></button>
                     </div></div>`;

		$(`.visionVisualizeDecomposeTreeClass_${level}`).after(appendDiv);
	} else {
		if (operation === 'REMOVE') {
			$(`.visionVisualizeDecomposeTreeClass_${level}`).prev().find('button.fa-plus').show();
			$(`.visionVisualizeDecomposeTreeClass_${level}`).remove();
		}
	}
}


function proceedGetChart(chartType, chartId) {
	var Count = chartId.replace("visionVisualizeChartId", "");
	if (chartType != null && chartType != '' && chartType != undefined) {
		var flag = false;
		var axisId = "visionVisualizeChartAxisId_" + Count;
		var valuesId = "visionVisualizeChartValuesId_" + Count;
		var comboValuesId = "visionVisualizeChartComboValuesId_" + Count;
		var percentValuesId = "visionVisualizeChartPercentValuesId_" + Count;
		var candleValuesId = "visionVisualizeChartCandleValuesId_" + Count;
		if (chartType == 'indicator') {
			if ($.trim($("#" + valuesId).html()).length > 0) {
				flag = true;
			}
		} else if (chartType == 'pie' || chartType == 'donut' || chartType == 'bar' || chartType == 'column' ||
			chartType == 'lines' || chartType == 'scatter' || chartType == 'treemap' || chartType == 'histogram' ||
			chartType == 'funnel' || chartType == 'waterfall' || chartType == 'scatterpolar' || chartType == 'heatMap' ||
			chartType == 'sunburst' || chartType == 'geochart' || chartType == 'geoLatLangchart' || chartType == 'sankey' || chartType == 'BasicAreaChart' ||
			chartType == 'StackedAreaChart' || chartType == 'GradStackAreaChart' || chartType == 'AreaPiecesChart' || chartType == 'kpiChart' || chartType == 'kpiBarChart'
		) {
			if ($.trim($("#" + axisId).html()).length > 0 && $.trim($("#" + valuesId).html()).length > 0) {
				flag = true;
			}
		} else if (chartType == 'BarAndLine' || chartType == 'boxplot' || chartType == 'stackedBarChart') {
			if ($.trim($("#" + axisId).html()).length > 0 && $.trim($("#" + valuesId).html()).length > 0 && $.trim($("#" + comboValuesId).html()).length > 0) {
				flag = true;
			}

		}
		else if (chartType == 'ganttChart') {
			if ($.trim($("#" + axisId).html()).length > 0 && $.trim($("#" + valuesId).html()).length > 0
				&& $.trim($("#" + comboValuesId).html()).length > 0 && $.trim($("#" + percentValuesId).html()).length > 0) {
				flag = true;
			}

		}
		else if (chartType == 'candlestick') {
			if ($.trim($("#" + axisId).html()).length > 0 && $.trim($("#" + valuesId).html()).length > 0 && $.trim($("#" + comboValuesId).html()).length > 0
				&& $.trim($("#" + percentValuesId).html()).length > 0 && $.trim($("#" + candleValuesId).html()).length > 0) {
				flag = true;
			}
		} else {
			if (chartType == 'decompositionTree') {
				if ($.trim($("#" + axisId).html()).length > 0) {
					flag = true;
				}
			}
		}


		setTimeout(function() {
			if (flag) {
				getChart(chartId, Count, chartType);
			}
			else {
				stopLoader();
				showStr("Error", "Please fill all the Chart Data Columns.");
				return;
			}
		}, 2000);
	}
}


function getExpandEchartsToolBox(chartId, expandChartId, chartType, tableName, chartCOnfigObjStr, response, count, noOfDataCount) {
	var dashBoard = $('#OptionDropdownData').val();
	var tempResponse = response;
	var colorsObj = Array.isArray(response['colorsObj']) ? response['colorsObj'].join(',') : response['colorsObj'];
	var tempData = response.data;
	$("#" + chartId + "_toolBox").show();
	$("#" + chartId + "_legends").hide();
	$("#" + chartId).addClass('chartMain');
	var responseData = response['data'] ?? response['source'];
	var li = '';
	if (!chartId.includes('visionVisualizeSuggestedQueryInnerChart') && !chartId.includes('visionVisualizeModalInnerChart')) {
		li += `<li rel="tooltip" class="modebar-btn" title="Save As Image" style="padding: 4px;border-bottom: 1px solid #ddd;text-align: center;">
		    <svg viewBox="0 0 1000 1000" class="icon" height="1em" width="1em" style="fill: rgb(11, 74, 153);" onclick="saveChartAsImage('${chartId}','${chartType}')">
		        <path d="m500 450c-83 0-150-67-150-150 0-83 67-150 150-150 83 0 150 67 150 150 0 83-67 150-150 150z m400 150h-120c-16 0-34 13-39 29l-31 93c-6 15-23 28-40 28h-340c-16 0-34-13-39-28l-31-94c-6-15-23-28-40-28h-120c-55 0-100-45-100-100v-450c0-55 45-100 100-100h800c55 0 100 45 100 100v450c0 55-45 100-100 100z m-400-550c-138 0-250 112-250 250 0 138 112 250 250 250 138 0 250-112 250-250 0-138-112-250-250-250z m365 380c-19 0-35 16-35 35 0 19 16 35 35 35 19 0 35-16 35-35 0-19-16-35-35-35z" transform="matrix(1 0 0 -1 0 850)"></path>
		    </svg>
		</li>`;
		li += `<li rel="tooltip" class="modebar-btn" title="Show Data" style="padding: 4px;border-bottom: 1px solid #ddd;text-align: center;">
		    <svg viewBox="0 0 448 512" class="icon" height="1em" width="1em" style="fill: rgb(11, 74, 153);" onclick="getGridData('','','${chartId}','','${tableName}','${expandChartId}')">
		        <path d='M448 32C483.3 32 512 60.65 512 96V416C512 451.3 483.3 480 448 480H64C28.65 480 0 451.3 0 416V96C0 60.65 28.65 32 64 32H448zM152 96H64V160H152V96zM208 160H296V96H208V160zM448 96H360V160H448V96zM64 288H152V224H64V288zM296 224H208V288H296V224zM360 288H448V224H360V288zM152 352H64V416H152V352zM208 416H296V352H208V416zM448 352H360V416H448V352z'></path>
		    </svg>
		</li>

		<li rel="tooltip" class="modebar-btn" title="Filter Chart" style="padding: 4px;border-bottom: 1px solid #ddd;text-align: center;">
		    <svg viewBox="0 0 448 512" class="icon" height="1em" width="1em" style="fill: rgb(11, 74, 153);" onclick="getfilterData('${chartId}','${tableName}','${chartType}','${expandChartId}')">
		        <path d='M3.853 54.87C10.47 40.9 24.54 32 40 32H472C487.5 32 501.5 40.9 508.1 54.87C514.8 68.84 512.7 85.37 502.1 97.33L320 320.9V448C320 460.1 313.2 471.2 302.3 476.6C291.5 482 278.5 480.9 268.8 473.6L204.8 425.6C196.7 419.6 192 410.1 192 400V320.9L9.042 97.33C-.745 85.37-2.765 68.84 3.854 54.87L3.853 54.87z'></path>
		    </svg>
		</li>`;

		li += `<li rel="tooltip" class="modebar-btn" title="Edit Chart" style="padding: 4px;border-bottom: 1px solid #ddd;text-align: center;">
    <svg viewBox="0 0 448 512" class="icon" height="1em" width="1em" style="fill: rgb(11, 74, 153);" onclick="homePageChartSetting('${chartId}','${chartType}','','','${count}','','${expandChartId}','${chartCOnfigObjStr}')">
		<path d='M490.3 40.4C512.2 62.27 512.2 97.73 490.3 119.6L460.3 149.7L362.3 51.72L392.4 21.66C414.3-.2135 449.7-.2135 471.6 21.66L490.3 40.4zM172.4 241.7L339.7 74.34L437.7 172.3L270.3 339.6C264.2 345.8 256.7 350.4 248.4 353.2L159.6 382.8C150.1 385.6 141.5 383.4 135 376.1C128.6 370.5 126.4 361 129.2 352.4L158.8 263.6C161.6 255.3 166.2 247.8 172.4 241.7V241.7zM192 63.1C209.7 63.1 224 78.33 224 95.1C224 113.7 209.7 127.1 192 127.1H96C78.33 127.1 64 142.3 64 159.1V416C64 433.7 78.33 448 96 448H352C369.7 448 384 433.7 384 416V319.1C384 302.3 398.3 287.1 416 287.1C433.7 287.1 448 302.3 448 319.1V416C448 469 405 512 352 512H96C42.98 512 0 469 0 416V159.1C0 106.1 42.98 63.1 96 63.1H192z'></path>"
		</svg></li> `;

	}

	$("#" + expandChartId + "_toolBox ul").html(li);
}

function handleGooglechartsResize(chartId, data, options, chartType, title) {
	$("#" + chartId).parent().resizable();
	$("#" + chartId).parent().resize(function(event, ui) {
		var dom = document.getElementById(chartId);
		var width = ui.size.width;
		var height = ui.size.height;

		var parentId = $("#" + chartId).parents('.homeChartWrapDiv').attr("id");
		$("#" + chartId).css("width", "100%", "!important");
		$("#" + chartId).css("height", "100%", "!important");
		$("#" + parentId).attr("class", "homeChartWrapDiv resizableChartDiv");
		var chart;
		if (chartType != null && chartType != undefined && chartType != '' && chartType === 'geochart') {
			chart = new google.visualization.GeoChart(document.getElementById(chartId));
		} else {
			if (chartType != null && chartType != undefined && chartType != '' && chartType === 'ganttChart') {
				chart = new google.visualization.Gantt(document.getElementById(chartId));
			}
		}
		chart.draw(data, Object.assign({}, options, {
			width: width,
			height: height
		}));

		var $titleElement = $(`<h1 id='geoChartTittleH1' class='geoChartTittleClass'></h1>`);
		$titleElement.text(title);
		$titleElement.css({
			'font-size': '12px',
			'font-weight': 'bold',
			'font-weight': '600',
			'color': '#0b4a99',
			'font-family': 'segeo-ui, -apple-system, Roboto, Helvetica Neue, sans-serif'
		});

		var $chartContainer = $('#' + chartId);
		$chartContainer.prepend($titleElement);
	});
}


function showDrillDownLevelCharts(axix, chartId, chartType, paramObj, createCount) {
	$.ajax({
		type: "POST",
		url: 'dashboardSetting',
		cache: false,
		dataType: 'json',
		data: {
			type: chartType,
			id: chartId
		},
		success: function(response) {
			chartFilterConfigObj = response['jsonChartFilterObj'];
			var chartfilterData = response['filtercolumn'];
			var dataarr = chartfilterData['dataarr'];
			if (dataarr !== null && !jQuery.isEmptyObject(dataarr)) {
				var values = dataarr[0]['yAxix'];
				var type = dataarr[0]['type'];
				var table = dataarr[0]['table'];
				var id = dataarr[0]['chartid'];
				var Lebel = dataarr[0]['Lebel'];
				var aggColumnName = dataarr[0]['aggColumnName'];
				var filterColumns = dataarr[0]['filterCondition'];
				var chartPropObj = dataarr[0]['chartPropObj'];
				var chartConfigObj = dataarr[0]['chartConfigObj'];
				var labelLegend = dataarr[0]['labelLegend'];
				var colorsObj = dataarr[0]['colorsObj'];
				var chartConfigToggleStatus = dataarr[0]['chartConfigToggleStatus'];
				var homeFilterColumn = dataarr[0]['homeFilterColumn'];
				var fetchQuery = dataarr[0]['fetchQuery'];
				var radioButtons = dataarr[0]['radioButtons'];
				var comboValue = dataarr[0]['comboValue'];
				var zAxisColumn = dataarr[0]['zAxisColumn'];
				var candleColumn = dataarr[0]['candleColumn'];
				var currencyConversionStrObject = dataarr[0]['currencyConversionStrObject'];
				var query = dataarr[0]['query'];
				var paramArray = [];
				var drilldownFilterArr = drilldownChartFilters[chartId];
				if (drilldownFilterArr != null && !jQuery.isEmptyObject(drilldownFilterArr)) {
					$.each(drilldownFilterArr, function(index, val) {
						paramArray.push(val);
					});
				}
				paramArray.push(paramObj);
				drilldownChartFilters[chartId] = paramArray;
				getVisualizeChart(id, type, axix, values, table, aggColumnName, filterColumns, chartPropObj, chartConfigObj, createCount, labelLegend, colorsObj, comboValue, chartConfigToggleStatus, "", "", fetchQuery, radioButtons, currencyConversionStrObject, zAxisColumn, " ", candleColumn);
				closeAllDialogsBoxes();
			}
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}


function updateChartDrilldownColumnsData(items, chartId) {
	var dashbordName;
	var item = $("#OptionDropdownData").jqxDropDownList('getSelectedItem');
	if (item != null) {
		dashbordName = item.value;
	}
	$.ajax({
		type: "POST",
		url: "updteDrillDownColumns",
		cache: false,
		dataType: 'html',
		async: false,
		data: {
			columndata: JSON.stringify(items),
			dashbordName: dashbordName,
			chartId: chartId
		},
		success: function(response) {
			if (response != null) {
				var modalObj = {
					title: 'Message',
					body: response
				};
				var buttonArray = [
					{
						text: 'Close',
						click: function() {
							$("#" + chartId + "_drillDownColumns").val(items);
							$("#dataDxpSplitterValue .close").click();
						},
						isCloseButton: true
					}
				];
				modalObj['buttons'] = buttonArray;
				createModal("dataDxpSplitterValue", modalObj);
				$(".modal-dialog").addClass("opacity-animate3");

			}
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}

function getHomePageVisualizeHeaderFilters(dashBoardName) {
	$.ajax({
		type: "POST",
		url: 'homePageVisualizeHeaderFilters',
		cache: false,
		dataType: 'json',
		data: {
			type: "FILTER",
			dashBoardName: dashBoardName
		},
		success: function(response) {
			if (response != null && !jQuery.isEmptyObject(response)) {
				var dataarr = response['dataarr'];
				if (dataarr !== null && !jQuery.isEmptyObject(dataarr)) {
					var values = dataarr[0]['yAxix'];
					var type = dataarr[0]['type'];
					var table = dataarr[0]['table'];
					var id = dataarr[0]['chartid'];
					var Lebel = dataarr[0]['Lebel'];
					var aggColumnName = dataarr[0]['aggColumnName'];
					var filterColumns = dataarr[0]['filterCondition'];
					var chartPropObj = dataarr[0]['chartPropObj'];
					var chartConfigObj = dataarr[0]['chartConfigObj'];
					var labelLegend = dataarr[0]['labelLegend'];
					var colorsObj = dataarr[0]['colorsObj'];
					var chartConfigToggleStatus = dataarr[0]['chartConfigToggleStatus'];
					var homeFilterColumn = dataarr[0]['homeFilterColumn'];
					var fetchQuery = dataarr[0]['fetchQuery'];
					var radioButtons = dataarr[0]['radioButtons'];
					var comboValue = dataarr[0]['comboValue'];
					var zAxisColumn = dataarr[0]['zAxisColumn'];
					var candleColumn = dataarr[0]['candleColumn'];
					var currencyConversionStrObject = dataarr[0]['currencyConversionStrObject'];
					var query = dataarr[0]['query'];
					$("#visionDashBoardHomeFilterId").html("");
					createFilterHeader('visionDashBoardHomeFilterId', type, homeFilterColumn, dashBoardName);
				}
			}
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}
function refreshVisualizationchart(dashBoardName, chartId, chartType, count) {
	$.ajax({
		type: "POST",
		url: 'refreshVisualizationchart',
		cache: false,
		dataType: 'json',
		data: {
			type: chartType,
			id: chartId,
			dashBoardName: dashBoardName
		},
		success: function(response) {
			if (response != null && !jQuery.isEmptyObject(response)) {
				var dataarr = response['dataarr'];
				if (dataarr !== null && !jQuery.isEmptyObject(dataarr)) {
					var XAxix = dataarr[i]['xAxix'];
					var yAxix = dataarr[i]['yAxix'];
					var type = dataarr[0]['type'];
					var table = dataarr[0]['table'];
					var id = dataarr[0]['chartid'];
					var Lebel = dataarr[0]['Lebel'];
					var aggColumnName = dataarr[0]['aggColumnName'];
					var filterColumns = dataarr[0]['filterCondition'];
					var chartPropObj = dataarr[0]['chartPropObj'];
					var chartConfigObj = dataarr[0]['chartConfigObj'];
					var labelLegend = dataarr[0]['labelLegend'];
					var colorsObj = dataarr[0]['colorsObj'];
					var chartConfigToggleStatus = dataarr[0]['chartConfigToggleStatus'];
					var homeFilterColumn = dataarr[0]['homeFilterColumn'];
					var fetchQuery = dataarr[0]['fetchQuery'];
					var radioButtons = dataarr[0]['radioButtons'];
					var comboValue = dataarr[0]['comboValue'];
					var zAxisColumn = dataarr[0]['zAxisColumn'];
					var candleColumn = dataarr[0]['candleColumn'];
					var currencyConversionStrObject = dataarr[0]['currencyConversionStrObject'];
					var query = dataarr[0]['query'];
					var resizeData = dataarr[i]['resizeData'];
					var chartSeqNo = dataarr[i]['chartSeqNo'];
					var drillDownColumns = dataarr[i]['drillDownColumns'];
					//$("#visionVisualizeChartHome" + count).remove();
					$("#" + chartid + "_filter").val("");
					$("#" + chartid + "_homePageFilter").val("");
					$("#" + chartid + "_chartFilter").val("");
					var chartid = id;
					var divClass = "col-md-6 col-sm-6 col-lg-3";
					if (type == 'decompositionTree') {
						divClass = "col-md-6 col-sm-6 col-lg-12";
					}
					/*else {
					   divClass = "col-md-6 col-sm-6 col-lg-3";
				   }*/
					var chartDivId = `
                                <div class='${divClass} homeChartWrapDiv' chart-level='0' id='visionVisualizeChartHome${count}'>
                                    <div class='flip-chart' id='chart-container${count}'>
                                        <div class='flip-chart-inner' id='chart-card${count}'>
                                            <div class='flip-chart-front' id='chart-front-id${count}'>
                                                <div id='homeChartParentDiv${count}' class='homeChartParentDiv'>
                                                    <div class='chartMain' id='${chartid}'></div>
                                                    <button class='${chartid}_PrevDataLeftCls visionVisualizeChartHomePrevDataLeft' style='display:none'><i class='fa fa-arrow-left'></i></button>
                                    				<button class='${chartid}_NextDataRightCls visionVisualizeChartHomeNextDataRight' style='display:none'><i class='fa fa-arrow-right'></i></button>
                                                      <div class="controlMenuBtnClass" id="controlMenuBtnClassid${chartid}">
												        <div class="modbarMaximizeClass" id='${chartid}_ExpandOnclick' onclick="expandChart()">
												            <img src="images/modbar-maximize.png" alt="maximize" class="maximizeBlueIcon">
												            <img src="images/modbar-maximize-white.png" alt="maximize" class="maximizeWhiteIcon">
												        </div>
												        <i class="fa fa-ellipsis-h" aria-hidden="true" onclick = showModebarContainer('${chartid}')></i>
												    </div>
                                                    <div id='${id}_traces' class='legendTraces'></div>
                                                    <div class='rightControls'>
                                                        <div class='iconDiv'><img src='images/Plus-Icon-02.svg' class='visionVisualizeHorizontalDotsClass visionVisualChartBoxSelected'></div>
                                                        <div class='iconDiv'><img src='images/FeedBack_Icon.svg' class='visionVisualizeHorizontalDotsClass visionVisualChartBoxSelected'></div>
                                                        <div class='iconDiv'><img src='images/Settings_Icon.svg' class='visionVisualizeHorizontalDotsClass visionVisualChartBoxSelected'></div>
                                                        <div class='iconDiv'><img src='images/Filter.svg' class='visionVisualizeHorizontalDotsClass visionVisualChartBoxSelected'></div>
                                                        <div class='iconDiv'><img src='images/search_blue.png' class='visionVisualizeHorizontalDotsClass visionVisualChartBoxSelected'></div>
                                                    </div>
                                                </div>
                                            </div>
                                            <div class='flip-chart-back' id='chart-back-id${count}'></div>
                                        </div>
                                    </div>
                                    <div>
                                        <div class='chartDialogClass' id='chartDialog${count}' style='display: none;'></div>
                                        <div class='createpopupClass' id='homepagecreatepopupId${count}' style='display: none;'></div>
                                        <input type='hidden' id='${chartid}_filter' value=''/>
                                        <input type='hidden' id='${chartid}_homePageFilter' value=''/>
                                        <input type='hidden' id='${chartid}_chartFilter' value=''/>
                                        <input type='hidden' id='${chartid}_startIndex' value='0'/>
                                        <input type='hidden' id='${chartid}_endIndex' value='10'/>
                                        <input type='hidden' id='${chartid}_pageSize' value='10'/>
                                        <input type='hidden' id='${chartid}_TotalChartCount' value='0'/>
                                        <input type='hidden' id='${chartid}_options' value=''/>
                                        <input type='hidden' id='${chartid}_chartType' value='${type}'/>
                                        <input type='hidden' id='${chartid}_count' value='${count}'/>
                                        <input type='hidden' id='${chartid}_dynamic_XAxisLength' value=''/>
                                        <input type='hidden' id='${chartid}_dynamic_YAxisLength' value=''/>
                                        <input type='hidden' id='${chartid}_resizeData' value='${resizeData}'/>
                                        <input type='hidden' id='${chartid}_dynamic_comboColLength' value=''/>
                                        <input type='hidden' id='${chartid}_dynamic_parentColLength' value=''/>
                                        <input type='hidden' id='${chartid}_echarts_toolBox_filterCond' value=''/>
                                        <input type='hidden' id='${chartid}_drillDownColumns' value='${drillDownColumns}'/>
                                        <input type='hidden' id='${chartid}_drillDownColumnsFlag' value=''/>
                                        <input type='hidden' id='${chartid}_drillDownColumnsAxisColumn' value=''/>
                                        <input type='hidden' id='${chartid}_chartSeqNo' value='${chartSeqNo}'/>`;
					//if (type != null && type != '' && type != undefined && eChartsArrList.indexOf(type) > -1) {
					chartDivId += `
                            			            <div id='${chartid}_toolBox' class='iconsDiv' style='position: absolute;top: 2px;right: 3px;height: 99%; background: #f1f1f1;height: 300px;overflow-y:auto'><ul style='height:100%'></ul></div>
                            			            <div id='${chartid}_radioButtons' class='visionVisualizeRadioButtonsClass'></div>`;
					//}

					chartDivId += `
                            			        </div>
                            			    </div>`;
					/*var afterDivCount = parseInt(count) + 1;
					if ($("#visionVisualizeChartHome" + afterDivCount).length > 0) {
						$(chartDivId).insertBefore($("#visionVisualizeChartHome" + afterDivCount));
					} else {
						var beforeDivCount;
						if(count == "0")
						{
						  beforeDivCount = parseInt(count);
						  $("#visualizechartId").prepend($(chartDivId));
						  
						}else{
						  beforeDivCount = parseInt(count) - 1;
						  $(chartDivId).insertAfter($("#visionVisualizeChartHome" + beforeDivCount));
						}
						
					}*/

					if (type != null && type != '' && type != undefined && (type == 'donut' || type == 'pie')) {
						// $("#" + id + "_traces").html("<div id='" + id + "_legendId'></div>")
					}
					if (query != null && query != '' && query != undefined) {
						getQueryVisualizeChart(chartid, type, filterColumns, chartPropObj, chartConfigObj, count, labelLegend, colorsObj, comboValue, chartConfigToggleStatus, "", "", fetchQuery, radioButtons, currencyConversionStrObject, query, table);
					} else {
						getVisualizeChart(chartid, type, XAxix, yAxix, table, aggColumnName, filterColumns, chartPropObj, chartConfigObj, count, labelLegend, colorsObj, comboValue, chartConfigToggleStatus, "", "", fetchQuery, radioButtons, currencyConversionStrObject, zAxisColumn, "", candleColumn);
					}


				}
			}
		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}




function removeDuplicates(checkBoxDataArr, tablename, exiFlag) {
	var gridId = "";
	if (exiFlag == "Y") {
		gridId = "divGrid-" + tablename;
	} else {
		gridId = "visualizeAreaGirdData1";
	}
	var columnDefinitions = $('#' + gridId).jqxGrid('getInstance').columns.records;

	// Iterate over the column definitions and extract the names, removing null and empty string values
	var columnNames = [];
	for (var i = 1; i < columnDefinitions.length; i++) {
		var columnName = columnDefinitions[i].datafield;
		if (columnName !== null && columnName.trim() !== '') {
			columnNames.push(columnName);
		}
	}
	checkBoxDataArr = columnNames;

	$("#visionVisualizationDataChartcount").hide();
	closeDialogBox("#dialog");
	var htmlText = "<div class='mainRemoveDuplicatesDialogDivId'>"
		+ "<div id = 'tablecolumnId' class = 'tablecolumnClass'></div>"
		+ "<span class='mainRemoveDuplicatesDialogSpanClass'></span>"
		+ "</div>";
	$("#dialog").html(htmlText);
	$("#tablecolumnId").jqxListBox({
		filterable: true,
		checkboxes: true,
		source: checkBoxDataArr,
		searchMode: "containsignorecase",
		theme: "energyblue",
		displayMember: "text",
		valueMember: "value",
		width: "310px",
	});
	$("#dialog").dialog({
		modal: true,
		title:
			labelObject["Please select column(s) for Remove duplicates"] != null
				? labelObject["Please select column(s) for Remove duplicates"]
				: "Please select column(s) for Remove duplicates",
		height: "auto",
		minHeight: "auto",
		minWidth: 350,
		maxWidth: "auto",
		fluid: true,
		buttons: [
			{
				text: labelObject["Show Duplicate Data"] != null ? labelObject["Show Duplicate Data"] : "Show Duplicate Data",
				click: function() {
					var checkValues =
						$("#tablecolumnId").jqxListBox("getCheckedItems");
					var values = [];
					$.each(checkValues, function(index) {
						values.push(this.value);
					});
					if (values != null && !jQuery.isEmptyObject(values)) {
						values = values.toString();
					}
					if (values != null && !jQuery.isEmptyObject(values)) {
						$(".mainRemoveDuplicatesDialogSpanClass").text("");
						$(this).html("");
						$(this).dialog("close");
						$(this).dialog("destroy");
						$.ajax({
							type: "post",
							traditional: true,
							dataType: "json",
							cache: false,
							url: "showDuplicateData",
							data: {
								tableName: tablename,
								columnName: values,
								gridId: gridId,
								totalColumns: JSON.stringify(checkBoxDataArr)

							},
							success: function(response) {
								if (response != null) {
									var dataFieldsArray = response['dataFieldsArray'];
									var columnsArray = response['columnsArray'];
									var tableId = "ivisualizationGridDataDiv";

									try {
										$("#" + tableId).jqxGrid("destroy");
										$("#" + tableId).remove();
									} catch (e) {
									}
									$("#dialog").html("<div id = '" + tableId + "' class = 'ivisualizationGridDataDivClass'></div>");
									$("#dialog").dialog({
										title:
											labelObject["Duplicate Data"] != null
												? labelObject["Duplicate Data"]
												: "Duplicate Data",
										modal: true,
										width: 1200,
										height: 600,
										fluid: true,
										buttons: [
											{
												text:
													labelObject["Remove Duplicates"] != null
														? labelObject["Remove Duplicates"]
														: "Remove Duplicates",
												click: function() {
													$(this).html("");
													$(this).dialog("close");
													$(this).dialog("destroy");
													removeDuplicateData(tablename, values, gridId, checkBoxDataArr);
												},
											},
											{
												text:
													labelObject["Cancel"] != null
														? labelObject["Cancel"]
														: "Cancel",
												click: function() {
													$(this).html("");
													$(this).dialog("close");
													$(this).dialog("destroy");
												}
											}
										],
										open: function() {

											setTimeout(function() {
												var source =
												{
													type: 'POST',
													datatype: "json",
													datafields: dataFieldsArray,
													data: {
														tableName: tablename,
														columnName: values,
														gridId: gridId,
														totalColumns: JSON.stringify(checkBoxDataArr)

													},
													url: 'showDuplicatesObjectData',
													cache: false,
													root: 'Rows',
													processdata: function(data) {
														showLoader();
														data['getOnlyDataArray'] = 'Y';

													},
													beforeSend: function() {
														//showLoader();

													}, loadError: function(xhr, status, error) {
														stopLoader();
													}, loadComplete: function(data) {
														stopLoader();
													},
													beforeprocessing: function(data) {
														source.totalrecords = data[data.length - 1];
													},
													sort: function() {
														$("[id='" + tableId + "']").jqxGrid('updatebounddata', 'sort');
														try {
															$("[id='" + tableId + "']").jqxGrid('clearselection');
														} catch (e) {
														}

													},
													filter: function() {

														$("[id='" + tableId + "']").jqxGrid('updatebounddata', 'filter');
														try {
															$("[id='" + tableId + "']").jqxGrid('clearselection');
														} catch (e) {
														}

													}
												};

												var dataAdapter = new $.jqx.dataAdapter(source);



												$("[id='" + tableId + "']").jqxGrid(
													{
														width: "100%",
														height: "90%",
														theme: 'energyblue',
														autoshowloadelement: false,
														source: dataAdapter,
														pageable: true,
														pagesize: 50,
														showfilterrow: true,
														filterable: true,
														sortable: true,
														virtualmode: true,
														pagesizeoptions: ['50', '100', '500'],
														rendergridrows: function(params) {
															return params.data;
														},
														columnsresize: true,
														columns: columnsArray,
														showtoolbar: true,
														rendertoolbar: function(statusbar) {
															var container = $("<div style='overflow: hidden; position: relative; margin: 5px;'></div>");
															var addButton = $("<div style='float: left; margin-left: 5px;'><img style='position: relative; margin-top: 5px;height: 20px' src='images/Pivot Descriptor-Icon.png'/></div>");
															container.append(addButton);
															statusbar.append(container);


															addButton.click(function(event) {
																getCrossTabData(tableName);
															});
														}
													});
											}, 1000);

											$(this)
												.closest(".ui-dialog")
												.find(".ui-button")
												.eq(1)
												.addClass("dialogyes");
											$(".visionHeaderMain").css("z-index", "999");
											$(".visionFooterMain").css("z-index", "999");
										},
										beforeClose: function(event, ui) {
											$(".visionHeaderMain").css("z-index", "99999");
											$(".visionFooterMain").css("z-index", "99999");
										},
									});
								}
							},
							error: function(e) {
								sessionTimeout(e);
							},
						});
					} else {
						$(".mainRemoveDuplicatesDialogSpanClass").text("Please select columns to proceed");
					}
				},
			},
			{
				text: labelObject["Remove Duplicate Data"] != null ? labelObject["Remove Duplicate Data"] : "Remove Duplicate Data",
				click: function() {
					var checkValues =
						$("#tablecolumnId").jqxListBox("getCheckedItems");
					var values = [];
					$.each(checkValues, function(index) {
						values.push(this.value);
					});
					if (values != null && !jQuery.isEmptyObject(values)) {
						values = values.toString();
					}
					if (values != null && !jQuery.isEmptyObject(values)) {
						$(".mainRemoveDuplicatesDialogSpanClass").text("");
						$(this).html("");
						$(this).dialog("close");
						$(this).dialog("destroy");
						removeDuplicateData(tablename, values, gridId, checkBoxDataArr);
					} else {
						$(".mainRemoveDuplicatesDialogSpanClass").text("Please select columns to proceed");
					}
				}
			}
		],
		open: function() {
			$(this)
				.closest(".ui-dialog")
				.find(".ui-button")
				.eq(1)
				.addClass("dialogyes");
			$(this)
				.closest(".ui-dialog")
				.find(".ui-button")
				.eq(2)
				.addClass("dialogno");
			$(this).closest(".ui-dialog").addClass("visionCommonDialog");
			$(".visionHeaderMain").css("z-index", "999");
			$(".visionFooterMain").css("z-index", "999");
		},
		beforeClose: function(event, ui) {
			$(".visionHeaderMain").css("z-index", "99999");
			$(".visionFooterMain").css("z-index", "99999");
		},
	});

}


function removeDuplicateData(tablename, values, gridId, checkBoxDataArr) {
	$.ajax({
		type: "post",
		traditional: true,
		dataType: "json",
		cache: false,
		url: "removeDuplicateData",
		data: {
			tableName: tablename,
			columnName: values,
			gridId: gridId,
			totalColumns: JSON.stringify(checkBoxDataArr)

		},
		success: function(response) {
			if (response != null) {
				var message = response["Message"];
				$("#dialog").html(message);
				$("#dialog").dialog({
					title:
						labelObject["Message"] != null
							? labelObject["Message"]
							: "Message",
					modal: true,
					width: 300,
					height: 135,
					fluid: true,
					buttons: [
						{
							text:
								labelObject["Ok"] != null
									? labelObject["Ok"]
									: "Ok",
							click: function() {
								if (gridId.startsWith("divGrid")) {
									var data = {
										startIndex: 0,
										endIndex: 50,
										tableName: tablename,
										analytics: "Y",
									};
									viewAnalyticsTableDataGrid(data);
									removeIiWidgetOverlay();
								} else {
									showalterColumnData(tablename, "N", gridId);
									showFileToDxpTableData(
										tablename,
										exiFlag,
										gridId
									);
									removeIiWidgetOverlay();
									leftFileUploadsDivToggle();
								}
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
							},
						},
					],
					open: function() {
						$(this)
							.closest(".ui-dialog")
							.find(".ui-button")
							.eq(1)
							.addClass("dialogyes");
						$(".visionHeaderMain").css("z-index", "999");
						$(".visionFooterMain").css("z-index", "999");
					},
					beforeClose: function(event, ui) {
						$(".visionHeaderMain").css("z-index", "99999");
						$(".visionFooterMain").css("z-index", "99999");
					},
				});
			}
		},
		error: function(e) {
			sessionTimeout(e);
		},
	});
}


function refreshIntegralGrid(tableName) {
	if (tableName != null && tableName != '' && tableName != undefined) {
		tableName = tableName.replace("divGrid-", "");
	}
	var data = {
		startIndex: 0,
		endIndex: 50,
		tableName: tableName,
		analytics: "Y"
	};
	viewAnalyticsTableDataGrid(data);
}

function showDataBaseTablesData(event) {
	var keycode = (event.keyCode ? event.keyCode : event.which);
	if (keycode == '13') {
		getFilteredSchemaObject();
	}
}


function getAILensAnalyticsInsights() {
	var tablesList = $("#userAIlensAnalyticsExistTableNamesDivId").find('input:checked').map(function() {
		return $(this).val();
	}).get();
	if (tablesList.length == 0) {
		showAnalyticsMsg("Error", "Please select at least one table", "noTableSelectedInAILensAnalaytics");
		return;
	}
	var tableName = tablesList.join(',');
	var randomNumber = generateRandomNumber();
	var msgDiv = `<div id='visionConversationalAI${randomNumber}' class='convai-message'>
                                     <div class='convai-left-message'>${tableName}</div>
                                     </div>`;
	$('#aiLensQuickAnalyticsContentDivId').append(msgDiv);
	var userName = $("#rsUserName").val();
	var [lang, sessionId] = getChatbotParams();
	showaiLoader();
	aiAutoScrollContainer();


	$.ajax({
		type: "POST",
		url: "getAILensInsightsAnalyticsInsights",
		cache: false,
		data: {
			userName: userName,
			sessionId: sessionId,
			tableName: tableName
		},
		success: function(response) {
			//TODO: Need to add Loader
			stopaiLoader();
			if (response != null && !jQuery.isEmptyObject(response)) {
				var questionsDivStr = response['questionsDivStr'];
				var responseId = response['response_id'];
				var textData = response['textData'];
				if (!($("#leftFileUploadMainDivwrapperID").length)) {
					loadVisuvalization();
				}
				//$("#visionChartsAutoSuggestionUserId1").html(questionsDivStr);
				setTimeout(function() {
					$("#visionChartsAutoSuggestionUserId1").html(questionsDivStr);
					aiAutoScrollContainer();
					switchSmartBiDesignTabs('li_autoSuggestionsView', 'visionChartAutoSuggestionsViewId');
					$("#data-quickInsightsAnalyticsInsightsDescriptions").html("<div id='subtitlesId' class='subtitlesClass'></div>");
				}, 2000);

				showSelectedDataOnPopup(responseId, textData);



			}

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopaiLoader();
			stopLoader();
		}
	});

	//showSelectedDataOnPopup("26CC0708E0892BAA29");


}
var myAudioInterval;
function showSelectedDataOnPopup(responseId, textData) {
	showaiLoader();
	var number = (Math.random() + ' ').substring(2, 10) + (Math.random() + ' ').substring(2, 10);
	var audioFrame = "<audio style='display:none' id='myAudio" + number + "' controls>"
		+ "<source id='audioSource" + number + "' src='' type='audio/mp3'>"
		+ "</audio>";
	$("#aiLensQuickAnalyticsContentDivId").append(audioFrame);
	$("#audioSource" + number).attr('src', 'getAILensAnalyticsInsightsAudio?responseId=' + responseId);
	setTimeout(function() {
		myAudioInterval = setInterval(callAudio(textData, number), 5000);
	}, 10000);



}
function callAudio(textData, number) {
	stopaiLoader();
	var audio = $("#audioSource" + number).attr('src')
	var subtitles = document.getElementById("subtitlesId");
	if (audio != null) {
		aiAutoScrollContainer();
		$("#myAudio" + number).show();
		$("#myAudio" + number)[0].play();
		var audioFile = $('#audioSource' + number)[0];
		var audioElement = $('#myAudio' + number)[0];
		// Get the audio element

		// Event listener for when the audio starts playing
		$(audioFile).on('play', function() {
			console.log('Audio has started playing.');
			$("#myAudio" + number)[0].play();
		});

		// Event listener for when the audio is paused
		$(audioFile).on('pause', function() {
			console.log('Audio has been paused.');
			$("#myAudio" + number)[0].pause();
		});
		clearInterval(myAudioInterval);

		if (textData != null && textData != "" && textData != undefined) {
			var voiceToText = JSON.parse(textData);
			var syncData = voiceToText['summary_timestamp'];

			// Event listener for time updates
			var lastDisplayedText = ""; // To track the last displayed text

			audioElement.addEventListener('timeupdate', function() {
				var currentTime = audioElement.currentTime;
				var foundText = false; // To check if any text is found within the time range
				var displayedText = ""; // Will hold the current text being displayed

				syncData.forEach(function(element) {
					// Check if the current time is within the start and end time range of this element
					if (currentTime >= element.start && currentTime <= element.end) {
						displayedText = element.text; // Set the text for the current time segment
						foundText = true;
					}
				});

				// Update the subtitles only if a new word is found
				if (foundText && displayedText !== lastDisplayedText) {
					// Append the current text with a space if subtitles already have text					
					subtitles.innerText += " " + displayedText; // Append the current text
					lastDisplayedText = displayedText; // Update the last displayed text to avoid duplication
				}
			});
		}

	}
}

var showAudioFileForAnalyticscallAudioInterval;
function showAudioFileForAnalytics(responseId) {
	showaiLoader();
	var number = (Math.random() + ' ').substring(2, 10) + (Math.random() + ' ').substring(2, 10);
	var audioFrame = "<audio style='display:none' id='myAudio" + number + "' controls>"
		+ "<source id='audioSource" + number + "' src='' type='audio/mp3'>"
		+ "</audio>";
	$("#aiLensQuickAnalyticsContentDivId").append(audioFrame);
	$("#audioSource" + number).attr('src', 'getAILensAnalyticsInsightsAudio?responseId=' + responseId);
	setTimeout(function() {
		myAudioInterval = setInterval(showAudioFileForAnalyticscallAudio(number), 5000);
	}, 10000);



}
function showAudioFileForAnalyticscallAudio(number) {
	stopaiLoader();
	var audio = $("#audioSource" + number).attr('src')
	if (audio != null) {
		aiAutoScrollContainer();
		//$("#myAudio"+number).show();
		$("#myAudio" + number)[0].play();
		var audioFile = $('#audioSource' + number)[0]; // Get the audio element

		// Event listener for when the audio starts playing
		$(audioFile).on('play', function() {
			console.log('Audio has started playing.');
			$("#myAudio" + number)[0].play();
		});

		// Event listener for when the audio is paused
		$(audioFile).on('pause', function() {
			console.log('Audio has been paused.');
			$("#myAudio" + number)[0].pause();
		});
		clearInterval(showAudioFileForAnalyticscallAudioInterval);
	}
}

function sendPythonPdfData() {
	var type = 'FILE';
	var typeName = 'PDF';
	if (type != null && type != "" && type != undefined && type == "FILE") {
		var response = "<div id ='visualizationDMFileId' class ='visualizationDMPDFFileDivClass'>"
			+ "<div id='visionShowFileUploadMsg'></div>";
		response += "<input type='file' name='importVisualizationDMPDFFile'  id='importVisualizationDMPDFFile' class='visionVisualizationDMFilesInput'/>";
		response += "<div class='visionVisualizationDMFileUploadclass' id='visionVisualizationDmPDFFileUpload'>";
		response += "<input type='hidden' id='selectedTreeTypeName' value=''>";
		response += "<input type='hidden' id='selectedTreeType' value=''>";

		if (typeName == 'PDF') {
			response += "<div id = 'imageDiv' class='imageDivClass'>"
			response += "<img src='images/pdficon.png'  id='pdfImageId' class='importFileClass'>";
			response += "</div>";
			response += "<div class='VisionVisualizationUploadFileContent'><h5>Import Data From PDF</h5></div>";

		} else if (typeName == 'JSON') {
			response += "<div id = 'imageDiv' class='imageDivClass'>"
			response += "<img src='images/JSON_Icon.svg'  id='jsonImageId' class='importFileClass'>";
			response += "</div>";
			response += "<div class='VisionVisualizationUploadFileContent'><h5>Import Data From JSON</h5></div>";

		} else if (typeName == 'XML') {
			response += "<div id = 'imageDiv' class='imageDivClass'>"
			response += "<img src='images/XML-Icon.svg'  id='xmlImageId' class='importFileClass'>";
			response += "</div>";
			response += "<div class='VisionVisualizationUploadFileContent'><h5>Import Data From XML</h5></div>";

		} else if (typeName == 'CSV') {
			response += "<div id = 'imageDiv' class='imageDivClass'>"
			response += "<img src='images/CSV-Icon.svg'  id='csvImageId' class='importFileClass'>";
			response += "</div>";
			response += "<div class='VisionVisualizationUploadFileContent'><h5>Import Data From CSV</h5></div>";

		} else if (typeName == 'TEXT') {
			response += "<div id = 'imageDiv' class='imageDivClass'>"
			response += "<img src='images/TEXT_Icon.svg'  id='csvtextImageId' class='importFileClass'>";
			response += "</div>";
			response += "<div class='VisionVisualizationUploadFileContent'><h5>Import Data From Text</h5></div>";

		} else {
			response += "<div id = 'imageDiv' class='imageDivClass'>"
			response += "<img src='images/Excel.png'  id='excelimageId' class='excelimageClass'>";
			response += "</div>";
			response += "<div class='VisionVisualizationUploadFileContent'><h5>Import Data From Excel</h5></div>";
		}
		uploadSMARTBIFilePopup(response, type, typeName);
	}

}

function uploadSMARTBIFilePopup(response, type, typeName) {
	var labelObject = {};
	try {
		labelObject = JSON.parse($("#labelObjectHidden").val());
	} catch (e) {
	}
	$("#dialog").html(response);
	$("#dialog").dialog({
		title: (labelObject['PDF Upload'] != null ? labelObject['PDF Upload'] : 'PDF Upload'),
		width: 500,
		height: 350,
		fluid: true,
		open: function() {

		},
		beforeClose: function(event, ui) {

		}
	});

	$("#importVisualizationDMPDFFile").hide();
	setTimeout(function() {
		$("html").on("dragover", function(e) {
			e.preventDefault();
			e.stopPropagation();
		});
		$("html").on("drop", function(e) {
			e.preventDefault();
			e.stopPropagation();
		});
		$('.visualizationDMPDFFileDivClass').on('drop', function(event) {
			$("#wait").css("display", "block");
			var filetype = 'pdf';
			var files = event.originalEvent.dataTransfer.files;
			sendPythonPdfUploadData(files[0], filetype);
			event.target.value = '';
		});
		$("#visionVisualizationDmPDFFileUpload").click(function() {
			console.log("iam in clickable ");
			$("#importVisualizationDMPDFFile").click();
		});

		$("#importVisualizationDMPDFFile").on('change', function(event) {
			console.log("iam in files change ");
			var filetype = 'pdf';
			var files = event.target.files;
			sendPythonPdfUploadData(files[0], filetype);
			event.target.value = '';
		});
	}, 300);
}

function sendPythonPdfUploadData(files, fileType, fileName) {
	showLoader();
	var xlsxPythonPDFFileData = new FormData();
	xlsxPythonPDFFileData.append("importTreeDMFile", files);
	xlsxPythonPDFFileData.append("selectedFiletype", fileType);
	xlsxPythonPDFFileData.append("selectedFileName", fileName);
	$.ajax({
		url: 'importTreeDMPythonPDFFile',
		type: "POST",
		data: xlsxPythonPDFFileData,
		enctype: 'multipart/form-data',
		processData: false,
		contentType: false,
		success: function(response) {
			stopLoader();
			if (response != null && !jQuery.isEmptyObject(response)) {
				$("#dialog").html("");
				$("#dialog").dialog("close");
				$("#dialog").dialog("destroy");
				stopLoader();
				if (response.hasOwnProperty("Messsage")) {
					var Messsage = response['Messsage'];
					showAnalyticsMsg("Message", Messsage, "errorMsgDivId");
				} else {

					closeDialogBox("#editPdfdialog");
					var fileData = response['fileData'];
					$("#editPdfdialog").html("<iframe id='editPdfdataIframe' style='width:100%;height:100%;'' src=''></iframe>");
					$("#editPdfdataIframe").attr("src", 'getPdfBasedonPath?fileName=' + encodeURIComponent(fileData));
					$("#editPdfdialog").dialog({
						title: (labelObject['Edit Pdf Data'] != null ? labelObject['Edit Pdf Data'] : 'Edit Pdf Data'),
						modal: true,
						html: true,
						height: 'auto',
						width: 1100,
						height: 550,
						fluid: true,

					});

					//$("#editPdfdataIframe").html(fileData);
					//$("#editPdfdataIframe").attr("src", 'getPdfBasedonPath?fileName='+fileData);    
				}




			}


		}, error: function(e) {
			console.log("The Error Message is:::" + e.message);
			sessionTimeout(e);
		}
	});
}

function showExpandAndModebarButtons(chartType, layout, data, chartId, createcount, table, axix, chartLabels, filterCondition) {
	$("#" + chartId + "_ExpandChartBodyLayoutId").remove();
	$("#" + chartId + "_ExpandChartBodyAxixId").remove();
	$("#" + chartId + "_ExpandChartBodyChartLabelsId").remove();
	$("#" + chartId + "_ExpandChartBodyFilterConditionId").remove();
	$("body").append("<input type='hidden' id='" + chartId + "_ExpandChartBodyLayoutId' value='" + JSON.stringify(layout) + "' class='maximizeWhiteIcon'>");
	$("body").append("<input type='hidden' id='" + chartId + "_ExpandChartBodyAxixId' value='" + JSON.stringify(axix) + "' class='maximizeWhiteIcon'>");
	$("body").append("<input type='hidden' id='" + chartId + "_ExpandChartBodyChartLabelsId' value='" + JSON.stringify(chartLabels) + "' class='maximizeWhiteIcon'>");
	$("body").append("<input type='hidden' id='" + chartId + "_ExpandChartBodyFilterConditionId' value='" + JSON.stringify(filterCondition) + "' class='maximizeWhiteIcon'>");
	var expandChartDivId = `<div class='modbarMaximizeClass' id='${chartId}_ExpandOnclick' onclick="expandBodyChart('${chartType}', 'layout', 'data', '${chartId}', '${createcount}', '${table}', 'axix', 'chartLabels', 'filterCondition')">
						 <img src='images/modbar-maximize.png' alt='maximize' class='maximizeBlueIcon'/>
						<img src='images/modbar-maximize-white.png' alt='maximize' class='maximizeWhiteIcon'/>
						</div>
						 <i class="fa fa-ellipsis-h" aria-hidden="true" onclick = showModebarContainer('${chartId}')></i>`;
	$("#controlMenuBtnClassid" + chartId).html(expandChartDivId);
}
function showEchartsExpandAndModebarButtons(chartId, chartType, count, tableName, result, expandChartDivId) {
	$("#" + chartId + "_ExpandEChartBodyResultId").remove();
	$("body").append("<input type='hidden' id='" + chartId + "_ExpandEChartBodyResultId' value='" + result + "' class='maximizeWhiteIcon'>");
	var expandChartDivId = `<div class='modbarMaximizeClass' id='${chartId}_ExpandOnclick' onclick="expandBodyEChart('${chartId}','${chartType}', '${count}', '${tableName}','result' ,'${expandChartDivId}')">
						 <img src='images/modbar-maximize.png' alt='maximize' class='maximizeBlueIcon'/>
						<img src='images/modbar-maximize-white.png' alt='maximize' class='maximizeWhiteIcon'/>
						</div>
						 <i class="fa fa-ellipsis-h" aria-hidden="true" onclick = showModebarContainer('${chartId}')></i>`;
	$("#controlMenuBtnClassid" + chartId).html(expandChartDivId);
}

function expandBodyChart(chartType, layout, data, chartId, createcount, table, axix, chartLabels, filterCondition) {
	var layoutStr = $("#" + chartId + "_ExpandChartBodyLayoutId").val();
	var axixStr = $("#" + chartId + "_ExpandChartBodyAxixId").val();
	var chartLabelsStr = $("#" + chartId + "_ExpandChartBodyChartLabelsId").val();
	var filterConditionStr = $("#" + chartId + "_ExpandChartBodyFilterConditionId").val();
	var graphDiv = document.getElementById(chartId);

	if (layoutStr != null && layoutStr != '' && layoutStr != undefined) {
		layout = JSON.parse(layoutStr);
	}
	if (graphDiv != null && !jQuery.isEmptyObject(graphDiv)) {
		data = graphDiv.data;
	}
	if (axixStr != null && axixStr != '' && axixStr != undefined) {
		axix = JSON.parse(axixStr);
	}
	if (chartLabelsStr != null && chartLabelsStr != '' && chartLabelsStr != undefined) {
		chartLabels = JSON.parse(chartLabelsStr);
	}
	if (filterConditionStr != null && filterConditionStr != '' && filterConditionStr != undefined) {
		filterCondition = JSON.parse(filterConditionStr);
	}
	expandChart(chartType, layout, data, chartId, createcount, table, axix, chartLabels, filterCondition);
}
function expandBodyEChart(chartId, chartType, count, tableName, result, expandChartDivId) {
	var resultStr = $("#" + chartId + "_ExpandEChartBodyResultId").val();
	if (resultStr != null && resultStr != '' && resultStr != undefined) {
		result = resultStr;
	}
	if (!(expandChartDivId != null && expandChartDivId != '' && expandChartDivId != undefined && expandChartDivId != 'undefined' && expandChartDivId != 'null')) {
		expandChartDivId = '';
	}
	expandEChart(chartId, chartType, count, tableName, result, expandChartDivId);
}

function scheduleDailyTask() {

	$.ajax({
		type: "POST",
		url: "saveSentDashBoardMailLastRun",
		cache: false,
		data: {},
		success: function(response) {
			//TODO: Need to add Loader
			stopaiLoader();
			if (response != null && !jQuery.isEmptyObject(response)) {
				const lastRun = response['lastRun'];
				if (!(lastRun != null && lastRun != '' && lastRun != undefined)) {
					lastRun = 0;
				}
				const now = new Date().getTime();
				const MILLISECONDS_IN_A_DAY = 24 * 60 * 60 * 1000;

				if (lastRun && now - lastRun < MILLISECONDS_IN_A_DAY) {
					// Task already run today
					return;
				}

				// Run the task immediately
				dailyTask();

				// Schedule the task for the next day
				setInterval(() => {
					dailyTask();

				}, MILLISECONDS_IN_A_DAY);


			}

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});



}


function dailyTask() {
	$.ajax({
		type: "POST",
		url: "getDashBoardsForMail",
		cache: false,
		data: {},
		success: function(response) {
			//TODO: Need to add Loader
			stopaiLoader();
			if (response != null && !jQuery.isEmptyObject(response)) {
				const dashBoardNames = response['dashBoardNames'];
				if (dashBoardNames != null && dashBoardNames != '' && dashBoardNames != undefined) {
					var dashBoardArr = dashBoardNames.split(",");
					$.each(dashBoardArr, function(i, dashBoardName) {
						let mypromise = new Promise(resolve => {
							getDashBoardVisualizationchart(dashBoardName);

						});
						getDashBoardMailAllImagesInPdf(dashBoardName);
					});
				}

			}

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}



async function getDashBoardVisualizationchart(dashbordname, dashbordTittle) {

	$.ajax({
		url: 'getChartData',
		type: "POST",
		dataType: 'json',
		traditional: true,
		cache: false,
		async: true,
		data: {
			dashbordname: dashbordname
		},
		success: function(response) {
			stopLoader();

			if (response != null && !jQuery.isEmptyObject(response)) {
				$("#visualizeDashBoardMailchartId").remove();
				$("body").append("<div id='visualizeDashBoardMailchartId' class='visualizeDashBoardMailchartClass' style='display:none'></div>");
				var bigChartsList = response['bigChartsList'];
				var dataarr = response['dataarr'];
				if (dataarr != null && dataarr != '' && dataarr != undefined) {
					var count = 0;
					for (var i = 0; i < dataarr.length; i++) {
						var XAxix = dataarr[i]['xAxix'];
						var yAxix = dataarr[i]['yAxix'];
						var type = dataarr[i]['type'];
						var table = dataarr[i]['table'];
						var id = dataarr[i]['chartid'];
						var Lebel = dataarr[i]['Lebel'];
						var aggColumnName = dataarr[i]['aggColumnName'];
						var filterCondition = dataarr[i]['filterCondition'];
						var chartPropObj = dataarr[i]['chartPropObj'];
						var chartConfigObj = dataarr[i]['chartConfigObj'];
						var labelLegend = dataarr[i]['labelLegend'];
						var colorsObj = dataarr[i]['colorsObj'];
						var chartConfigToggleStatus = dataarr[i]['chartConfigToggleStatus'];
						var homeFilterColumn = dataarr[i]['homeFilterColumn'];
						var fetchQuery = dataarr[i]['fetchQuery'];
						var radioButtons = dataarr[i]['radioButtons'];
						var comboValue = dataarr[i]['comboValue'];
						var currencyConversionStrObject = dataarr[i]['currencyConversionStrObject'];
						var paramCardDateObj = dataarr[i]['paramCardDateObj'];
						var cardType = dataarr[i]['cardType'];
						var cardTrendType = dataarr[i]['cardTrendType'];
						var cardTrend = dataarr[i]['cardTrend'];
						var zAxix = dataarr[i]['zAxis'];
						var query = dataarr[i]['query'];
						var resizeData = dataarr[i]['resizeData'];
						var chartSeqNo = dataarr[i]['chartSeqNo'];
						var candleCols = dataarr[i]['candleColumns'];
						var drillDownColumns = dataarr[i]['drillDownColumns'];
						if (type != null && type != '' && type != undefined && type == 'COMPARE_FILTER') {

						} else if (type != null && type != '' && type != undefined && type == "FILTER") {

						} else if ((yAxix != null && yAxix != '' && yAxix != 'undefined' && type != 'Card') || (query != null && query != '' && query != undefined)) {
							var chartid = id;
							var divClass = "col-md-6 col-sm-6 col-lg-3";
							if (type == 'decompositionTree') {
								divClass = "col-md-6 col-sm-6 col-lg-12";
							}
							if (bigChartsList != null && !jQuery.isEmptyObject(bigChartsList) && bigChartsList.indexOf(type) > -1) {
								divClass = "col-md-6 col-sm-6 col-lg-12";
							}
							var chartDivId = `
                                <div class='${divClass} homeChartWrapDiv' chart-level='0' id='visionVisualizeChartHome${count}'>
                                     <button class='${chartid}_PrevDataLeftCls visionVisualizeChartHomePrevDataLeft' style='display:none'><i class='fa fa-arrow-left'></i></button>
                                    <button class='${chartid}_NextDataRightCls visionVisualizeChartHomeNextDataRight' style='display:none'><img src="images/chart-right-arrow.png" alt="chart-right-arrow.png"></button>
                                    <div class='flip-chart' id='chart-container${count}'>
                                        <div class='flip-chart-inner' id='chart-card${count}'>
                                            <div class='flip-chart-front' id='chart-front-id${count}'>
                                                <div id='homeChartParentDiv${count}' class='homeChartParentDiv'>
                                                    <div class='chartMain chartBoardDashBoardMail' id='${chartid}'></div>
                                                    <div class="controlMenuBtnClass" id="controlMenuBtnClassid${chartid}">
												        <div class="modbarMaximizeClass" id='${chartid}_ExpandOnclick' onclick="expandChart()">
												            <img src="images/modbar-maximize.png" alt="maximize" class="maximizeBlueIcon">
												            <img src="images/modbar-maximize-white.png" alt="maximize" class="maximizeWhiteIcon">
												        </div>
												        <i class="fa fa-ellipsis-h" aria-hidden="true" onclick = showModebarContainer('${chartid}')></i>
												    </div>
                                                    <div id='${id}_traces' class='legendTraces'></div>
                                                    <div class='rightControls'>
                                                        <div class='iconDiv'><img src='images/Plus-Icon-02.svg' class='visionVisualizeHorizontalDotsClass visionVisualChartBoxSelected'></div>
                                                        <div class='iconDiv'><img src='images/FeedBack_Icon.svg' class='visionVisualizeHorizontalDotsClass visionVisualChartBoxSelected'></div>
                                                        <div class='iconDiv'><img src='images/Settings_Icon.svg' class='visionVisualizeHorizontalDotsClass visionVisualChartBoxSelected'></div>
                                                        <div class='iconDiv'><img src='images/Filter.svg' class='visionVisualizeHorizontalDotsClass visionVisualChartBoxSelected'></div>
                                                        <div class='iconDiv'><img src='images/search_blue.png' class='visionVisualizeHorizontalDotsClass visionVisualChartBoxSelected'></div>
                                                    </div>
                                                </div>
                                            </div>
                                            <div class='flip-chart-back' id='chart-back-id${count}'></div>
                                        </div>
                                    </div>
                                    <div>
                                        <div class='chartDialogClass' id='chartDialog${count}' style='display: none;'></div>
                                        <div class='createpopupClass' id='homepagecreatepopupId${count}' style='display: none;'></div>
                                        <input type='hidden' id='${chartid}_filter' value=''/>
                                        <input type='hidden' id='${chartid}_homePageFilter' value=''/>
                                        <input type='hidden' id='${chartid}_chartFilter' value=''/>
                                        <input type='hidden' id='${chartid}_startIndex' value='0'/>
                                        <input type='hidden' id='${chartid}_endIndex' value='10'/>
                                        <input type='hidden' id='${chartid}_pageSize' value='10'/>
                                        <input type='hidden' id='${chartid}_TotalChartCount' value='0'/>
                                        <input type='hidden' id='${chartid}_options' value=''/>
                                        <input type='hidden' id='${chartid}_chartType' value='${type}'/>
                                        <input type='hidden' id='${chartid}_count' value='${count}'/>
                                        <input type='hidden' id='${chartid}_dynamic_XAxisLength' value=''/>
                                        <input type='hidden' id='${chartid}_dynamic_YAxisLength' value=''/>
                                        <input type='hidden' id='${chartid}_resizeData' value='${resizeData}'/>
                                        <input type='hidden' id='${chartid}_dynamic_comboColLength' value=''/>
                                        <input type='hidden' id='${chartid}_dynamic_parentColLength' value=''/>
                                        <input type='hidden' id='${chartid}_echarts_toolBox_filterCond' value=''/>
                                        <input type='hidden' id='${chartid}_drillDownColumns' value='${drillDownColumns}'/>
                                        <input type='hidden' id='${chartid}_drillDownColumnsFlag' value=''/>
                                        <input type='hidden' id='${chartid}_drillDownColumnsAxisColumn' value=''/>
                                        <input type='hidden' id='${chartid}_chartSeqNo' value='${chartSeqNo}'/>`;
							//if (type != null && type != '' && type != undefined && eChartsArrList.indexOf(type) > -1) {
							chartDivId += `
                            			            <div id='${chartid}_toolBox' class='iconsDiv' style='position: absolute;top: 2px;right: 3px;height: 99%; background: #f1f1f1;height: 300px;overflow-y:auto;display:none'><ul style='height:100%'></ul></div>
                            			            <div id='${chartid}_radioButtons' class='visionVisualizeRadioButtonsClass'></div>`;
							//}

							chartDivId += `
                            			        </div>
                            			    </div>`;

							$("#visualizeDashBoardMailchartId").append(chartDivId);


							if (query != null && query != '' && query != undefined) {
								//getQueryVisualizeChart(chartid, type, filterCondition, chartPropObj, chartConfigObj, count, labelLegend, colorsObj, comboValue, chartConfigToggleStatus, "", "", fetchQuery, radioButtons, currencyConversionStrObject, query, table);
							} else {
								getDashBoardMailVisualizeChart(chartid, type, XAxix, yAxix, table, aggColumnName, filterCondition, chartPropObj, chartConfigObj, count, labelLegend, colorsObj, comboValue, chartConfigToggleStatus, "", "", fetchQuery, radioButtons, currencyConversionStrObject, zAxix, "", candleCols);
							}
							count++;
						}

					}

				}

			}
		}, error: function(e) {
			sessionTimeout(e);
		}
	});

}


function getDashBoardMailVisualizeChart(chartId, chartType, axix, values, table, aggColumnName, filterCondition, chartPropObj, chartConfigObj, createcount, labelLegend, colorsObj, comboValue, chartConfigToggleStatus, compareChartsFlag, comparechartFilterObj, fetchQuery, radioButtons, currencyConversionStrObject, zAxix, foreCastFlag, candleCols) {
	// var joinQuery = $("#visionVisualizeConfigJoinQuery0").val();
	//	if(isClickedPrevNextBtn){
	// handlePrevNextBtnsHomePage(chartId, chartType, axix, values, table, aggColumnName, filterCondition, chartPropObj, chartConfigObj, createcount, labelLegend, colorsObj, comboValue, chartConfigToggleStatus, compareChartsFlag, comparechartFilterObj, fetchQuery, radioButtons, currencyConversionStrObject, zAxix);
	//    	}
	$("#" + chartId + "_toolBox").hide();
	$("#" + chartId + "_legends").show();
	var axisColumns = [];
	var valuesColumns = [];
	var tablesObj = [];

	var drillDownColumnsFlag = $("#" + chartId + "_drillDownColumnsFlag").val();
	if (drillDownColumnsFlag != null && drillDownColumnsFlag != '' && drillDownColumnsFlag != undefined && drillDownColumnsFlag == 'Y') {
		axix = $("#" + chartId + "_drillDownColumnsAxisColumn").val();
		var drillDownChartFilters = drilldownChartFilters[chartId];
		if (filterCondition != null && filterCondition != '' && filterCondition != undefined) {
			filterCondition = JSON.parse(filterCondition);
			if (!(filterCondition != null && !jQuery.isEmptyObject(filterCondition))) {
				filterCondition = [];
			}
			$.each(drillDownChartFilters, function(index, val) {
				filterCondition.push(val);
			});
		} else if (drillDownChartFilters != null && !jQuery.isEmptyObject(drillDownChartFilters)) {
			filterCondition = drillDownChartFilters;
		}
		if (filterCondition != null && !jQuery.isEmptyObject(filterCondition)) {
			filterCondition = JSON.stringify(filterCondition);
		}
	}

	if (axix != null && axix != '' && axix != undefined) {
		var axisArr = JSON.parse(axix);
		$.each(axisArr, function(index, value) {
			if (value != null && !jQuery.isEmptyObject(value)) {
				var columnObj = {};
				var tableName = value['tableName'];
				columnObj['tableName'] = value['tableName'];
				columnObj['columnName'] = value['columnName'];
				axisColumns.push(columnObj);
				if (!(tablesObj.indexOf(tableName) > -1)) {
					tablesObj.push(tableName);
				}
			}
		});
	}

	if (values != null && values != '' && values != undefined) {
		var valuesArr = JSON.parse(values);
		$.each(valuesArr, function(index, value) {
			if (value != null && !jQuery.isEmptyObject(value)) {
				var columnObj = {};
				columnObj['tableName'] = value['tableName'];
				columnObj['columnName'] = value['columnName'];
				columnObj['aggColumnName'] = value['aggColumnName'];
				columnObj['userProvidedLegendLabel'] = value['userProvidedLegendLabel'];
				if (value['columnLabel'] != null && value['columnLabel'] != ''
					&& value['columnLabel'] != undefined && value['columnLabel'] != 'null') {
					columnObj['columnLabel'] = value['columnLabel'];
				}
				valuesColumns.push(columnObj);
				if (compareChartsFlag != null && compareChartsFlag != '' && compareChartsFlag != undefined && compareChartsFlag == 'Y' || compareChartsFlag == 'YF') {
					valuesColumns.push(columnObj);
				}
				var tableName = value['tableName'];
				if (!(tablesObj.indexOf(tableName) > -1)) {
					tablesObj.push(tableName);
				}
			}
		});
	}


	var comboValues = [];
	if (comboValue != null && comboValue != '' && comboValue != undefined) {
		var comboValueArr = JSON.parse(comboValue);
		$.each(comboValueArr, function(index, value) {
			if (value != null && !jQuery.isEmptyObject(value)) {
				var comboValueObj = {};
				comboValueObj['tableName'] = value['tableName'];
				comboValueObj['columnName'] = value['columnName'];
				comboValueObj['aggColumnName'] = value['aggColumnName'];
				comboValueObj['columnLabel'] = value['columnLabel'];
				comboValueObj['userProvidedLegendLabel'] = value['userProvidedLegendLabel'];
				comboValues.push(comboValueObj);
				if (compareChartsFlag != null && compareChartsFlag != '' && compareChartsFlag != undefined && compareChartsFlag == 'Y' || compareChartsFlag == 'YF') {
					comboValues.push(comboValueObj);
				}
				var tableName = value['tableName'];
				if (!(tablesObj.indexOf(tableName) > -1)) {
					tablesObj.push(tableName);
				}
			}
		});
	}
	var zAxixValues = [];
	if (zAxix != null && zAxix != '' && zAxix != undefined) {
		var zAxixArr = JSON.parse(zAxix);
		$.each(zAxixArr, function(index, value) {
			if (value != null && !jQuery.isEmptyObject(value)) {
				var zAxixObj = {};
				zAxixObj['tableName'] = value['tableName'];
				zAxixObj['columnName'] = value['columnName'];
				zAxixObj['aggColumnName'] = value['aggColumnName'];
				zAxixObj['columnLabel'] = value['columnLabel'];
				zAxixObj['userProvidedLegendLabel'] = value['userProvidedLegendLabel'];
				zAxixValues.push(zAxixObj);
				if (compareChartsFlag != null && compareChartsFlag != '' && compareChartsFlag != undefined && compareChartsFlag == 'Y' || compareChartsFlag == 'YF') {
					zAxixValues.push(zAxixObj);
				}
				var tableName = value['tableName'];
				if (!(tablesObj.indexOf(tableName) > -1)) {
					tablesObj.push(tableName);
				}
			}
		});
	}
	var candleColumnsObj = [];
	if (candleCols != null && candleCols != '' && candleCols != undefined) {
		var candleColsArr = JSON.parse(candleCols);
		$.each(candleColsArr, function(index, value) {
			if (value != null && !jQuery.isEmptyObject(value)) {
				var candleColsObj = {};
				candleColsObj['tableName'] = value['tableName'];
				candleColsObj['columnName'] = value['columnName'];
				candleColsObj['aggColumnName'] = value['aggColumnName'];
				candleColsObj['columnLabel'] = value['columnLabel'];
				candleColsObj['userProvidedLegendLabel'] = value['userProvidedLegendLabel'];
				candleColumnsObj.push(candleColsObj);
				var tableName = value['tableName'];
				if (!(tablesObj.indexOf(tableName) > -1)) {
					tablesObj.push(tableName);
				}
			}
		});
	}




	var startIndex = $("#" + chartId + "_startIndex").val();
	var endIndex = $("#" + chartId + "_endIndex").val();
	var pageSize = $("#" + chartId + "_pageSize").val();
	var data = {};
	var currencySymbol = '';
	var isCurrencyConversionEvent = '';
	var toCurrencyDropDownValue = '';
	if (currencyConversionStrObject !== undefined && currencyConversionStrObject != null
		&& currencyConversionStrObject !== '') {
		var currencyConversionObject = JSON.parse(currencyConversionStrObject);
		isCurrencyConversionEvent = currencyConversionObject['isCurrencyConversionEvent'];
		var fromCurrencyDropDownValue = currencyConversionObject['fromCurrencyDropDownValue'];
		toCurrencyDropDownValue = currencyConversionObject['toCurrencyDropDownValue'];
		currencySymbol = currencyConversionObject['currencySymbol'];
		data['isCurrencyConversionEvent'] = isCurrencyConversionEvent;
		data['fromCurrencyDropDownValue'] = fromCurrencyDropDownValue;
		data['toCurrencyDropDownValue'] = toCurrencyDropDownValue;
	}





	$(`#${chartId}_dynamic_XAxisLength`).val(axisColumns.length);
	$(`#${chartId}_dynamic_YAxisLength`).val(valuesColumns.length);
	$(`#${chartId}_dynamic_comboColLength`).val(comboValues.length);
	$(`#${chartId}_dynamic_parentColLength`).val(zAxixValues.length);
	$(`#${chartId}_dynamic_candleColLength`).val(candleColumnsObj.length);
	data['axisColumns'] = JSON.stringify(axisColumns);
	data['valuesColumns'] = JSON.stringify(valuesColumns);
	data['comboColumns'] = JSON.stringify(comboValues);
	data['zAxixValues'] = JSON.stringify(zAxixValues);
	data['candleValues'] = JSON.stringify(candleColumnsObj);
	data['tablesObj'] = JSON.stringify(tablesObj);
	data['chartType'] = chartType;
	data['chartId'] = chartId;
	data['filterColumns'] = filterCondition;
	data['chartCOnfigObjStr'] = chartPropObj;
	data['chartConfigPositionKeyStr'] = chartConfigObj;
	data['compareChartsFlag'] = compareChartsFlag;
	data['compareChartFilters'] = JSON.stringify(comparechartFilterObj);
	data['radioButtons'] = radioButtons;
	data['startIndex'] = startIndex;
	data['endIndex'] = endIndex;
	data['pageSize'] = pageSize;
	data['colorsObj'] = colorsObj;
	data['foreCastFlag'] = foreCastFlag;
	var chartPropObject = JSON.parse(chartPropObj);
	var title = chartPropObject[chartType.toUpperCase() + 'CHARTTITLE'];
	showLoader();
	$.ajax({
		url: 'fetchChartData',
		type: "POST",
		data: data,
		dataType: 'json',
		traditional: true,
		cache: false,
		success: function(response) {
			stopLoader();
			$("#Loader").css("display", "none");
			$("body").css("pointer-events", "auto");
			if (response != null && !jQuery.isEmptyObject(response) && !response.hasOwnProperty('ERROR_MSG')) {
				if (response.hasOwnProperty('FORECAST_MSG')) {
					showMesgModelPopup(response['FORECAST_MSG']);
					return;
				}
				var axisColumnName;
				var defaultLegendNames = {};
				$.each(axisColumns, function(index, value) {
					var axisName = value.columnName.split(".")[1];
					axisColumnName = axisName.replace(/[()]/g, "");
					axisColumnName = axisColumnName.replace(/_/g, " ");
					defaultLegendNames[axisColumnName] = axisColumnName;
				});

				response['axisColumnName'] = axisColumnName;
				$("#" + chartId).addClass("chartMainBorderBox");
				var compareChartTypes = ['column', 'lines', 'scatterpolar'];
				response['isCurrencyConversionEvent'] = isCurrencyConversionEvent;
				response['currencySymbol'] = currencySymbol;
				response['toCurrencyDropDownValue'] = toCurrencyDropDownValue;
				if (chartType != null && chartType != '' && chartType == 'heatMap') {
					getEchartHeatMap(chartId, response, createcount, chartType, '', filterCondition);
					//getHetaMap(chartId, response, createcount);
					return;
				} else if (chartType != null && chartType != '' && chartType == 'sunburst') {
					getSunburstChart(chartId, response, createcount, chartType, '', filterCondition);
					return;
				} else if (chartType != null && chartType != '' && chartType == 'BarAndLine') {
					getBarAndLineChart(chartId, response, createcount, chartType, '', filterCondition);
					return;
				} else if (chartType != null && chartType != '' && chartType == 'treemap') {
					getTreeMapChart(chartId, response, createcount, chartType, '', axisColumnName, filterCondition);
					return;
				} else if (chartType !== null && chartType !== '' && chartType !== undefined && chartType === 'BasicAreaChart') {
					$('#' + chartId).remove();
					getBasicAreaChartTypeFromDashBoard(chartId, response, createcount, chartType, '', filterCondition);
					return;
				}
				else if (chartType !== null && chartType !== '' && chartType !== undefined && chartType === 'StackedAreaChart') {
					$('#' + chartId).remove();
					getStackedAreaChartFromDashBoard(chartId, response, createcount, chartType, '', filterCondition);

					return;
				}
				else if (chartType !== null && chartType !== '' && chartType !== undefined && chartType === 'GradStackAreaChart') {
					$('#' + chartId).remove();
					getGradientStackedAreaChartFromDashBoard(chartId, response, createcount, chartType, '', filterCondition)
					return;
				}
				else if (chartType !== null && chartType !== '' && chartType !== undefined && chartType === 'AreaPiecesChart') {
					//	 73
					$('#' + chartId).remove();
					getAreaPiecesChartFromDashBoard(chartId, response, createcount, chartType, '', filterCondition);
					return;
				} else if (chartType !== null && chartType !== '' && chartType !== undefined && chartType === 'ganttChart') {
					google.charts.setOnLoadCallback(getGanttChart(chartId, response, createcount, chartType, '', filterCondition));
					return;
				}
				else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'candlestick') {
					getCandlestickChart(chartId, response, createcount, chartType, '', filterCondition);
					return;
				}
				else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'geochart') {
					google.charts.setOnLoadCallback(getGeoChart(chartId, response, createcount, chartType, 'Y', filterCondition));
					return;
				}
				else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'sankey') {
					getSankeyChart(chartId, response, createcount, chartType, '', filterCondition);
					return;
				} else if (chartType != null && chartType != '' && chartType == 'stackedBarChart') {
					getstackedBarChart(chartId, response, count, chartType, '', filterCondition);
					return;
				}
				else if (chartType != null && chartType != '' && chartType == 'decompositionTree') {
					getDecompositionTreeEcharts(response, chartId, count, chartType, '', filterCondition);
					return;
				} else if (chartType != null && chartType != '' && chartType == 'boxplot') {
					getBoxPlotChart(chartId, response, count, chartType, "Y", filterCondition);
					return;
				} else if (chartType != null && chartType != '' && chartType == 'geoLatLangchart') {
					geoLatLangchart(chartId, response, count, chartType, "Y", filterCondition);
					return;
				}
				var iconArrowUp = {
					'height': 20,
					'width': 20,
					'path': 'M4.29289 15.7071C3.90237 15.3166 3.90237 14.6834 4.29289 14.2929L9.29289 9.29289C9.68342 8.90237 10.3166 8.90237 10.7071 9.29289L15.7071 14.2929C16.0976 14.6834 16.0976 15.3166 15.7071 15.7071C15.3166 16.0976 14.6834 16.0976 14.2929 15.7071L10 11.4142L5.70711 15.7071C5.31658 16.0976 4.68342 16.0976 4.29289 15.7071ZM4.29289 9.70711C3.90237 9.31658 3.90237 8.68342 4.29289 8.29289L9.29289 3.29289C9.68342 2.90237 10.3166 2.90237 10.7071 3.29289L15.7071 8.29289C16.0976 8.68342 16.0976 9.31658 15.7071 9.70711C15.3166 10.0976 14.6834 10.0976 14.2929 9.70711L10 5.41421L5.70711 9.70711C5.31658 10.0976 4.68342 10.0976 4.29289 9.70711Z',
					'color': '#0b4a99'
				};

				var icon = {
					'height': 512,
					'width': 512,
					'path': 'M3.853 54.87C10.47 40.9 24.54 32 40 32H472C487.5 32 501.5 40.9 508.1 54.87C514.8 68.84 512.7 85.37 502.1 97.33L320 320.9V448C320 460.1 313.2 471.2 302.3 476.6C291.5 482 278.5 480.9 268.8 473.6L204.8 425.6C196.7 419.6 192 410.1 192 400V320.9L9.042 97.33C-.745 85.37-2.765 68.84 3.854 54.87L3.853 54.87z',
					'color': '#0b4a99'
				};
				var icon1 = {
					'height': 512,
					'width': 448,
					'path': 'M448 32C483.3 32 512 60.65 512 96V416C512 451.3 483.3 480 448 480H64C28.65 480 0 451.3 0 416V96C0 60.65 28.65 32 64 32H448zM152 96H64V160H152V96zM208 160H296V96H208V160zM448 96H360V160H448V96zM64 288H152V224H64V288zM296 224H208V288H296V224zM360 288H448V224H360V288zM152 352H64V416H152V352zM208 416H296V352H208V416zM448 352H360V416H448V352z',
					'color': '#0b4a99'
				};
				var deleteicon = {
					'height': 512,
					'width': 448,
					'path': 'M135.2 17.69C140.6 6.848 151.7 0 163.8 0H284.2C296.3 0 307.4 6.848 312.8 17.69L320 32H416C433.7 32 448 46.33 448 64C448 81.67 433.7 96 416 96H32C14.33 96 0 81.67 0 64C0 46.33 14.33 32 32 32H128L135.2 17.69zM31.1 128H416V448C416 483.3 387.3 512 352 512H95.1C60.65 512 31.1 483.3 31.1 448V128zM111.1 208V432C111.1 440.8 119.2 448 127.1 448C136.8 448 143.1 440.8 143.1 432V208C143.1 199.2 136.8 192 127.1 192C119.2 192 111.1 199.2 111.1 208zM207.1 208V432C207.1 440.8 215.2 448 223.1 448C232.8 448 240 440.8 240 432V208C240 199.2 232.8 192 223.1 192C215.2 192 207.1 199.2 207.1 208zM304 208V432C304 440.8 311.2 448 320 448C328.8 448 336 440.8 336 432V208C336 199.2 328.8 192 320 192C311.2 192 304 199.2 304 208z',
					'color': '#0b4a99'
				};
				var Expand = {
					'height': 512,
					'width': 448,
					'path': 'M447.1 319.1v135.1c0 13.26-10.75 23.1-23.1 23.1h-135.1c-12.94 0-24.61-7.781-29.56-19.75c-4.906-11.1-2.203-25.72 6.937-34.87l30.06-30.06L224 323.9l-71.43 71.44l30.06 30.06c9.156 9.156 11.91 22.91 6.937 34.87C184.6 472.2 172.9 479.1 160 479.1H24c-13.25 0-23.1-10.74-23.1-23.1v-135.1c0-12.94 7.781-24.61 19.75-29.56C23.72 288.8 27.88 288 32 288c8.312 0 16.5 3.242 22.63 9.367l30.06 30.06l71.44-71.44L84.69 184.6L54.63 214.6c-9.156 9.156-22.91 11.91-34.87 6.937C7.798 216.6 .0013 204.9 .0013 191.1v-135.1c0-13.26 10.75-23.1 23.1-23.1h135.1c12.94 0 24.61 7.781 29.56 19.75C191.2 55.72 191.1 59.87 191.1 63.1c0 8.312-3.237 16.5-9.362 22.63L152.6 116.7l71.44 71.44l71.43-71.44l-30.06-30.06c-9.156-9.156-11.91-22.91-6.937-34.87c4.937-11.95 16.62-19.75 29.56-19.75h135.1c13.26 0 23.1 10.75 23.1 23.1v135.1c0 12.94-7.781 24.61-19.75 29.56c-11.1 4.906-25.72 2.203-34.87-6.937l-30.06-30.06l-71.43 71.43l71.44 71.44l30.06-30.06c9.156-9.156 22.91-11.91 34.87-6.937C440.2 295.4 447.1 307.1 447.1 319.1z',
					'color': '#0b4a99'
				};

				var EditIcon = {
					'height': 512,
					'width': 512,
					'id': chartId,
					'path': 'M490.3 40.4C512.2 62.27 512.2 97.73 490.3 119.6L460.3 149.7L362.3 51.72L392.4 21.66C414.3-.2135 449.7-.2135 471.6 21.66L490.3 40.4zM172.4 241.7L339.7 74.34L437.7 172.3L270.3 339.6C264.2 345.8 256.7 350.4 248.4 353.2L159.6 382.8C150.1 385.6 141.5 383.4 135 376.1C128.6 370.5 126.4 361 129.2 352.4L158.8 263.6C161.6 255.3 166.2 247.8 172.4 241.7V241.7zM192 63.1C209.7 63.1 224 78.33 224 95.1C224 113.7 209.7 127.1 192 127.1H96C78.33 127.1 64 142.3 64 159.1V416C64 433.7 78.33 448 96 448H352C369.7 448 384 433.7 384 416V319.1C384 302.3 398.3 287.1 416 287.1C433.7 287.1 448 302.3 448 319.1V416C448 469 405 512 352 512H96C42.98 512 0 469 0 416V159.1C0 106.1 42.98 63.1 96 63.1H192z',
					'color': '#0b4a99'
				};
				var pridictiveAnalysis = {
					'height': 512,
					'width': 512,
					'id': chartId,
					'path': 'M384 160C366.3 160 352 145.7 352 128C352 110.3 366.3 96 384 96H544C561.7 96 576 110.3 576 128V288C576 305.7 561.7 320 544 320C526.3 320 512 305.7 512 288V205.3L342.6 374.6C330.1 387.1 309.9 387.1 297.4 374.6L191.1 269.3L54.63 406.6C42.13 419.1 21.87 419.1 9.372 406.6C-3.124 394.1-3.124 373.9 9.372 361.4L169.4 201.4C181.9 188.9 202.1 188.9 214.6 201.4L320 306.7L466.7 159.1L384 160z',
					'color': '#0b4a99'
				};
				var AssignUser = {
					'height': 512,
					'width': 512,
					'id': chartId,
					'path': 'M424.1 287c-15.13-15.12-40.1-4.426-40.1 16.97V352H336L153.6 108.8C147.6 100.8 138.1 96 128 96H32C14.31 96 0 110.3 0 128s14.31 32 32 32h80l182.4 243.2C300.4 411.3 309.9 416 320 416h63.97v47.94c0 21.39 25.86 32.12 40.99 17l79.1-79.98c9.387-9.387 9.387-24.59 0-33.97L424.1 287zM336 160h47.97v48.03c0 21.39 25.87 32.09 40.1 16.97l79.1-79.98c9.387-9.391 9.385-24.59-.0013-33.97l-79.1-79.98c-15.13-15.12-40.99-4.391-40.99 17V96H320c-10.06 0-19.56 4.75-25.59 12.81L254 162.7L293.1 216L336 160zM112 352H32c-17.69 0-32 14.31-32 32s14.31 32 32 32h96c10.06 0 19.56-4.75 25.59-12.81l40.4-53.87L154 296L112 352z',
					'color': '#0b4a99'
				};
				var ColorPallete = {
					'height': 512,
					'width': 512,
					'id': chartId,
					'path': 'M512 255.1C512 256.9 511.1 257.8 511.1 258.7C511.6 295.2 478.4 319.1 441.9 319.1H344C317.5 319.1 296 341.5 296 368C296 371.4 296.4 374.7 297 377.9C299.2 388.1 303.5 397.1 307.9 407.8C313.9 421.6 320 435.3 320 449.8C320 481.7 298.4 510.5 266.6 511.8C263.1 511.9 259.5 512 256 512C114.6 512 0 397.4 0 256C0 114.6 114.6 0 256 0C397.4 0 512 114.6 512 256V255.1zM96 255.1C78.33 255.1 64 270.3 64 287.1C64 305.7 78.33 319.1 96 319.1C113.7 319.1 128 305.7 128 287.1C128 270.3 113.7 255.1 96 255.1zM128 191.1C145.7 191.1 160 177.7 160 159.1C160 142.3 145.7 127.1 128 127.1C110.3 127.1 96 142.3 96 159.1C96 177.7 110.3 191.1 128 191.1zM256 63.1C238.3 63.1 224 78.33 224 95.1C224 113.7 238.3 127.1 256 127.1C273.7 127.1 288 113.7 288 95.1C288 78.33 273.7 63.1 256 63.1zM384 191.1C401.7 191.1 416 177.7 416 159.1C416 142.3 401.7 127.1 384 127.1C366.3 127.1 352 142.3 352 159.1C352 177.7 366.3 191.1 384 191.1z',
					'color': '#0b4a99'
				};
				var compareChart = {
					'height': 512,
					'width': 512,
					'id': chartId,
					'path': 'M320 488C320 497.5 314.4 506.1 305.8 509.9C297.1 513.8 286.1 512.2 279.9 505.8L199.9 433.8C194.9 429.3 192 422.8 192 416C192 409.2 194.9 402.7 199.9 398.2L279.9 326.2C286.1 319.8 297.1 318.2 305.8 322.1C314.4 325.9 320 334.5 320 344V384H336C371.3 384 400 355.3 400 320V153.3C371.7 140.1 352 112.8 352 80C352 35.82 387.8 0 432 0C476.2 0 512 35.82 512 80C512 112.8 492.3 140.1 464 153.3V320C464 390.7 406.7 448 336 448H320V488zM456 79.1C456 66.74 445.3 55.1 432 55.1C418.7 55.1 408 66.74 408 79.1C408 93.25 418.7 103.1 432 103.1C445.3 103.1 456 93.25 456 79.1zM192 24C192 14.52 197.6 5.932 206.2 2.076C214.9-1.78 225-.1789 232.1 6.161L312.1 78.16C317.1 82.71 320 89.2 320 96C320 102.8 317.1 109.3 312.1 113.8L232.1 185.8C225 192.2 214.9 193.8 206.2 189.9C197.6 186.1 192 177.5 192 168V128H176C140.7 128 112 156.7 112 192V358.7C140.3 371 160 399.2 160 432C160 476.2 124.2 512 80 512C35.82 512 0 476.2 0 432C0 399.2 19.75 371 48 358.7V192C48 121.3 105.3 64 176 64H192V24zM56 432C56 445.3 66.75 456 80 456C93.25 456 104 445.3 104 432C104 418.7 93.25 408 80 408C66.75 408 56 418.7 56 432z',
					'color': '#0b4a99'
				};

				var refreshChart = {
					'height': 512,
					'width': 512,
					'id': chartId,
					'path': 'M464 16c-17.67 0-32 14.31-32 32v74.09C392.1 66.52 327.4 32 256 32C161.5 32 78.59 92.34 49.58 182.2c-5.438 16.81 3.797 34.88 20.61 40.28c16.89 5.5 34.88-3.812 40.3-20.59C130.9 138.5 189.4 96 256 96c50.5 0 96.26 24.55 124.4 64H336c-17.67 0-32 14.31-32 32s14.33 32 32 32h128c17.67 0 32-14.31 32-32V48C496 30.31 481.7 16 464 16zM441.8 289.6c-16.92-5.438-34.88 3.812-40.3 20.59C381.1 373.5 322.6 416 256 416c-50.5 0-96.25-24.55-124.4-64H176c17.67 0 32-14.31 32-32s-14.33-32-32-32h-128c-17.67 0-32 14.31-32 32v144c0 17.69 14.33 32 32 32s32-14.31 32-32v-74.09C119.9 445.5 184.6 480 255.1 480c94.45 0 177.4-60.34 206.4-150.2C467.9 313 458.6 294.1 441.8 289.6z',
					'color': '#0b4a99'
				};
				var reset = {
					'width': 928.6,
					'height': 1000,
					'id': chartId,
					'path': 'm786 296v-267q0-15-11-26t-25-10h-214v214h-143v-214h-214q-15 0-25 10t-11 26v267q0 1 0 2t0 2l321 264 321-264q1-1 1-4z m124 39l-34-41q-5-5-12-6h-2q-7 0-12 3l-386 322-386-322q-7-4-13-4-7 2-12 7l-35 41q-4 5-3 13t6 12l401 334q18 15 42 15t43-15l136-114v109q0 8 5 13t13 5h107q8 0 13-5t5-13v-227l122-102q5-5 6-12t-4-13z',
					'transform': 'matrix(1 0 0 -1 0 850)'
				};
				var AIChart = {
					'height': 512,
					'width': 512,
					'id': chartId,
					'path': 'M184 0c30.9 0 56 25.1 56 56V456c0 30.9-25.1 56-56 56c-28.9 0-52.7-21.9-55.7-50.1c-5.2 1.4-10.7 2.1-16.3 2.1c-35.3 0-64-28.7-64-64c0-7.4 1.3-14.6 3.6-21.2C21.4 367.4 0 338.2 0 304c0-31.9 18.7-59.5 45.8-72.3C37.1 220.8 32 207 32 192c0-30.7 21.6-56.3 50.4-62.6C80.8 123.9 80 118 80 112c0-29.9 20.6-55.1 48.3-62.1C131.3 21.9 155.1 0 184 0zM328 0c28.9 0 52.6 21.9 55.7 49.9c27.8 7 48.3 32.1 48.3 62.1c0 6-.8 11.9-2.4 17.4c28.8 6.2 50.4 31.9 50.4 62.6c0 15-5.1 28.8-13.8 39.7C493.3 244.5 512 272.1 512 304c0 34.2-21.4 63.4-51.6 74.8c2.3 6.6 3.6 13.8 3.6 21.2c0 35.3-28.7 64-64 64c-5.6 0-11.1-.7-16.3-2.1c-3 28.2-26.8 50.1-55.7 50.1c-30.9 0-56-25.1-56-56V56c0-30.9 25.1-56 56-56z',
					'color': '#0b4a99'
				};
				var notes = {
					'height': 20,
					'width': 20,
					'path': 'M 19.5 14.25 v -2.625 a 3.375 3.375 0 0 0 -3.375 -3.375 h -1.5 A 1.125 1.125 0 0 1 13.5 7.125 v -1.5 a 3.375 3.375 0 0 0 -3.375 -3.375 H 8.25 m -0.25 11.75 C 8 13.6667 8 13.3333 8 13 h 7.5 C 15 13.3333 14.5 13.6667 14 14 m -6 3 C 8 16.6667 8 16.3333 8 16 H 16 M 10.5 2.25 H 5.625 c -0.621 0 -1.125 0.504 -1.125 1.125 v 17.25 c 0 0.621 0.504 1.125 1.125 1.125 h 12.75 c 0.621 0 1.125 -0.504 1.125 -1.125 V 11.25 a 9 9 0 0 0 -9 -9 Z',
					'color': '#0b4a99'
				};
				var camera = {
					'width': 1000,
					'height': 1000,
					'path': 'm500 450c-83 0-150-67-150-150 0-83 67-150 150-150 83 0 150 67 150 150 0 83-67 150-150 150z m400 150h-120c-16 0-34 13-39 29l-31 93c-6 15-23 28-40 28h-340c-16 0-34-13-39-28l-31-94c-6-15-23-28-40-28h-120c-55 0-100-45-100-100v-450c0-55 45-100 100-100h800c55 0 100 45 100 100v450c0 55-45 100-100 100z m-400-550c-138 0-250 112-250 250 0 138 112 250 250 250 138 0 250-112 250-250 0-138-112-250-250-250z m365 380c-19 0-35 16-35 35 0 19 16 35 35 35 19 0 35-16 35-35 0-19-16-35-35-35z',
					'transform': 'matrix(1 0 0 -1 0 850)',
					'color': '#0b4a99'
				};
				var iconArrowDown = {
					'height': 20,
					'width': 20,
					'path': 'M15.7071 4.29289C16.0976 4.68342 16.0976 5.31658 15.7071 5.70711L10.7071 10.7071C10.3166 11.0976 9.68342 11.0976 9.29289 10.7071L4.29289 5.70711C3.90237 5.31658 3.90237 4.68342 4.29289 4.29289C4.68342 3.90237 5.31658 3.90237 5.70711 4.29289L10 8.58579L14.2929 4.29289C14.6834 3.90237 15.3166 3.90237 15.7071 4.29289ZM15.7071 10.2929C16.0976 10.6834 16.0976 11.3166 15.7071 11.7071L10.7071 16.7071C10.3166 17.0976 9.68342 17.0976 9.29289 16.7071L4.29289 11.7071C3.90237 11.3166 3.90237 10.6834 4.29289 10.2929C4.68342 9.90237 5.31658 9.90237 5.70711 10.2929L10 14.5858L14.2929 10.2929C14.6834 9.90237 15.3166 9.90237 15.7071 10.2929Z',
					'color': '#0b4a99'
				};
				var resizeIcon = {
					'height': 15,
					'width': 15,
					'path': 'M0 12.5v-9A1.5 1.5 0 0 1 1.5 2h13A1.5 1.5 0 0 1 16 3.5v9a1.5 1.5 0 0 1-1.5 1.5h-13A1.5 1.5 0 0 1 0 12.5zM2.5 4a.5.5 0 0 0-.5.5v3a.5.5 0 0 0 1 0V5h2.5a.5.5 0 0 0 0-1h-3zm11 8a.5.5 0 0 0 .5-.5v-3a.5.5 0 0 0-1 0V11h-2.5a.5.5 0 0 0 0 1h3z',
					'color': '#0b4a99'
				};
				var AIForecast = {
					'height': 512,
					'width': 512,
					'id': chartId,
					'path': 'M184 0c30.9 0 56 25.1 56 56V456c0 30.9-25.1 56-56 56c-28.9 0-52.7-21.9-55.7-50.1c-5.2 1.4-10.7 2.1-16.3 2.1c-35.3 0-64-28.7-64-64c0-7.4 1.3-14.6 3.6-21.2C21.4 367.4 0 338.2 0 304c0-31.9 18.7-59.5 45.8-72.3C37.1 220.8 32 207 32 192c0-30.7 21.6-56.3 50.4-62.6C80.8 123.9 80 118 80 112c0-29.9 20.6-55.1 48.3-62.1C131.3 21.9 155.1 0 184 0zM328 0c28.9 0 52.6 21.9 55.7 49.9c27.8 7 48.3 32.1 48.3 62.1c0 6-.8 11.9-2.4 17.4c28.8 6.2 50.4 31.9 50.4 62.6c0 15-5.1 28.8-13.8 39.7C493.3 244.5 512 272.1 512 304c0 34.2-21.4 63.4-51.6 74.8c2.3 6.6 3.6 13.8 3.6 21.2c0 35.3-28.7 64-64 64c-5.6 0-11.1-.7-16.3-2.1c-3 28.2-26.8 50.1-55.7 50.1c-30.9 0-56-25.1-56-56V56c0-30.9 25.1-56 56-56z',
					'color': '#0b4a99'
				};
				var drillDownIcon = {
					'height': 512,
					'width': 512,
					'id': chartId,
					'path': 'M98 190.06l139.78 163.12a24 24 0 0036.44 0L414 190.06c13.34-15.57 2.28-39.62-18.22-39.62h-279.6c-20.5 0-31.56 24.05-18.18 39.62z',
					'color': '#0b4a99'
				};
				$(".visualizationDashboardView").css("display", "block");
				var resultObj = response;
				var dataPropObject = resultObj['dataPropObject'];
				var chartDataObj = [];
				if (chartType !== null && chartType !== '' && chartType !== undefined && chartType === 'indicator') {
					chartDataObj['data'] = resultObj['data'];
				} else {
					chartDataObj = resultObj['data'];
				}

				var layoutObj = resultObj['layout'];
				var treeMapColObj = resultObj['treeMapCol'];
				var compareChartFlag = resultObj['compareChartFlag'];
				var resizeData = $('#' + chartId + "_resizeData").val();
				var dashbordname;
				var item = $("#OptionDropdownData").jqxDropDownList('getSelectedItem');
				if (item != null) {
					dashbordname = item.value;
				}
				var totalChartCount = resultObj['totalChartCount'];
				var totalChartCountData = resultObj['totalChartCountData'];
				$('#' + chartId + "_TotalChartCount").val(totalChartCountData);
				showOrdHideNextPrevButton(chartId);
				var plotlyChartLabelType = dataPropObject['textinfo'];
				var plotlyChartHoverType = dataPropObject['hoverinfo'];
				var data = [];
				var chartLabels = [];
				var dataObj = {};
				var layout = {};
				var chartButtons = {
					Scroll_Up: {
						name: 'Scroll Up', icon: iconArrowUp, click: function() {
							scrollUp(chartId, chartType);
						}
					},
					Name: {
						name: 'Name',
						title: 'Download as Image',
						icon: camera, click: function(gd) {
							const now = new Date();
							const day = String(now.getDate()).padStart(2, '0');
							const month = String(now.getMonth() + 1).padStart(2, '0'); // Months are 0-indexed
							const year = now.getFullYear();

							const hours = String(now.getHours()).padStart(2, '0');
							const minutes = String(now.getMinutes()).padStart(2, '0');
							const seconds = String(now.getSeconds()).padStart(2, '0');

							// Combine parts into desired format
							const formattedDateTime = `${day}-${month}-${year} ${hours}:${minutes}:${seconds}`;
							Plotly.downloadImage(gd, {
								format: 'png',
								height: 500,
								width: 500,
								filename: title + "-" + formattedDateTime
							})
						}
					},
					Show_Data: {
						name: 'Show Data', icon: icon1, click: function() {
							getGridData(axix, "", chartId, filterCondition, table);
						}
					},
					Filters: {
						name: 'Filters', icon: icon, click: function() {
							getfilterData(chartId, table, chartType);
						}
					},
					Delete: {
						name: 'Delete', icon: deleteicon, click: function() {
							deleteVisualizeChart(chartId, table, chartType);
						}
					},
					Expand: {
						name: 'Expand', icon: Expand, click: function() {
							expandChart(chartType, layout, data, chartId, createcount, table, axix, chartLabels, filterCondition); //jaggu
						}
					},
					Edit: {
						name: 'Edit', icon: EditIcon, click: function() {
							homePageChartSetting(chartId, chartType, layout, data, createcount, event, "", chartConfigToggleStatus);
						}
					},
					Trends: {
						name: 'Trends', icon: pridictiveAnalysis, click: function(event) {
							getChartContent(chartId, createcount, chartType, 'pridictiveAnalysis');
						}
					},
					Color_Pallete: {
						name: 'Color Pallete', icon: ColorPallete, click: function() {
							updatechartColor(chartId, createcount, data);
						}
					},
					Chart_Types: {
						name: 'Chart Types', icon: AssignUser, click: function() {
							changegraph(chartId, chartType, layout, data, createcount, Object.keys(response.data).length);
						}
					},
					Reset: {
						name: 'Reset', icon: reset, click: function() {
							refreshVisualizationchart(dashbordname, chartId, chartType, createcount);
						}
					},
					AI_Insights: {
						name: 'AI Insights', icon: AIChart, click: function() {
							getArtIntAPI(chartId, chartType, table);
						}
					},
					Notes: {
						name: 'Notes', icon: notes, click: function() {

							getChartNotes(chartId);
						}
					},
					Filp_Data: {
						name: 'Flip Data', icon: icon1, click: function() {
							getChartDataonFlip(chartId, JSON.stringify(chartDataObj));
						}
					},
					Compare_Charts: {
						name: 'Compare Charts', icon: compareChart, click: function(chartId) {
							getHomeDashboardCompareFilters(chartId['id'], chartType);
						}
					},
					Remove_Compare_Charts: {
						name: 'Remove Compare Charts', icon: refreshChart, click: function(chartId) {
							getRemoveCompareChart(chartId['id'], chartType);
						}

					},
					Scroll_Down: {
						name: 'Scroll Down',
						icon: iconArrowDown,
						click: function() {
							scrollDownArrow(chartId, chartType);
						}
					},
					Drill_Down: {
						name: 'DrillDown', icon: drillDownIcon, click: function() {
							getDrillDownColumnData(chartId, table, chartType);
						}
					},
				};
				var config = {
					responsive: true,
					displayModeBar: true,
					downloadImage: true,
					displaylogo: false,
					dragmode: false,
					modeBarButtonsToAdd: [],
					modeBarButtonsToRemove: ['toImage', 'zoomin', 'resetViews', 'resetScale2d', 'zoomout', 'pan2d', 'sendDataToCloud', 'hoverClosestCartesian', 'autoScale2d', 'lasso2d', 'select2d', 'zoom2d']
				};
				var allchartButtonsToBeAddedStr = $('#chartControlButtonsConfig').val();
				if (allchartButtonsToBeAddedStr != null && allchartButtonsToBeAddedStr != undefined && allchartButtonsToBeAddedStr != "") {
					var allchartButtonsToBeAddedObj = JSON.parse(allchartButtonsToBeAddedStr);
					var chartTypeButtonToBeAddedStr = allchartButtonsToBeAddedObj[chartType];
					if (chartTypeButtonToBeAddedStr != null && chartTypeButtonToBeAddedStr != undefined && chartTypeButtonToBeAddedStr != '') {
						var chartTypeButtonsToBeAddedObj = JSON.parse(chartTypeButtonToBeAddedStr);
						config.modeBarButtonsToAdd = Object.keys(chartTypeButtonsToBeAddedObj).reduce(function(result, key) {
							if (chartTypeButtonsToBeAddedObj[key] === 'Y' && chartButtons.hasOwnProperty(key)) {
								result.push(chartButtons[key]);
							}
							return result;
						}, []);
					}
				}
				var axisColumnName;
				var valuesColumnName;
				var defaultLegendNames = {};
				var userProvidedLegendNames = {};

				$.each(axisColumns, function(index, value) {
					var axisName = value.columnName.split(".")[1];
					axisColumnName = axisName.replace(/[()]/g, "");
					axisColumnName = axisColumnName.replace(/_/g, " ");
					defaultLegendNames[axisColumnName] = axisColumnName;
				});
				$.each(valuesColumns, function(index, value) {
					var valueName = value.columnName.split(".")[1];
					valuesColumnName = valueName.replace(/[()]/g, "");
					valuesColumnName = valuesColumnName.replace(/_/g, " ");
					defaultLegendNames[valuesColumnName] = valuesColumnName;
					var legendLabels = value['userProvidedLegendLabel'];
					if (legendLabels !== '' && legendLabels !== undefined && legendLabels !== null) {
						userProvidedLegendNames[valuesColumnName] = legendLabels;
					}
				});

				if (chartDataObj != null && !jQuery.isEmptyObject(chartDataObj)) {
					var traceHeight = 0;
					if (chartType !== null && chartType !== '' && chartType !== undefined && chartType === 'pie') {
						$.each(chartDataObj, function(key) {
							var traceObj = {};
							var colorObj = {};
							if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
								colorObj = JSON.parse(colorsObj)['clrs'];
							} else {
								colorObj = ["#EAC117", "#347C2C", "#806517", "#E66C2C", "#5C3317", "#C11B17"];
							}
							if (key !== axisColumnName) {
								chartLabels = chartDataObj[axisColumnName];
								traceObj['labels'] = chartDataObj[axisColumnName];
								traceObj['values'] = chartDataObj[key];
								traceObj['type'] = chartType;
								traceObj['name'] = '';
								traceObj['marker'] = colorObj;
								traceObj = getChartLabelOrHoverDataFormatter('texttemplate', traceObj, chartDataObj[key], plotlyChartLabelType, 'label', 'value', "", currencySymbol);
								traceObj = getChartLabelOrHoverDataFormatter('hovertemplate', traceObj, chartDataObj[key], plotlyChartHoverType, 'label', 'value', "", currencySymbol);
								$.each(dataPropObject, function(key, val) {
									traceObj[key] = val;
								});
								traceHeight = traceObj['labels'].length;
							}
							if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
								data.push(traceObj);
							}
						});
					} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'donut') {
						$.each(chartDataObj, function(key) {

							var traceObj = {};
							var colorObj = {};
							if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
								colorObj = JSON.parse(colorsObj)['clrs'];
							} else {
								colorObj = ["#EAC117", "#347C2C", "#806517", "#E66C2C", "#5C3317", "#C11B17"];
							}
							if (key !== axisColumnName) {
								chartLabels = chartDataObj[axisColumnName];
								traceObj['labels'] = chartDataObj[axisColumnName];
								traceObj['values'] = chartDataObj[key];
								traceObj['hole'] = 0.4;
								traceObj['type'] = 'pie';
								traceObj['name'] = '';
								traceObj['marker'] = colorObj;
								traceObj = getChartLabelOrHoverDataFormatter('texttemplate', traceObj, chartDataObj[key], plotlyChartLabelType, 'label', 'value', "", currencySymbol);
								traceObj = getChartLabelOrHoverDataFormatter('hovertemplate', traceObj, chartDataObj[key], plotlyChartHoverType, 'label', 'value', "", currencySymbol);
								$.each(dataPropObject, function(key, val) {
									traceObj[key] = val;
								});
								traceHeight = traceObj['labels'].length;
							}
							if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
								data.push(traceObj);
							}
						});
					} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'bar') {
						var colorCount = 0;
						if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
							colorArray = JSON.parse(colorsObj)['clrs'];
						} else {
							colorArray = ["#EAC117", "#347C2C", "#806517", "#E66C2C", "#5C3317", "#C11B17"];
						}
						$.each(chartDataObj, function(keyName) {

							var traceObj = {};
							var colorObj = {};
							if (keyName !== axisColumnName) {
								traceObj['y'] = chartDataObj[axisColumnName];
								traceObj['x'] = chartDataObj[keyName];
								traceObj['type'] = chartType;
								traceObj['orientation'] = 'h';
								traceObj = getChartLabelOrHoverDataFormatter('texttemplate', traceObj, chartDataObj[keyName], plotlyChartLabelType, 'y', 'x', "", currencySymbol);
								traceObj = getChartLabelOrHoverDataFormatter('hovertemplate', traceObj, chartDataObj[keyName], plotlyChartHoverType, 'y', 'x', "", currencySymbol);
								var keys = keyName.split("ASCOL");
								keyName = keys[0];
								traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
								$.each(dataPropObject, function(key, val) {
									if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
										var colorsArray = val['colors'];
										if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
											colorObj['color'] = colorsArray;
										} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
											colorObj['color'] = colorsArray[colorCount++ % colorArray.length];
										} else {
											colorObj['color'] = colorArray[colorCount++ % colorArray.length];
										}
										traceObj[key] = colorObj;
									} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
										colorObj['color'] = colorArray[colorCount++ % colorArray.length];
										traceObj[key] = colorObj;
									} else {
										traceObj[key] = val;
									}
								});
							}
							if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
								data.push(traceObj);
							}
						});
					} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'column') {
						var colorCount = 0;
						if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
							colorArray = JSON.parse(colorsObj)['clrs'];
						} else {
							colorArray = ["#EAC117", "#347C2C", "#806517", "#E66C2C", "#5C3317", "#C11B17"];
						}
						var i = 1;
						$.each(chartDataObj, function(keyName, val) {
							var traceObj = {};
							var colorObj = {};
							if (keyName !== axisColumnName) {
								traceObj['x'] = chartDataObj[axisColumnName];
								traceObj['y'] = chartDataObj[keyName];
								traceObj['type'] = 'bar';
								traceObj = getChartLabelOrHoverDataFormatter('texttemplate', traceObj, chartDataObj[keyName], plotlyChartLabelType, 'x', 'y', "", currencySymbol);
								traceObj = getChartLabelOrHoverDataFormatter('hovertemplate', traceObj, chartDataObj[keyName], plotlyChartHoverType, 'x', 'y', "", currencySymbol);
								var keys = keyName.split("ASCOL");
								keyName = keys[0];
								if (compareChartsFlag == 'YF') {
									traceObj['name'] = compareChartsLegendsTraceObj[chartId + '_name_' + i];
								} else {
									traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
								}
								i++;
								$.each(dataPropObject, function(key, val) {
									if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
										var colorsArray = val['colors'];
										if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
											colorObj['color'] = colorsArray;
										} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
											colorObj['color'] = colorsArray[colorCount++];
										} else {
											colorObj['color'] = colorArray[colorCount++];
										}
										traceObj[key] = colorObj;
									} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
										colorObj['color'] = colorArray[colorCount++];
										traceObj[key] = colorObj;
									} else {
										traceObj[key] = val;
									}
								});
							}

							if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
								data.push(traceObj);
							}
						});
					} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'lines') {
						var colorCount = 0;
						var lineColorCount = 0;
						if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
							colorArray = JSON.parse(colorsObj)['clrs'];
						} else {
							colorArray = ["#EAC117", "#347C2C", "#806517", "#E66C2C", "#5C3317", "#C11B17"];
						}
						var i = 1;
						$.each(chartDataObj, function(keyName, val) {
							var traceObj = {};
							var colorObj = {};
							if (keyName !== axisColumnName) {
								traceObj['x'] = chartDataObj[axisColumnName];
								traceObj['y'] = chartDataObj[keyName];
								traceObj['type'] = chartType;
								traceObj = getChartLabelOrHoverDataFormatter('texttemplate', traceObj, chartDataObj[keyName], plotlyChartLabelType, 'x', 'y', "", currencySymbol);
								traceObj = getChartLabelOrHoverDataFormatter('hovertemplate', traceObj, chartDataObj[keyName], plotlyChartHoverType, 'x', 'y', "", currencySymbol);
								var keys = keyName.split("ASCOL");
								keyName = keys[0];
								if (compareChartsFlag == 'YF') {
									traceObj['name'] = compareChartsLegendsTraceObj[chartId + '_name_' + i];
								} else {
									traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
								}
								i++;
								$.each(dataPropObject, function(key, val) {
									if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
										var colorsArray = val['color'];
										if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
											colorObj['color'] = colorsArray;
										} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
											colorObj['color'] = colorsArray[colorCount++];
										} else {
											colorObj['color'] = colorArray[colorCount++];
										}
										colorObj['size'] = val['size'];
										traceObj[key] = colorObj;
									} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
										colorObj['color'] = colorArray[colorCount++];
										traceObj[key] = colorObj;
									} else if (key === 'line' && !jQuery.isEmptyObject(val) && val !== null) {
										var lineObject = Object.assign({}, val);
										var colorsArray = lineObject['color'];
										if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
											lineObject['color'] = colorsArray;
										} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
											lineObject['color'] = colorsArray[lineColorCount++];
										} else {
											lineObject['color'] = colorArray[lineColorCount++];
										}
										traceObj[key] = lineObject;
									} else {
										traceObj[key] = val;
									}
								});
							}

							if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
								data.push(traceObj);
							}
						});
					} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'scatter') {
						var colorCount = 0;
						if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
							colorArray = JSON.parse(colorsObj)['clrs'];
						} else {
							colorArray = ["#EAC117", "#347C2C", "#806517", "#E66C2C", "#5C3317", "#C11B17"];
						}
						$.each(chartDataObj, function(keyName, val) {

							var traceObj = {};
							var colorObj = {};
							if (keyName !== axisColumnName) {
								traceObj['x'] = chartDataObj[axisColumnName];
								traceObj['y'] = chartDataObj[keyName];
								traceObj['type'] = chartType;
								traceObj['mode'] = 'markers';
								traceObj['marker'] = colorObj;
								traceObj = getChartLabelOrHoverDataFormatter('texttemplate', traceObj, chartDataObj[keyName], plotlyChartLabelType, 'x', 'y', "", currencySymbol);
								traceObj = getChartLabelOrHoverDataFormatter('hovertemplate', traceObj, chartDataObj[keyName], plotlyChartHoverType, 'x', 'y', "", currencySymbol);
								var keys = keyName.split("ASCOL");
								keyName = keys[0];
								traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
								$.each(dataPropObject, function(key, val) {
									if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
										var colorsArray = val['color'];
										if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
											colorObj['color'] = colorsArray;
										} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
											colorObj['color'] = colorsArray[colorCount++];
										} else {
											colorObj['color'] = colorArray[colorCount++];
										}
										colorObj['size'] = val['size'];
										traceObj[key] = colorObj;
									} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
										colorObj['color'] = colorArray[colorCount++];
										traceObj[key] = colorObj;
									} else {
										traceObj[key] = val;
									}
								});
							}
							if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
								data.push(traceObj);
							}
						});
					} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'waterfall') {
						var colorCount = 0;
						if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
							colorArray = JSON.parse(colorsObj)['clrs'];
						} else {
							colorArray = ["#EAC117", "#347C2C", "#806517", "#E66C2C", "#5C3317", "#C11B17"];
						}
						$.each(chartDataObj, function(keyName) {

							var traceObj = {};
							var colorObj = {};
							if (keyName !== axisColumnName) {
								var measureArr = [];
								var axisCols = chartDataObj[axisColumnName];
								if (axisCols != null && !jQuery.isEmptyObject(axisCols)) {
									var axisLength = axisCols.length;
									for (var l = 0; l < axisLength; l++) {
										measureArr.push("relative");
									}
									traceObj['measure'] = measureArr;
								}
								traceObj['x'] = chartDataObj[axisColumnName];
								traceObj['y'] = chartDataObj[key];
								traceObj['type'] = chartType;
								traceObj['orientation'] = 'v';
								traceObj = getChartLabelOrHoverDataFormatter('texttemplate', traceObj, chartDataObj[keyName], plotlyChartLabelType, 'y', 'x', "", currencySymbol);
								traceObj = getChartLabelOrHoverDataFormatter('hovertemplate', traceObj, chartDataObj[keyName], plotlyChartHoverType, 'y', 'x', "", currencySymbol);
								var keys = keyName.split("ASCOL");
								keyName = keys[0];
								traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
								$.each(dataPropObject, function(key, val) {
									if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
										var colorsArray = val['colors'];
										if (colorsArray !== undefined && colorsArray !== null && colorsArray.length !== null) {
											colorObj['color'] = colorsArray[colorCount++];
										} else {
											colorObj['color'] = colorArray[colorCount++];
										}
										traceObj[key] = colorObj;
									} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
										colorObj['color'] = colorArray[colorCount++];
										traceObj[key] = colorObj;
									} else {
										traceObj[key] = val;
									}
								});
							}
							if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
								data.push(traceObj);
							}
						});
					} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'treemap') {
						var treeDomain = 0
						for (var key in chartDataObj) {
							var treeDataObj = chartDataObj[key];
							var treeObj = {};
							treeObj['type'] = chartType;
							treeObj['labels'] = treeDataObj[treeMapColObj['labels']];
							treeObj['parents'] = treeDataObj[treeMapColObj['parents']];
							treeObj['values'] = treeDataObj[treeMapColObj['values']];
							treeObj['textinfo'] = "label+value";
							treeObj['branchvalues'] = "total";
							var length = Object.keys(chartDataObj).length;
							var split = ((100 / length) / 100);
							if (chartDataObj != null && !jQuery.isEmptyObject(chartDataObj) && Object.keys(chartDataObj).length > 1) {
								treeObj['domain'] = { x: [treeDomain, ((treeDomain + split) - 0.02)] };
								treeDomain = treeDomain + split + 0.02;
							}
							if (dataPropObject != null && !jQuery.isEmptyObject(dataPropObject)) {
								$.each(dataPropObject, function(key, val) {
									treeObj[key] = val;
								});
							}

							data.push(treeObj);
						}

					} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'histogram') {
						var colorCount = 0;
						colorArray = ['#1864ab', '#fd7e14', '#0b7285', '#ff6b6b'];
						$.each(chartDataObj, function(keyName, val) {
							var traceObj = {};
							var colorObj = {};
							if (keyName !== axisColumnName) {
								traceObj['x'] = chartDataObj[axisColumnName];
								traceObj['y'] = chartDataObj[keyName];
								traceObj['type'] = 'bar';
								traceObj = getChartLabelOrHoverDataFormatter('texttemplate', traceObj, chartDataObj[keyName], plotlyChartLabelType, 'x', 'y');
								traceObj = getChartLabelOrHoverDataFormatter('hovertemplate', traceObj, chartDataObj[keyName], plotlyChartHoverType, 'x', 'y');
								var keys = keyName.split("ASCOL");
								keyName = keys[0];
								traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
								$.each(dataPropObject, function(key, val) {
									if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
										var colorsArray = val['colors'];
										if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
											colorObj['color'] = colorsArray;
										} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
											colorObj['color'] = colorsArray[colorCount++];
										} else {
											colorObj['color'] = colorArray[colorCount++];
										}
										traceObj[key] = colorObj;
									} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
										colorObj['color'] = colorArray[colorCount++];
										traceObj[key] = colorObj;
									} else {
										traceObj[key] = val;
									}
								});
							}
							if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
								data.push(traceObj);
							}
						});
					} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'scatterpolar') {
						var colorCount = 0;
						var labelCount = 0;
						if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
							colorArray = JSON.parse(colorsObj)['clrs'];
						} else {
							colorArray = ["#EAC117", "#347C2C", "#806517", "#E66C2C", "#5C3317", "#C11B17"];
						}
						var i = 1;
						$.each(chartDataObj, function(keyName) {
							var traceObj = {};
							var colorObj = {};
							if (keyName !== axisColumnName) {
								traceObj['r'] = chartDataObj[keyName];
								traceObj['theta'] = chartDataObj[axisColumnName];
								traceObj['type'] = chartType;
								traceObj['fill'] = 'toself';
								var keys = keyName.split("ASCOL");
								keyName = keys[0];
								if (compareChartsFlag == 'YF') {
									traceObj['name'] = compareChartsLegendsTraceObj[chartId + '_name_' + i];
								} else {
									traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
								}
								i++;
								$.each(dataPropObject, function(key, val) {
									if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
										var colorsArray = val['colors'];
										if (colorsArray !== undefined && colorsArray !== null && !$.isArray(colorsArray)) {
											colorObj['color'] = colorsArray;
										} else if (colorsArray !== undefined && colorsArray !== null && $.isArray(colorsArray)) {
											colorObj['color'] = colorsArray[colorCount++];
										} else {
											colorObj['color'] = colorArray[colorCount++];
										}
										traceObj[key] = colorObj;
									} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
										colorObj['color'] = colorArray[colorCount++];
										traceObj[key] = colorObj;
									} else {
										traceObj[key] = val;
									}
								});
							}
							if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
								data.push(traceObj);
							}
						});
					} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'funnel') {
						var colorCount = 0;
						if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
							colorArray = JSON.parse(colorsObj)['clrs'];
						} else {
							colorArray = ["#EAC117", "#347C2C", "#806517", "#E66C2C", "#5C3317", "#C11B17"];
						}
						$.each(chartDataObj, function(keyName) {

							var traceObj = {};
							var colorObj = {};
							if (keyName !== axisColumnName) {
								traceObj['y'] = chartDataObj[axisColumnName];
								traceObj['x'] = chartDataObj[keyName];
								traceObj['type'] = chartType;
								traceObj = getChartLabelOrHoverDataFormatter('texttemplate', traceObj, chartDataObj[keyName], plotlyChartLabelType, 'y', 'x', "", currencySymbol);
								traceObj = getChartLabelOrHoverDataFormatter('hovertemplate', traceObj, chartDataObj[keyName], plotlyChartHoverType, 'y', 'x', "", currencySymbol);
								var keys = keyName.split("ASCOL");
								keyName = keys[0];
								traceObj = addlegendLabelToTrace(traceObj, keyName, defaultLegendNames, userProvidedLegendNames);
								$.each(dataPropObject, function(key, val) {
									if (key === 'marker' && !jQuery.isEmptyObject(val) && val !== null) {
										var colorsArray = val['colors'];
										if (colorsArray !== undefined && colorsArray !== null && colorsArray.length !== null) {
											colorObj['color'] = colorsArray[colorCount++];
										} else {
											colorObj['color'] = colorArray[colorCount++];
										}
										traceObj[key] = colorObj;
									} else if (key === 'marker' && (jQuery.isEmptyObject(val) || val === null)) {
										colorObj['color'] = colorArray[colorCount++];
										traceObj[key] = colorObj;
									} else {
										traceObj[key] = val;
									}
								});
							}
							if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
								data.push(traceObj);
							}
						});
					} else if (chartType != null && chartType != '' && chartType != undefined && chartType == 'indicator') {
						var traceObj = {};
						var domainObj = {};
						var domainArr = [];
						domainArr.push(0);
						domainArr.push(1);
						domainObj["x"] = domainArr;
						domainObj["y"] = domainArr;
						traceObj['domain'] = domainObj;
						traceObj['value'] = chartDataObj['data'];
						traceObj['type'] = chartType;
						traceObj['mode'] = "gauge+number";
						traceObj['gauge'] = resultObj['gauge'];
						if (dataPropObject != null && !jQuery.isEmptyObject(dataPropObject)) {
							$.each(dataPropObject, function(key, val) {
								traceObj[key] = val;
							});
						}
						if (traceObj !== null && !jQuery.isEmptyObject(traceObj)) {
							data.push(traceObj);
						}
					} else {
						dataObj['x'] = chartDataObj[0];
						dataObj['y'] = chartDataObj[key];
						dataObj['type'] = chartType;
						dataObj['name'] = 'value';
						dataObj['marker'] = colorObj;
					}

					var margin = {};
					if (chartType != null && chartType != '' && chartType != undefined && chartType == 'treemap') {
						margin = {
							l: 20,
							r: 20,
							b: 10,
							t: 40,
							pad: 0
						};
					} else if (chartType != null && chartType != '' && chartType != undefined && (chartType == 'pie' || chartType == 'donut')) {
						margin = {
							l: 20,
							r: 20,
							b: 10,
							t: 40,
							autoexpand: true,
							pad: 0
						};
					}
					else if (chartType != null && chartType != '' && chartType != undefined && (chartType == 'pie' || chartType == 'scatterpolar')) {
						margin = {
							l: 50,
							r: 50,
							b: 40,
							t: 60,
							pad: 0
						};
					}
					else {
						margin = {
							l: 35,
							r: 35,
							b: 40,
							t: 40
						};
					}

					layout = {
						margin: margin,
						height: 180,
						dragmode: false,
						font: {
							size: 10,
							color: '#000'
						},
						modebar: {
							orientation: 'v',
							color: '#0b4a99',
							activecolor: '#9ED3CD'
						},

						title: {
							text: title,
							font: {
								family: '"Open Sans", verdana, arial, sans-serif',
								size: 24
							},
							xref: 'paper',
							x: 0.1,
						}
					};
					var legend = {
						"x": -0.2,
						"y": 0.2,
						"orientation": "h"
					};
					if (layoutObj != null && !jQuery.isEmptyObject(layoutObj)) {
						$.each(layoutObj, function(key, val) {
							layout[key] = val;
						});
					}


					if (chartType != null && chartType != '' && chartType != undefined && (chartType == 'donut' || chartType == 'pie')) {
						//layout['showlegend'] = true;
						var updatemenus = [{
							type: 'buttons',
							buttons: [{
								label: '≡',
								method: 'relayout',
								args: ['showlegend', false],
								args2: ['showlegend', true]
							}]
						}];
						//                    layout['updatemenus'] = updatemenus;
						layout['legend'] = legend;
						layout['showlegend'] = false;
					} else {
						layout['height'] = 220;
					}
					var polar = {
						radialaxis: {
							visible: true
						},
						showlegend: true
					};
					if (chartType == 'scatterpolar') {
						layout['polar'] = polar;
						layout['dragmode'] = true;
					}
					if (chartType == 'histogram') {
						layout['bargap'] = '0';
						layout['barmode'] = "stack";
					}
					if (compareChartFlag == 'Y' || compareChartFlag == 'YF') {
						if (chartType == 'bar' || chartType == 'column') {
							layout['barmode'] = 'group';
							layout['showlegend'] = true;
						}
						$("#" + chartId).closest(".homeChartWrapDiv").attr("class", "col-md-6 col-sm-6 col-lg-6 homeChartWrapDiv");
						layout['legend'] = legend;
					} else if (compareChartFlag == 'N') {
						if (compareChartsArr != null && !jQuery.isEmptyObject(compareChartsArr) && compareChartsArr.indexOf(chartId) > -1) {
							var index = compareChartsArr.indexOf(chartId);
							$("#" + chartId).closest(".homeChartWrapDiv").attr("class", "col-md-6 col-sm-6 col-lg-3 homeChartWrapDiv");
							if (index > -1) {
								compareChartsArr.splice(compareChartsArr.indexOf(chartId), 1);
								if (compareChartsArr.length <= 0) {
									$("#visionDashBoardHomeFilterId").show();
									$("#visionDashBoardHomeCompareFilterId").hide();
								}
							}

						}

					}
					$("#" + chartId).empty();
					//					if (chartType != null && chartType != '' && chartType != undefined && (chartType != 'lines' && chartType != 'treemap' && chartType != 'bar' && chartType != 'waterfall')) {
					//						for (var m = 0; m < data.length; m++) {
					//							var markerObj = data[m]['marker'];
					//							if ((!(markerObj != null && !jQuery.isEmptyObject(markerObj))) ||
					//								(markerObj != null && !jQuery.isEmptyObject(markerObj) &&
					//									!(markerObj['colors'] != null && !jQuery.isEmptyObject(markerObj['colors'])))) {
					//								var colorObj = [];
					//								if (chartType == 'pie' || chartType == 'donut') {
					//									colorObj = ['#2F6345', '#40875E', '#58B07E', '#C48C00', '#F0AB00', '#FFBE1D', '#FFCC4B', '#827E32', '#A8A240', '#C5C169'];
					//									markerObj['colors'] = colorObj;
					//								} else {
					//									var x = data[m]['x'];
					//									colorObj = ['#1864B1', '#FF7F0E']
					//									var colorsX = [];
					//									if (x != null && x != '' && x != undefined && !jQuery.isEmptyObject(x)) {
					//										for (var p = 0; p < x.length; p++) {
					//											colorsX.push(colorObj[m]);
					//										}
					//										markerObj['color'] = colorsX;
					//									}
					//								}
					//								if (markerObj != null && !jQuery.isEmptyObject(markerObj)) {
					//									data[m]['marker'] = markerObj;
					//								}
					//							}
					//						}
					//					}
					if (isCurrencyConversionEvent !== undefined && isCurrencyConversionEvent !== ''
						&& isCurrencyConversionEvent === 'true') {
						var chartTraceArray = [];
						$.each(data, function(index, value) {
							var chartTraceData = value;
							if (chartType != null && chartType != '' && chartType != undefined
								&& (chartType == 'pie' || chartType == 'donut')) {
								var chartData = chartTraceData['values'];
								chartTraceData = getChartLabelOrHoverDataFormatter('texttemplate', chartTraceData, chartData, plotlyChartLabelType, 'label', 'value', isCurrencyConversionEvent, currencySymbol);
								chartTraceData = getChartLabelOrHoverDataFormatter('hovertemplate', chartTraceData, chartData, plotlyChartHoverType, 'label', 'value', isCurrencyConversionEvent, currencySymbol);
							} else {
								if (chartType == 'bar') {
									var chartData = chartTraceData['x'];
									chartTraceData = getChartLabelOrHoverDataFormatter('texttemplate', chartTraceData, chartData, plotlyChartLabelType, 'y', 'x', isCurrencyConversionEvent, currencySymbol);
									chartTraceData = getChartLabelOrHoverDataFormatter('hovertemplate', chartTraceData, chartData, plotlyChartHoverType, 'y', 'x', isCurrencyConversionEvent, currencySymbol);
								} else {
									var chartData = chartTraceData['y'];
									chartTraceData = getChartLabelOrHoverDataFormatter('texttemplate', chartTraceData, chartData, plotlyChartLabelType, 'x', 'y', isCurrencyConversionEvent, currencySymbol);
									chartTraceData = getChartLabelOrHoverDataFormatter('hovertemplate', chartTraceData, chartData, plotlyChartHoverType, 'x', 'y', isCurrencyConversionEvent, currencySymbol);
								}
							}
							chartTraceArray.push(chartTraceData);
						});
						data = chartTraceArray;
					}
					Plotly.newPlot(chartId, data, layout, config);

				} else {
					var parentId = $("#" + chartId).parents(".homeChartWrapDiv").attr("id");
					$("#" + parentId).remove();
				}
				var myPlot = document.getElementById(chartId);
				//                if (chartType != null && chartType != '' && chartType != undefined && (chartType == 'donut' || chartType == 'pie'))
				//                {
				//                    var height = (traceHeight * 19) + 10;
				//                    // $("#" + chartId).append("<div id='" + chartId + "_legendDivId' class='pieChartLegendTraces'></div>");
				//                    var traces = $("#" + chartId).find(".legend").find(".groups").html();
				//                    var groups = $("#" + chartId).find("g.infolayer").find("g.legend").find("g.scrollbox").find("g.groups");
				//                    $("#" + chartId).find(".legend").find(".groups").html("<foreignObject class='chartLegendScroll' width='353.05' height='75' style='overflow-y:scroll'>"
				//                            + "<span id='" + chartId + "_legendId' style='height:75px'></span></foreignObject>");
				//                    $("#" + chartId + "_legendId").attr("class", "pieLegendTraces");
				//                    $("#" + chartId + "_legendId").html("<svg class='legend_main-svg' "
				//                            + "height ='" + height + "' style='position:fixed;left:-12px;'>"
				//                            + traces + "</svg>");
				//                    //$("#" + chartId).find(".plot-container").find(".legend").hide();
				//
				//                }
				//                $('.homeChartParentDiv .chartMain .svg-container').unbind('scroll').on('scroll', function (event) {
				//                    if ($(this).scrollTop() + $(this).innerHeight() >= ($(this)[0].scrollHeight - 5)) {
				//                        var parentTarget = $(event.target).parent().parent();
				//                        var scrollChartId = parentTarget[0]['id']
				//                        var startIndex = $('#' + scrollChartId + '_startIndex').val();
				//                        var endIndex = $('#' + scrollChartId + '_endIndex').val();
				//                        var pageSize = $('#' + scrollChartId + '_pageSize').val();
				//                        if (parseInt(startIndex) >= 0) {
				//                            startIndex = endIndex;
				//                            endIndex = (parseInt(startIndex) + parseInt(pageSize)) - 1;
				//                            $("#" + scrollChartId + "_startIndex").val(startIndex);
				//                            $("#" + scrollChartId + "_endIndex").val(endIndex);
				//                            var TOTALUSERCOUNT = $("#" + scrollChartId + "_TotalChartCount").val();
				//                            if (TOTALUSERCOUNT != null && TOTALUSERCOUNT != '' && parseInt(TOTALUSERCOUNT) >= parseInt(startIndex)) {
				//                                getChartDataonScroll(scrollChartId, chartType)
				//                            }
				//                        }
				//                    }
				//                });

				if (chartType != null && chartType != '' && chartType != undefined && chartType == 'scatterpolar1' && myPlot != undefined && myPlot != null) {
					myPlot.on('plotly_hover', function(data) {
						var filterString = '';
						for (var i = 0; i < data.points.length; i++) {
							if (chartType != null && chartType == 'funnel') {
								filterString = data.points[i].y;
							} else {
								filterString = data.points[i].label;
							}

						}
						//getGridData(axix, filterString, chartId, "DXP_DASHBOARD_GRID", filterCondition);
						alert('Closest point clicked:\n\n');
					});
				} else {
					myPlot.on('plotly_click', function(data) {
						chartClickDataObj = {};
						chartClickDataObj[chartId] = data;
						if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
							selectHomeAggregateFunction(chartId, chartType, axisColumns, filterCondition, colorsObj, createcount);
						} else {
							colorsObj = '';
							selectHomeAggregateFunction(chartId, chartType, axisColumns, filterCondition, colorsObj, createcount);
						}
					});
				}


				if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
					var colorobj = JSON.parse(colorsObj);
					var pn = colorobj['pn'];
					var tn = colorobj['tn'];
					if (pn != null && pn != undefined && tn != null && tn != undefined) {
						applyChartColors(colorsObj, chartId, chartType);
					} else {
						var graphDiv = document.getElementById(chartId);
						var data = graphDiv.data;
						if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
							colorsObj = JSON.parse(colorsObj);
							if (colorsObj != null && !jQuery.isEmptyObject(colorsObj)) {
								/*var colors = colorsObj['clrs'];*/
								var colorarr = [];
								var n = colorsObj['clrs'].length;
								var len = 10;
								if (chartType == 'pie' || chartType == 'donut') {
									len = data[0]['labels'].length;
								} else if (chartType == 'scatterpolar') {
									len = data[0]['r'].length;
								} else {
									if (chartType !== 'indicator' && data.length == 1) {
										len = data[0]['x'].length;
									} else {
										len = data.length;
									}
								}
								for (var i = 0; i < len; i++) {
									var color = colorsObj['clrs'][i % n];
									colorarr.push(color);
								}
								if (colorarr != null && !jQuery.isEmptyObject(colorarr) && colorarr.length > 0) {
									var update;
									if (chartType != null && chartType != '' && chartType != undefined && (chartType == 'pie' || chartType == 'donut')) {
										update = {
											'marker': {
												'colors': colorarr
											}
										};
										Plotly.restyle(chartId, update);

									} else {
										var lenOfdata = data.length;
										if (lenOfdata == 1) {
											if (chartType != null && chartType != undefined && chartType == 'lines') {
												update = {
													'line': {
														'color': colorarr[0],
														width: 3
													}
												};
												Plotly.restyle(chartId, update);

											} else if (chartType != null && chartType != undefined && chartType == 'indicator') {

												update = {
													'gauge.bar.color': colorarr[0]
												};
											} else if (chartType != null && chartType != undefined && chartType == 'waterfall') {
												update = {
													'increasing': {
														'marker': {
															'color': colorarr[0]
														}
													}
												};
											} else if (chartType != null && chartType != undefined && chartType == 'scatterpolar') {
												update = {
													'fillcolor': colorarr[0]
												};
											} else {
												update = {
													'marker': {
														'color': colorarr
													}
												};

											}
											Plotly.restyle(chartId, update);
										}
										else {
											var updateData = data;
											try {
												updateData.forEach((value, index) => {
													if (chartType != null && chartType != undefined && chartType == 'indicator') {
														value['gauge']['bar']['color'] = colorarr[index % colorarr.length];
													} else {
														if (chartType != null && chartType != undefined && chartType == 'waterfall') {
															value['increasing']['marker']['color'] = colorarr[index % colorarr.length];
														} else {
															if (chartType != null && chartType != undefined && chartType == 'scatterpolar') {
																value['fillcolor'] = colorarr[index % colorarr.length];
															} else {
																if (chartType != null && chartType != undefined && (chartType == 'lines')) {
																	value['line']['color'] = colorarr[index % colorarr.length];

																} else {
																	value['marker']['color'] = colorarr[index % colorarr.length]
																}
															}
														}
													}
												});
												Plotly.newPlot(chartId, updateData, layout, config);
											} catch (err) {
												Plotly.newPlot(chartId, data, layout, config);
											}
										}
									}

								}
							}
						}
					}

				}



				$("#" + chartId + " .modebar-container").unbind("mouseenter").mouseenter(function() {
					//console.log("hovered");
					$(this).addClass("modeBarMainContainer");
				});

				$("#" + chartId + " .modebar-container").unbind("mouseleave").mouseleave(function() {
					//console.log("hovered");
					$(this).removeClass("modeBarMainContainer");
				});
				if (chartType != null && chartType != '' && chartType != undefined && (chartType == 'donut' || chartType == 'pie')) {
					var colors = [];
					if (colorsObj != null && colorsObj != '' && colorsObj != undefined) {
						var colorobj;
						if (colorsObj.constructor === String) {
							colorobj = JSON.parse(colorsObj);
						} else {
							colorobj = colorsObj;
						}

						if (colorobj != null && !jQuery.isEmptyObject(colorobj)) {
							colors = colorobj['clrs'];
						} else {
							colors = ['#2F6345', '#40875E', '#58B07E', '#C48C00', '#F0AB00', '#FFBE1D', '#FFCC4B', '#827E32', '#A8A240', '#C5C169'];
						}
					} else {
						colors = ['#2F6345', '#40875E', '#58B07E', '#C48C00', '#F0AB00', '#FFBE1D', '#FFCC4B', '#827E32', '#A8A240', '#C5C169'];
					}
					positionChartLegend(chartType, chartId, colors, chartLabels, data, layout, config);
					showExpandAndModebarButtons(chartType, layout, data, chartId, createcount, table, axix, chartLabels, filterCondition);
				} else {
					$("#" + chartId + "_legends").remove();
					showExpandAndModebarButtons(chartType, layout, data, chartId, createcount, table, axix, chartLabels, filterCondition);
				}




				// jaggu
				//                $("#" + chartId).draggable({
				$(".homeChartWrapDiv").draggable({
					//revert: true,
					refreshPositions: true,
					cursor: 'move',
					zindex: false,
					opacity: false,
					start: function(event, ui) {
						var charts = $(".homeChartWrapDiv");
						var zindexMaxVal = 999;
						$.each(charts, function(i, val) {
							var zIndex = $(this).css("z-index");
							if (zIndex != null && zIndex != '' && zIndex == 'auto') {
								zIndex = 999;
							}
							zIndex = parseInt(zIndex);
							if (zIndex > zindexMaxVal) {
								zindexMaxVal = zIndex
							}

						})
						var target = event.target;
						var chartDragId = target['id'];
						//                        $("#" + chartDragId).css("z-index", zindexMaxVal + 1);
					},
					stop: function(event, ui) {
						ui.helper.removeClass("draggableTable");
					}
				});
				//$("#visualizechartId").droppable({
				$(".homeChartWrapDiv").droppable({
					//                $("#" + chartId).droppable({
					revert: "invalid",
					refreshPositions: true,
					cursor: 'move',
					accept: '.homeChartWrapDiv',
					drop: function(event, ui) {

						/*var $this = $(this);
						var children = $(this).children();
						var draggable = $(ui.draggable);
						if (children.length > 0) {
							var move = children.detach();
							$(ui.draggable).parent().append(move);
							$(ui.draggable).append(children);
							
						}

						$(this).append($(ui.draggable).children()[0]);*/

						var $draggable = ui.draggable;
						var draggableId = ui.draggable.attr("id");
						var droppableId = $(this).attr("id");

						// Swap the HTML content of the dragged and dropped elements
						var temp1 = $("#" + droppableId).children().detach();
						var temp2 = $("#" + draggableId).children().detach();
						$("#" + droppableId).html(temp2);
						$("#" + draggableId).html(temp1);

						// Swap the IDs of the dragged and dropped elements
						$(this).attr("id", draggableId);
						ui.draggable.attr("id", droppableId);
						$draggable.animate({
							top: 0,
							left: 0
						});

						if (draggableId != null && droppableId != undefined && droppableId != null && droppableId != undefined) {
							updateSeqNoAfterSwap(draggableId, droppableId);
						}
					}
				});

				$("#" + chartId).parent().resizable();
				//var resizeData = '';
				if (resizeData != null && resizeData != '' && resizeData != undefined && resizeData != 'undefined' && resizeData != 'null') {

					var id = chartId;
					var type = chartType;
					var parentId = $("#" + chartId).parent().parent().parent().parent().parent().attr("id");
					$("#" + parentId).attr("class", "homeChartWrapDiv resizableChartDiv");
					var width = resizeData.split(":")[0];
					var height = resizeData.split(":")[1];
					if (type == 'lines') {
						margin = {
							l: 100,
							r: 50,
							b: 180,
							t: 20,
							pad: 4
						};
					} else {
						margin = {
							l: 100,
							r: 50,
							b: 120,
							t: 20,
							pad: 4
						};
					}

					var update =
					{
						width: width,
						height: height,
						//	margin: margin,

					}

					Plotly.relayout(chartId, update);
					$("#" + chartId + "_legends").css("width", width, "!important");

					/*var width = resizeData.split(":")[0];
					var height = resizeData.split(":")[1];
					var fontSize = ((width * height) / 1000);
					var parentElement = $("#" + chartId).parent().parent().parent().parent().parent().attr("id");
					$("#" + parentElement).css({ width: width + 'px', height: height + 'px' });
					$("#" + parentElement).trigger('resize');
					$("#" + parentElement).attr("class", "homeChartWrapDiv resizableChartDiv");

					var update =
					{
						width: width,
						height: height,
						//	margin: margin,

					}

					Plotly.relayout(chartId, update);
					$("#" + chartId + "_legends").css("width", width, "!important");*/

				}
				$("#" + chartId).parent().resizable();
				$("#" + chartId).parent().resize(function(event, ui) {
					var target = event.currentTarget;
					var id = target['id'];
					var type = event.currentTarget.firstChild.data[0].type;
					var parentId = $("#" + id).parent().parent().parent().parent().attr("id");
					//var parentId = $("#" + id).parent().parent().attr("id");
					$("#" + parentId).attr("class", "homeChartWrapDiv resizableChartDiv");
					var width = ui.size.width;
					var height = ui.size.height;
					if (type == 'lines') {
						margin = {
							l: 100,
							r: 50,
							b: 180,
							t: 20,
							pad: 4
						};
					} else {
						margin = {
							l: 100,
							r: 50,
							b: 120,
							t: 20,
							pad: 4
						};
					}
					//                    margin = calculateLegendMargins(chartId);
					if (type == 'pie' || type == 'donut') {
						height = height - 60;
					}
					var update =
					{
						width: width,
						height: height,
						//	margin: margin,

					}

					Plotly.relayout(chartId, update);
					$("#" + chartId + "_legends").css("width", width, "!important");
					$('#' + chartId + "_resizeData").val(width + ":" + height);
					var axisLabelCountToShow = 6;
					$("#" + chartId + " .svg-container").append("<div class='xAxisLabelTooltip'></div>");
					var currentChartXaxisLabelSelector = $("#" + chartId).find(".xaxislayer-above").children();
					currentChartXaxisLabelSelector.each(function(index, element) {
						var labelTitle = $(this).children().text();
						var result = labelTitle.slice(0, axisLabelCountToShow) + (labelTitle.length > axisLabelCountToShow ? "..." : "");
						$("#" + chartId + " .xAxisLabelTooltip").append('<span class="xlabelTooltipText">' + labelTitle + "</span>");
						$(this).children().text(result);

					});
					$("#" + chartId + " .xtick").unbind("mouseenter").mouseenter(function(e) {
						var cssTransformProp = $(this).children().attr("transform");
						var firstIndexOfTransformProp = cssTransformProp.split(",")[0];
						var indexOfTransformOpenPar = firstIndexOfTransformProp.indexOf("(");
						var transformHorStr = firstIndexOfTransformProp.substring(indexOfTransformOpenPar + 1, cssTransformProp.length);
						var transformHorVal = parseInt(transformHorStr) - 15;
						showAxisLabelsTooltipOnHover($(this), "xAxisLabelTooltip", chartId, transformHorVal, 0);
					});
					$("#" + chartId + " .xtick").unbind("mouseleave").mouseleave(function(e) {
						hideAxisLabelsTooltipOnHover($(this), "xAxisLabelTooltip", chartId);
					});

					$("#" + chartId + " .svg-container").append("<div class='yAxisLabelTooltip'></div>");
					var currentChartXaxisLabelSelector = $("#" + chartId).find(".yaxislayer-above").children();
					currentChartXaxisLabelSelector.each(function(index, element) {
						var labelTitle = $(this).children().text();
						var result = labelTitle.slice(0, axisLabelCountToShow) + (labelTitle.length > axisLabelCountToShow ? "..." : "");
						$("#" + chartId + " .yAxisLabelTooltip").append('<span class="ylabelTooltipText">' + labelTitle + "</span>");
						$(this).children().text(result);
					});
					$("#" + chartId + " .ytick").unbind("mouseenter").mouseenter(function(e) {
						var cssTransformProp = $(this).children().attr("transform");
						var firstIndexOfTransformProp = cssTransformProp.split(",")[1];
						var transformVerStr = firstIndexOfTransformProp.substring(0, firstIndexOfTransformProp.length - 1);
						var transformVerVal = parseInt(transformVerStr) - 230;
						showAxisLabelsTooltipOnHover($(this), "yAxisLabelTooltip", chartId, 0, transformVerVal);
					});
					$("#" + chartId + " .ytick").unbind("mouseleave").mouseleave(function(e) {
						hideAxisLabelsTooltipOnHover($(this), "yAxisLabelTooltip", chartId);
					});

					//$("#"+chartId+"_legends").css("height","60px","!important");
				});




				//shakir
				var axisLabelCountToShow = 6;
				$("#" + chartId + " .svg-container").append("<div class='xAxisLabelTooltip'></div>");
				var currentChartXaxisLabelSelector = $("#" + chartId).find(".xaxislayer-above").children();
				currentChartXaxisLabelSelector.each(function(index, element) {
					var labelTitle = $(this).children().text();
					var result = labelTitle.slice(0, axisLabelCountToShow) + (labelTitle.length > axisLabelCountToShow ? "..." : "");
					$("#" + chartId + " .xAxisLabelTooltip").append('<span class="xlabelTooltipText">' + labelTitle + "</span>");
					$(this).children().text(result);

				});
				$("#" + chartId + " .xtick").unbind("mouseenter").mouseenter(function(e) {
					var cssTransformProp = $(this).children().attr("transform");
					var firstIndexOfTransformProp = cssTransformProp.split(",")[0];
					var indexOfTransformOpenPar = firstIndexOfTransformProp.indexOf("(");
					var transformHorStr = firstIndexOfTransformProp.substring(indexOfTransformOpenPar + 1, cssTransformProp.length);
					var transformHorVal = parseInt(transformHorStr) - 15;
					showAxisLabelsTooltipOnHover($(this), "xAxisLabelTooltip", chartId, transformHorVal, 0);
				});
				$("#" + chartId + " .xtick").unbind("mouseleave").mouseleave(function(e) {
					hideAxisLabelsTooltipOnHover($(this), "xAxisLabelTooltip", chartId);
				});

				$("#" + chartId + " .svg-container").append("<div class='yAxisLabelTooltip'></div>");
				var currentChartXaxisLabelSelector = $("#" + chartId).find(".yaxislayer-above").children();
				currentChartXaxisLabelSelector.each(function(index, element) {
					var labelTitle = $(this).children().text();
					var result = labelTitle.slice(0, axisLabelCountToShow) + (labelTitle.length > axisLabelCountToShow ? "..." : "");
					$("#" + chartId + " .yAxisLabelTooltip").append('<span class="ylabelTooltipText">' + labelTitle + "</span>");
					$(this).children().text(result);
				});
				$("#" + chartId + " .ytick").unbind("mouseenter").mouseenter(function(e) {
					var cssTransformProp = $(this).children().attr("transform");
					var firstIndexOfTransformProp = cssTransformProp.split(",")[1];
					var transformVerStr = firstIndexOfTransformProp.substring(0, firstIndexOfTransformProp.length - 1);
					var transformVerVal = parseInt(transformVerStr) - 230;
					showAxisLabelsTooltipOnHover($(this), "yAxisLabelTooltip", chartId, 0, transformVerVal);
				});
				$("#" + chartId + " .ytick").unbind("mouseleave").mouseleave(function(e) {
					hideAxisLabelsTooltipOnHover($(this), "yAxisLabelTooltip", chartId);
				});
				//jaggu


			} else {
				var parentId = $("#" + chartId).parents(".homeChartWrapDiv").attr("id");
				$("#" + parentId).remove();
			}

		}, error: function(e) {
			console.log("The Error Message is:::" + e.message);
			sessionTimeout(e);
		}
	});

}

function getDashBoardMailAllImagesInPdf(dashBoardName) {
	var allImageContent = {};
	var i = 1;
	$(".chartBoardDashBoardMail").each(function() {
		var id = $(this).attr('id');
		if (id != null && id !== '' && id !== undefined) {
			if (!($("#" + id).hasClass("js-plotly-plot"))) {
				var img = new Image();
				var chartType = $(`#${id}_chartType`).val();
				if (['ganttChart', 'geochart', 'boxplot'].includes(chartType)) {
					var chartDiv = document.getElementById(id);
					var svg = chartDiv.getElementsByTagName('svg')[0];
					if (!(svg != null && svg != undefined)) {
						return true;
					}
					var canvas = document.createElement('canvas');
					if (!(canvas != null && canvas != undefined)) {
						return true;
					}
					var ctx = canvas.getContext('2d');
					if (!(ctx != null && ctx != undefined)) {
						return true;
					}
					canvas.width = svg.clientWidth;
					canvas.height = svg.clientHeight;

					var svgData = new XMLSerializer().serializeToString(svg);
					var svgBlob = new Blob([svgData], { type: 'image/svg+xml;charset=utf-8' });
					var url = URL.createObjectURL(svgBlob);
					if (!(url != null && url != undefined)) {
						return true;
					}
					var img = new Image();
					img.onload = function() {
						ctx.drawImage(img, 0, 0);
						allImageContent[i] = canvas.toDataURL('image/png');
						i++;
						URL.revokeObjectURL(url);
					};
					img.src = url;
				} else {
					// ECharts handling
					var dom = document.getElementById(id);
					var myChart = echarts.init(dom, null, {
						renderer: 'canvas',
						useDirtyRect: true
					});
					img.src = myChart.getDataURL({
						pixelRatio: 2,
						backgroundColor: '#fff',
						type: 'png'
					});
					allImageContent[i] = img.src;
					i++;
				}
			} else {

				var graphDiv = document.getElementById(id);
				Plotly.toImage(graphDiv, { format: 'png', height: 400, width: 500 }).then(function(url) {
					allImageContent[i] = url;
					i++;
				});
			}
		}
	});
	setTimeout(function() {
		if (allImageContent != null && !jQuery.isEmptyObject(allImageContent)) {
			$("#chartImagesDashBoardMailName").remove();
			$("#pdfDashBoardMailChartForm").append(`<input type="hidden" value="${dashBoardName}" id="chartImagesDashBoardMailName" name="chartImagesDashBoardMailName"/>`);
			$("#chartImagesDashBoardMailName").val(dashBoardName);
			$("#chartDashBoardMailImageObj").val(JSON.stringify(allImageContent));
			$("#pdfDashBoardMailChartForm").submit();
		}
	}, 1500);

}


function homeDashboardchartResize(chartId, resizeFlag) {
	var resizableDiv = "<div class='ui-resizable-handle ui-resizable-nw' id='nwgrip'></div>"
		+ "<div class='ui-resizable-handle ui-resizable-ne' id='negrip'></div>"
		+ "<div class='ui-resizable-handle ui-resizable-sw' id='swgrip'></div>"
		+ "<div class='ui-resizable-handle ui-resizable-se' id='segrip'></div>"
		+ "<div class='ui-resizable-handle ui-resizable-n' id='ngrip'></div>"
		+ "<div class='ui-resizable-handle ui-resizable-s' id='sgrip'></div>"
		+ "<div class='ui-resizable-handle ui-resizable-e' id='egrip'></div>"
		+ "<div class='ui-resizable-handle ui-resizable-w' id='wgrip'></div>";
	var parentDivId = $("#" + chartId).parents(".homeChartParentDiv").attr('id');
	if (resizeFlag != null && resizeFlag != '' && resizeFlag != undefined && resizeFlag === "Y") {
		$("#" + parentDivId).append(resizableDiv);
	} else if (resizeFlag != null && resizeFlag != '' && resizeFlag != undefined && resizeFlag === "N") {
		$("#" + parentDivId).find(".ui-resizable-handle").remove();
	}
	$("#" + chartId).parent().resizable({

		handles: {
			'nw': '#nwgrip',
			'ne': '#negrip',
			'sw': '#swgrip',
			'se': '#segrip',
			'n': '#ngrip',
			'e': '#egrip',
			's': '#sgrip',
			'w': '#wgrip'
		}, resize: function(event, ui) {
			var target = event.currentTarget;
			var id = chartId;
			var type = $("#" + chartId + "_chartType").val();
			var parentId = $("#" + id).parents(".homeChartWrapDiv").attr("id");
			//var parentId = $("#" + id).parent().parent().attr("id");
			$("#" + parentId).attr("class", "homeChartWrapDiv resizableChartDiv newClassResize ");
			var width = ui.size.width;
			var height = ui.size.height;
			if (type == 'lines') {
				margin = {
					l: 100,
					r: 50,
					b: 180,
					t: 20,
					pad: 4
				};
			} else {
				margin = {
					l: 100,
					r: 50,
					b: 120,
					t: 20,
					pad: 4
				};
			}
			//                    margin = calculateLegendMargins(chartId);
			if (type == 'pie' || type == 'donut') {
				height = height - 60;
			}
			var update =
			{
				width: width,
				height: height,
				//	margin: margin,

			}

			Plotly.relayout(chartId, update);
			$("#" + chartId + "_legends").css("width", width, "!important");
			$('#' + chartId + "_resizeData").val(width + ":" + height);
			var axisLabelCountToShow = 6;
			$("#" + chartId + " .svg-container").append("<div class='xAxisLabelTooltip'></div>");
			var currentChartXaxisLabelSelector = $("#" + chartId).find(".xaxislayer-above").children();
			currentChartXaxisLabelSelector.each(function(index, element) {
				var labelTitle = $(this).children().text();
				var result = labelTitle.slice(0, axisLabelCountToShow) + (labelTitle.length > axisLabelCountToShow ? "..." : "");
				$("#" + chartId + " .xAxisLabelTooltip").append('<span class="xlabelTooltipText">' + labelTitle + "</span>");
				$(this).children().text(result);

			});
			$("#" + chartId + " .xtick").unbind("mouseenter").mouseenter(function(e) {
				var cssTransformProp = $(this).children().attr("transform");
				var firstIndexOfTransformProp = cssTransformProp.split(",")[0];
				var indexOfTransformOpenPar = firstIndexOfTransformProp.indexOf("(");
				var transformHorStr = firstIndexOfTransformProp.substring(indexOfTransformOpenPar + 1, cssTransformProp.length);
				var transformHorVal = parseInt(transformHorStr) - 15;
				var dataRange = parseInt($("#" + chartId).attr("data-length")) > 2 ? -50 : 0;
				showAxisLabelsTooltipOnHover($(this), "xAxisLabelTooltip", chartId, transformHorVal, dataRange);
			});
			$("#" + chartId + " .xtick").unbind("mouseleave").mouseleave(function(e) {
				hideAxisLabelsTooltipOnHover($(this), "xAxisLabelTooltip", chartId);
			});

			$("#" + chartId + " .svg-container").append("<div class='yAxisLabelTooltip'></div>");
			var currentChartXaxisLabelSelector = $("#" + chartId).find(".yaxislayer-above").children();
			currentChartXaxisLabelSelector.each(function(index, element) {
				var labelTitle = $(this).children().text();
				var result = labelTitle.slice(0, axisLabelCountToShow) + (labelTitle.length > axisLabelCountToShow ? "..." : "");
				$("#" + chartId + " .yAxisLabelTooltip").append('<span class="ylabelTooltipText">' + labelTitle + "</span>");
				$(this).children().text(result);
			});
			$("#" + chartId + " .ytick").unbind("mouseenter").mouseenter(function(e) {
				var cssTransformProp = $(this).children().attr("transform");
				var firstIndexOfTransformProp = cssTransformProp.split(",")[1];
				var transformVerStr = firstIndexOfTransformProp.substring(0, firstIndexOfTransformProp.length - 1);
				var elementHeight = $("#" + chartId).outerHeight() - 10;
				var transformVerVal = parseInt(transformVerStr) - elementHeight;
				//var transformVerVal = parseInt(transformVerStr) - 230;
				showAxisLabelsTooltipOnHover($(this), "yAxisLabelTooltip", chartId, 0, transformVerVal);
			});
			$("#" + chartId + " .ytick").unbind("mouseleave").mouseleave(function(e) {
				hideAxisLabelsTooltipOnHover($(this), "yAxisLabelTooltip", chartId);
			});

			//$("#"+chartId+"_legends").css("height","60px","!important");  
		}
	});
	$("#" + chartId).parent().resizable({    // after resize function
		minHeight: 220,
		minWidth: 220,
		/*maxHeight: 400,
		maxWidth: 600*/
	});
}


function showExistingUsers(dashBoardName, liId) {
	var userName = $("#rsUserName").val();
	$.ajax({
		type: "POST",
		url: "getShareDashBoardUsersList",
		cache: false,
		data: {
			userName: userName
		},
		success: function(response) {
			//TODO: Need to add Loader
			if (response != null && !jQuery.isEmptyObject(response)) {
				var userNamesArr = response['userNamesArr'];
				var divHtml = "<div id='homePageShareDashBoardNameToUsersId' class='HomePageShareDashBoardNameToUsersClass'></div>"
					+ "<span class='homePageShareDashBoardNameToUsersspanclass'></span>";
				closeDialogBox("#homePageShareDashBoardNameToUsersdialog");
				$("body").append("<div id='homePageShareDashBoardNameToUsersdialog'></div>")
				$("#homePageShareDashBoardNameToUsersdialog").html(divHtml);
				$("#homePageShareDashBoardNameToUsersdialog").dialog({
					title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
					modal: true,
					width: 350,
					height: 200,
					fluid: true,
					buttons: [{
						text: (labelObject['Share'] != null ? labelObject['Share'] : 'Share'),
						click: function() {
							var items = $("#homePageShareDashBoardNameToUsersId").jqxListBox('getCheckedItems');
							var colsArray = [];
							if (items != null) {
								$.each(items, function(i) {
									colsArray.push(this.value);
								});
							}
							if (colsArray != null && !jQuery.isEmptyObject(colsArray)) {
								saveShareDashBoardUsersNames(dashBoardName, colsArray);
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
							} else {
								$(".homePageShareDashBoardNameToUsersspanclass").text("Please select atleast one username to share the DashBoard");
							}

						}

					},
					{
						text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
						click: function() {
							$(this).html("");
							$(this).dialog("close");
							$(this).dialog("destroy");
						}

					}],
					open: function() {
						$("#homePageShareDashBoardNameToUsersId").jqxListBox({
							source: userNamesArr,
							theme: 'energyblue',
							width: '200px',
							height: '30px',
							filterable: true,
							checkboxes: true,
							searchMode: "containsignorecase",
							width: 200,
							height: 200
						});

						$("#homePageShareDashBoardNameToUsersId").unbind('change').on('change', function(event) {
							$(".homePageShareDashBoardNameToUsersspanclass").text("");
						});
						$("#homePageShareDashBoardNameToUsersId").on('checkChange', function(event) {
							$(".homePageShareDashBoardNameToUsersspanclass").text("");
						});

						$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
						$(".visionHeaderMain").css("z-index", "999");
						$(".visionFooterMain").css("z-index", "999");
						$(".ui-dialog").addClass("editDashboardPopup");

					},
					beforeClose: function(event, ui) {
						$(".visionHeaderMain").css("z-index", "99999");
						$(".visionFooterMain").css("z-index", "99999");
					}
				});

			}

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}
function showExistingUsersMailDashBoard(selectedDashBoardName, liId) {
	var dashbordname;
	var item = $("#OptionDropdownData").jqxDropDownList('getSelectedItem');
	if (item != null) {
		dashbordname = item.value;
	}
	if (selectedDashBoardName != null && selectedDashBoardName != '' && selectedDashBoardName != undefined &&
		dashbordname != null && dashbordname != '' && dashbordname != undefined &&
		selectedDashBoardName != dashbordname) {
		getVisualizationchart(selectedDashBoardName);
	}
	var userName = $("#rsUserName").val();
	$.ajax({
		type: "POST",
		url: "getMailShareDashBoardUsersList",
		cache: false,
		data: {
			userName: userName
		},
		success: function(response) {
			//TODO: Need to add Loader
			if (response != null && !jQuery.isEmptyObject(response)) {
				var userNamesArr = response['userNamesArr'];
				var divHtml = "<div id='homePageShareDashBoardNameToUsersId' class='HomePageShareDashBoardNameToUsersClass'></div>"
					+ "<span class='homePageShareDashBoardNameToUsersspanclass'></span>";
				closeAllDialogsBoxes();
				$("body").append("<div id='homePageShareDashBoardNameToUsersdialog'></div>")
				$("#homePageShareDashBoardNameToUsersdialog").html(divHtml);
				$("#homePageShareDashBoardNameToUsersdialog").dialog({
					title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
					modal: true,
					width: 350,
					height: 200,
					fluid: true,
					resize: function(event, ui) {
						$(this).css({
							"max-width": "100%",
							"min-width": "350px",
							"min-height": "200px"
						});
					},
					buttons: [{
						text: (labelObject['Send Mail'] != null ? labelObject['Send Mail'] : 'Send Mail'),
						click: function() {
							showLoader();
							var items = $("#homePageShareDashBoardNameToUsersId").jqxListBox('getCheckedItems');
							var colsArray = [];
							if (items != null) {
								$.each(items, function(i) {
									colsArray.push(this.value);
								});
							}
							if (colsArray != null && !jQuery.isEmptyObject(colsArray)) {
								getAllImagesInPdf(selectedDashBoardName, colsArray);
								$(this).html("");
								$(this).dialog("close");
								$(this).dialog("destroy");
							} else {
								$(".homePageShareDashBoardNameToUsersspanclass").text("Please select atleast one username to share the DashBoard");
							}

						}

					},
					{
						text: (labelObject['Cancel'] != null ? labelObject['Cancel'] : 'Cancel'),
						click: function() {
							$(this).html("");
							$(this).dialog("close");
							$(this).dialog("destroy");
						}

					}],
					open: function() {
						$(this).css({
							"max-width": "100%",
							"min-width": "350px",
							"min-height": "200px",
							"overflow": "hidden",
						});
						$("#homePageShareDashBoardNameToUsersId").jqxListBox({
							source: userNamesArr,
							theme: 'energyblue',
							height: '30px',
							filterable: true,
							checkboxes: true,
							searchMode: "containsignorecase",
							width: '100%',
							//width: 200,
							height: 200
						});

						$("#homePageShareDashBoardNameToUsersId").unbind('change').on('change', function(event) {
							$(".homePageShareDashBoardNameToUsersspanclass").text("");
						});
						$("#homePageShareDashBoardNameToUsersId").on('checkChange', function(event) {
							$(".homePageShareDashBoardNameToUsersspanclass").text("");
						});

						$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
						$(".visionHeaderMain").css("z-index", "999");
						$(".visionFooterMain").css("z-index", "999");
						$(".ui-dialog").addClass("editDashboardPopup");

					},
					beforeClose: function(event, ui) {
						$(".visionHeaderMain").css("z-index", "99999");
						$(".visionFooterMain").css("z-index", "99999");
					}
				});

			}

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}

function saveShareDashBoardUsersNames(dashBoardName, usersArr) {
	$.ajax({
		type: "POST",
		url: "saveDashBoardUsersList",
		cache: false,
		data: {
			dashBoardName: dashBoardName,
			userNamesArr: JSON.stringify(usersArr)
		},
		success: function(response) {
			//TODO: Need to add Loader
			if (response != null && !jQuery.isEmptyObject(response)) {
				var message = response['message'];
				showMesgModelPopup(message);
			}

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});
}


function wrappedTitle(title, width) {
	if (title != null && title !== "" && title !== undefined) {
		// Adjust the available width by subtracting 140px
		const adjustedWidth = width - 100;

		// Create a temporary span element to measure text width
		const tempSpan = document.createElement('span');
		tempSpan.style.visibility = 'hidden';
		tempSpan.style.whiteSpace = 'nowrap';
		document.body.appendChild(tempSpan);

		// Split by words while keeping parentheses content intact, including spaces        
		const words = title.match(/\(.*?\)|[^\s()]+/g);

		let wrappedTitle = '';
		let line = '';

		for (let i = 0; i < words.length; i++) {
			const testLine = line + (line ? ' ' : '') + words[i];
			tempSpan.textContent = testLine;

			if (tempSpan.offsetWidth > adjustedWidth) {
				// Add the current line to the wrapped title and start a new line
				wrappedTitle += `${line.trim()}<br>`;
				line = words[i];
			} else {
				line = testLine;
			}
		}

		// Add the last line and a final break with an empty line
		wrappedTitle += line.trim() + "<br><br>";

		// Clean up
		document.body.removeChild(tempSpan);

		return wrappedTitle;
	}
	return '';
}


function getShareMailDashBoardList() {

	var userName = $("#rsUserName").val();
	$.ajax({
		type: "POST",
		url: "getShareMailDashBoardList",
		cache: false,
		data: {
			userName: userName
		},
		success: function(response) {
			//TODO: Need to add Loader
			if (response != null && !jQuery.isEmptyObject(response)) {
				var tableDiv = response['tableDiv'];
				if (tableDiv != null && tableDiv != '' && tableDiv != undefined) {
					var divHtml = "<div class='HomePageShareDashBoardNamesId'>" + tableDiv + "</div>";
					closeAllDialogsBoxes();
					$("#dialog").html(divHtml);
					$("#dialog").dialog({
						title: (labelObject['Message'] != null ? labelObject['Message'] : 'Message'),
						modal: true,
						width: 350,
						height: 200,
						fluid: true,
						resize: function(event, ui) {
							$(this).css({
								"max-width": "100%",
								"min-width": "350px",
								"min-height": "200px"
							});
						},
						open: function() {
							$(this).css({
								"max-width": "100%",
								"min-width": "350px",
								"min-height": "200px"
							});
							$('#dashBoard-sharenames-search').on('keyup', function() {
								var searchVal = $(this).val();
								var filterItems = $('[data-integralDashBoardNamesViewTablefilter-item]');

								if (searchVal != '') {
									filterItems.addClass('intelliSenseViewDashBoardNameshidden');
									$('[data-filter-name*="' + searchVal.toUpperCase() + '"]').removeClass('intelliSenseViewDashBoardNameshidden');
								} else {
									filterItems.removeClass('intelliSenseViewDashBoardNameshidden');
								}
							});
							$(this).closest(".ui-dialog").find(".ui-button").eq(1).addClass("dialogyes");
							$(".visionHeaderMain").css("z-index", "999");
							$(".visionFooterMain").css("z-index", "999");
							$(".ui-dialog").addClass("editDashboardPopup");

						},
						beforeClose: function(event, ui) {
							$(".visionHeaderMain").css("z-index", "99999");
							$(".visionFooterMain").css("z-index", "99999");
						}
					});
				}
			}

		},
		error: function(e) {
			console.log(e);
			sessionTimeout(e);
			stopLoader();
		}
	});

}
function createChartWithoutDrag() {
	showLoader();
	var parentDiv = $("#visionVisualizationDataChartViewId").find(".ui-resizable-handle").parent();
	if (parentDiv.length > 0) {
		var id = parentDiv[0]['id'];
		var chartCount;
		if (id != null && id != '' && id != undefined) {
			chartCount = id.replace("visionVisualizeChart", "");
		}
	}
	var chartId = "visionVisualizeChartId" + chartCount;
	var chartType = $("#visionVisualizeChart" + chartCount).attr("type");
	var childLength = $('#visualizeArea').find("div.ui-resizable-handle").length;
	if (childLength > 0) {
		proceedGetChart(chartType, chartId);
	}
	else {
		stopLoader();
		showMesg("Please select any chart to create");
	}


}

function echartHandleDragAndDropEvent() {
	$(".homeChartWrapDiv").draggable({
		//revert: true,
		refreshPositions: true,
		cursor: 'move',
		zindex: false,
		opacity: false,
		start: function(event, ui) {
			var charts = $(".homeChartWrapDiv");
			var zindexMaxVal = 3;
			var mainChartDivId = ui.helper[0]['id'];
			$.each(charts, function(i, val) {
				var thisChartMainId = $(this).attr('id');
				if (mainChartDivId != thisChartMainId) {
					if ($(this).hasClass(".newClassResize")) {
						$(this).css("z-index", "5", "!important");
					} else {
						$(this).css("z-index", "3", "!important");
					}
				}


			})
			var z_indexChart = $("#" + mainChartDivId).css("z-index");
			if (z_indexChart > 3 && z_indexChart <= 5) {
				z_indexChart = z_indexChart + 1;
				$(this).css("z-index", "" + z_indexChart + "", "!important");
			} else {
				$(this).css("z-index", "5", "!important");
			}

		},
		stop: function(event, ui) {
			ui.helper.removeClass("draggableTable");
			var mainChartDivId = ui.helper[0]['id'];

		}
	});


	//$("#visualizechartId").droppable({
	$(".homeChartWrapDiv").droppable({
		//                $("#" + chartId).droppable({
		revert: "invalid",
		refreshPositions: true,
		cursor: 'move',
		accept: '.homeChartWrapDiv',
		drop: function(event, ui) {

			var $draggable = ui.draggable;
			var draggableId = ui.draggable.attr("id");
			var droppableId = $(this).attr("id");


			if (draggableId != null && droppableId != undefined && droppableId != null && droppableId != undefined) {
				updateSeqNoAfterSwap(draggableId, droppableId);
			}
		}
	});
}


function showTermsAndConditions() {
	$('#termsAndConditionsDialog').remove();

	$('body').append(
		'<div id="termsAndConditionsDialog" class="termsConditionsClassId" title="Terms and Conditions">' +
		'<div>' +
		'<ul>' +
		'<li>' +
		'<div><span> 1.Payment Authorization:</span>By providing your payment details, you authorize us to charge the agreed subscription fee to your chosen payment method at the beginning of each billing cycle.<div>' +
		'<div><span> 2.No Refund Policy:</span> All payments made for subscriptions or services are final. We do not offer refunds under any circumstances. You acknowledge and accept that once a payment is processed, no refunds will be issued, regardless of whether you use the service or not.</div>' +
		'<div><span> 3.No Changes to Subscription Model:</span> Once the payment process is completed, the subscription model cannot be changed. You are committed to the subscription you have chosen, and any changes to the subscription model will only be allowed at the end of the current billing cycle.</div>' +
		'<div><span> 4.Automatic Renewal:</span> Your subscription will automatically renew at the end of each billing cycle, and the payment method on file will be charged for the next period unless you cancel your subscription prior to the renewal date.</div>' +
		'<div><span> 5.Cancellation of Subscription:</span> You may cancel your subscription at any time before the next billing cycle. However, cancellation will only take effect at the end of the current billing cycle, and no prorated refunds will be issued for unused time.</div>' +
		'<div><span> 6.Changes to Fees:</span> We reserve the right to modify the subscription fees, but you will be notified in advance if such changes occur. Continued use of the service after the fee change constitutes your acceptance of the new rates.</div>' +
		'<div><span> 7.Payment Failures:</span> If your payment method fails to process, you will be notified, and you may have a grace period to update your payment details. Failure to provide updated payment information may result in suspension or termination of your subscription.</div>' +
		'<div><span> 8.Security:</span> We take reasonable precautions to ensure that your payment information is secure, and we comply with relevant security standards to protect your data.</div>' +
		'</li>' +
		'</ul>' +
		'</div>' +
		'</div>'
	);

	$("#termsAndConditionsDialog").dialog({
		title: "Terms and Conditions",
		modal: true,
		width: 600,
		height: 600,
		buttons: {
			"Agree": function() {
				$("#paymentFormCheck").prop("checked", true);
				$(this).dialog("close");
			},
			"Cancel": function() {
				$(this).dialog("close");
			}
		},
		open: function() {
			$(".ui-dialog-content").css("overflow-x", "hidden");
			$(".ui-dialog-content").css("overflow-y", "auto");
		}
	});
}

function getMergeTablesAndLinks(tablesObj) {
	$("#userMergeTableColumnsDivIdDXP").flowchart({
		linkWidth: 2,
		defaultSelectedLinkColor: 'blue',
		grid: 10,
		distanceFromArrow: 0,
		multipleLinksOnInput: true,
		multipleLinksOnOutput: true,
		defaultSelectedLinkColor: 'blue'

	});
	var i = 0;
	var operators = {};
	var left = 0;
	var height = 0;
	$.each(tablesObj, function(key, val) {
		var tableName = key;
		var inputoutputObj = val;
		var inputs = inputoutputObj['inputs'];
		var outputs = inputoutputObj['outputs'];
		var operator = "operator" + i;
		var tableInputSize = Object.keys(inputs).length;
		var tableOutputSize = Object.keys(outputs).length;
		if (tableInputSize >= height) {
			height = tableInputSize;
		}
		if (tableOutputSize >= height) {
			height = tableOutputSize;
		}
		var operatorObj = {
			top: 20,
			left: left + 50,
			properties: {
				title: tableName,
				inputs: inputs,
				outputs: outputs,
			}
		};
		operators[operator] = operatorObj;
		if (i == 0 || i == ((Object.keys(tablesObj).length) - 1)) {
			left = 250;
		} else {
			left = 550;
		}
		i++;
		$("#userMergeTableColumnsDivIdDXP").flowchart('createOperator', operator, operatorObj);
	});
	height = (height + 2) * 2;
	var data = {
		operators: operators
	};

	$("#userMergeTableColumnsDivIdDXP").find(".flowchart-operator-title").show();
	$("#userMergeTableColumnsDivIdDXP").find(".flowchart-operator-connector-label").show();
	$("#userMergeTableColumnsDivIdDXP").find(".flowchart-operator").css("width", "auto", "!important");
	$("#userMergeTableColumnsDivIdDXP").find(".flowchart-operator").css("height", "auto", "!important");
	$("#userMergeTableColumnsDivIdDXP").find(".flowchart-operator-connector-arrow").css("top", "13px", "!important");
	var j = 0;
	var childLeft;
	$("#userMergeTableColumnsDivIdDXP div").find("div.flowchart-operator").each(function() {
		var nextDivWidth = $(this).width();
		var nextDivHeight = $(this).height();
		$(this).find(".flowchart-operator-connector-label").each(function() {
			const tempSpan = $("<span>").css({
				position: "absolute",
				visibility: "hidden",
				whiteSpace: "nowrap",
				font: $(this).css("font")
			}).text($(this).text()).appendTo("body");

			const textWidth = tempSpan.width();
			tempSpan.remove();

			if (textWidth > nextDivWidth) {
				nextDivWidth = textWidth;
			}
			/*		  $(this).find(".flowchart-operator-connector-label").css("width",nextDivHeight + "px", "!important");*/
		});
		var totalWidth;
		if (j == 0) {
			totalWidth = nextDivWidth + 130;
		} else if (j != 0) {
			totalWidth = childLeft + nextDivWidth + 80;
			$(this).css("left", childLeft, "!important");
		}
		j++;
		var tableWidth = nextDivWidth + 20;
		$(this).css("width", tableWidth + "px", "!important");
		$(this).css("height", nextDivHeight + "px", "!important");
		childLeft = totalWidth;
	});

	$("#userMergeTableColumnsDivIdDXP div").find("div.flowchart-operator").each(function() {
		$(this).draggable({
			containment: "#userMergeTableColumnsDivIdDXP",
		});
		$("#userMergeTableColumnsDivIdDXP").droppable({
			accept: '.flowchart-operator',
			drop: function(event, ui) {

				var uiDrag = ui.draggable[0];
				var droppedOn = $(this).find(".flowchart-operators-layer");
				var droppedHeight = $(uiDrag).height();
				$(uiDrag).appendTo(droppedOn);
				var childDiv = $("#userMergeTableColumnsDivIdDXP div").children('.flowchart-operator').last();
				$(childDiv[0]).css("top", "20px", "!important");
				$(childDiv[0]).css("height", droppedHeight + "px", "!important");
			}
		});
	});

	setTimeout(function() {//linkJoinOperators("#userMergeTableColumnsDivIdDXP");
	}, 2000);

	/*        var divHeight = $("#userMergeTableColumnsDivIdDXP").height();
			$("#userMergeTableColumnsDivIdDXP").css("height", divHeight + "px", "!important");*/


	$("#userMergeTableColumnsDivIdDXP").find('.flowchart-links-layer').css('height', `${height}rem`);
	$("#userMergeTableColumnsDivIdDXP").find('.flowchart-operators-layer').css('height', `${height}rem`);
	$("#userMergeTableColumnsDivIdDXP").find('.flowchart-temporary-link-layer').css('height', `${height}rem`);

}

function getMergeTableJoinColumns(replyId) {

	if (convAIMergeTables != null && !jQuery.isEmptyObject(convAIMergeTables)) {
		var values = convAIMergeTables;
		$("#userMergeTableNamesDivId").remove();
		$.ajax({
			type: "POST",
			url: "getUserMergeTableNamesColumns",
			cache: false,
			data: {
				tableNames: JSON.stringify(values),
				replyId: replyId
			},
			success: function(response) {
				//TODO: Need to add Loader
				/*if (response != null && !jQuery.isEmptyObject(response)) {
						var tableDiv = response['tableDiv'];
						var tablesObj = response['tablesObj'];
						$("#visionChartsAutoSuggestionUserId").append(tableDiv);
						$("#userMergeTableColumnsDivId").flowchart({
								linkWidth: 2,
								defaultSelectedLinkColor: 'blue',
								grid: 10,
								distanceFromArrow: 0,
								multipleLinksOnInput: true,
								multipleLinksOnOutput: true,
								defaultSelectedLinkColor: 'blue'

						});
						var i = 0;
						var operators = {};
						var left = 0;
						$.each(tablesObj, function(key, val) {
								var tableName = key;
								var inputoutputObj = val;
								var inputs = inputoutputObj['inputs'];
								var outputs = inputoutputObj['outputs'];
								var operator = "operator" + i;
								var operatorObj = {
										top: 20,
										left: left,
										properties: {
												title: tableName,
												inputs: inputs,
												outputs: outputs,
										}
								};
								operators[operator] = operatorObj;
								if (i == 0 || i == ((Object.keys(tablesObj).length) - 1)) {
										left = 250;
								}
								else {
										left = 550;
								}
								i++;
								$("#userMergeTableColumnsDivId").flowchart('createOperator', operator, operatorObj);
						});
						var data = {
										operators:operators
								};

						$(".flowchart-operator-title").show();
						$(".flowchart-operator-connector-label").show();
						$(".flowchart-operator").css("width", "auto", "!important");
						$(".flowchart-operator").css("height", "auto", "!important");
						$(".flowchart-operator-connector-arrow").css("top", "13px", "!important");
						var j = 0;
						var childLeft;
						$("#userMergeTableColumnsDivId div").find("div.flowchart-operator").each(function() {
								var nextDivWidth = $(this).width();
								var nextDivHeight = $(this).height();
								var totalWidth;
								if (j == 0) {
										totalWidth = nextDivWidth + 80;
								}
								else if (j != 0) {
										totalWidth = childLeft + nextDivWidth + 80;
										$(this).css("left", childLeft, "!important");
								}
								j++;
								var tableWidth = nextDivWidth + 20;
								$(this).css("width", tableWidth + "px", "!important");
								$(this).css("height", nextDivHeight + "px", "!important");
								childLeft = totalWidth;
						});

						$("#userMergeTableColumnsDivId div").find("div.flowchart-operator").each(function() {
								$(this).draggable({
										containment: "#userMergeTableColumnsDivId",
								});
								$("#userMergeTableColumnsDivId").droppable({
										accept: '.flowchart-operator',
										drop: function(event, ui) {

												var uiDrag = ui.draggable[0];
												var droppedOn = $(this).find(".flowchart-operators-layer");
												var droppedHeight = $(uiDrag).height();
												$(uiDrag).appendTo(droppedOn);
												var childDiv = $("#userMergeTableColumnsDivId div").children('.flowchart-operator').last();
												$(childDiv[0]).css("top", "20px", "!important");
												$(childDiv[0]).css("height", droppedHeight + "px", "!important");
										}
								});
						});
						showAnimatedBubbleSequnce();
						setTimeout(function() {
								linkJoinOperators();
						}, 2000);
						var divHeight = $("#userMergeTableColumnsDivId").height();
						$("#userMergeTableColumnsDivId").css("height", divHeight + "px", "!important");
						//scrollAreaToBottom(divHeight - 100);

				}*/


				if (response != null && !jQuery.isEmptyObject(response)) {
					var tableDiv = response['tableDiv'];
					var tablesObj = response['tablesObj'];
					$("#visionChartsAutoSuggestionUserId").append(tableDiv);
					$("#userMergeTableColumnsDivId").flowchart({
						linkWidth: 2,
						defaultSelectedLinkColor: 'blue',
						grid: 10,
						distanceFromArrow: 0,
						multipleLinksOnInput: true,
						multipleLinksOnOutput: true,
						defaultSelectedLinkColor: 'blue'

					});
					var i = 0;
					var operators = {};
					var left = 0;
					var height = 0;
					$.each(tablesObj, function(key, val) {
						var tableName = key;
						var inputoutputObj = val;
						var inputs = inputoutputObj['inputs'];
						var outputs = inputoutputObj['outputs'];
						var operator = "operator" + i;
						var tableInputSize = Object.keys(inputs).length;
						var tableOutputSize = Object.keys(outputs).length;
						if (tableInputSize >= height) {
							height = tableInputSize;
						}
						if (tableOutputSize >= height) {
							height = tableOutputSize;
						}
						var operatorObj = {
							top: 20,
							left: left,
							properties: {
								title: tableName,
								inputs: inputs,
								outputs: outputs,
							}
						};
						operators[operator] = operatorObj;
						if (i == 0 || i == ((Object.keys(tablesObj).length) - 1)) {
							left = 250;
						}
						else {
							left = 550;
						}
						i++;
						$("#userMergeTableColumnsDivId").flowchart('createOperator', operator, operatorObj);
					});
					height = (height + 2) * 2;
					var data = {
						operators: operators
					};

					$("#userMergeTableColumnsDivId").find(".flowchart-operator-title").show();
					$("#userMergeTableColumnsDivId").find(".flowchart-operator-connector-label").show();
					$("#userMergeTableColumnsDivId").find(".flowchart-operator").css("width", "auto", "!important");
					$("#userMergeTableColumnsDivId").find(".flowchart-operator").css("height", "auto", "!important");
					$("#userMergeTableColumnsDivId").find(".flowchart-operator-connector-arrow").css("top", "13px", "!important");
					var j = 0;
					var childLeft;
					$("#userMergeTableColumnsDivId div").find("div.flowchart-operator").each(function() {
						var nextDivWidth = $(this).width();
						var nextDivHeight = $(this).height();
						var totalWidth;
						if (j == 0) {
							totalWidth = nextDivWidth + 80;
						}
						else if (j != 0) {
							totalWidth = childLeft + nextDivWidth + 80;
							$(this).css("left", childLeft, "!important");
						}
						j++;
						var tableWidth = nextDivWidth + 20;
						$(this).css("width", tableWidth + "px", "!important");
						$(this).css("height", nextDivHeight + "px", "!important");
						childLeft = totalWidth;
					});

					$("#userMergeTableColumnsDivId div").find("div.flowchart-operator").each(function() {
						$(this).draggable({
							containment: "#userMergeTableColumnsDivId",
						});
						$("#userMergeTableColumnsDivId").droppable({
							accept: '.flowchart-operator',
							drop: function(event, ui) {

								var uiDrag = ui.draggable[0];
								var droppedOn = $(this).find(".flowchart-operators-layer");
								var droppedHeight = $(uiDrag).height();
								$(uiDrag).appendTo(droppedOn);
								var childDiv = $("#userMergeTableColumnsDivId div").children('.flowchart-operator').last();
								$(childDiv[0]).css("top", "20px", "!important");
								$(childDiv[0]).css("height", droppedHeight + "px", "!important");
							}
						});
					});
					showAnimatedBubbleSequnce();
					setTimeout(function() {
						linkJoinOperators();
					}, 2000);
					// var divHeight = $("#userMergeTableColumnsDivId").height();
					// $("#userMergeTableColumnsDivId").css("height", divHeight + "px", "!important");
					//scrollAreaToBottom(divHeight - 100);
					$("#userMergeTableColumnsDivId").find('.flowchart-links-layer').css('height', `${height}rem`);
					$("#userMergeTableColumnsDivId").find('.flowchart-operators-layer').css('height', `${height}rem`);
					$("#userMergeTableColumnsDivId").find('.flowchart-temporary-link-layer').css('height', `${height}rem`);

				}

			},
			error: function(e) {
				console.log(e);
				sessionTimeout(e);
				stopLoader();
			}
		});
	}
}


function getBarColor(actual, target, greenValue, yellowValue, redValue, greenColor, yellowColor, redColor) {
	var percentage = (actual / target) * 100;
	if (percentage >= greenValue) {
		return greenColor; // Green
	} else if (percentage >= yellowValue) {
		return yellowColor; // Orange
	} else if (percentage >= redValue) {
		return redColor; // Red
	} else {
		return 'rgba(169, 169, 169, 0.6)'; // Gray for cases below 0% if needed
	}
}


