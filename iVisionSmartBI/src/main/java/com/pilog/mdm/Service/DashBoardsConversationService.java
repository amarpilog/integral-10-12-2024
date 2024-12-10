package com.pilog.mdm.Service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.rowset.serial.SerialBlob;

import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Image;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.pdf.PdfWriter;
import com.itextpdf.xmp.impl.Base64;
import com.pilog.mdm.DAO.DashBoardsConversationDAO;
import com.pilog.mdm.pojo.BApplProperties;
import com.pilog.mdm.utilities.AuditIdGenerator;
import com.pilog.mdm.utilities.PilogUtilities;

import oracle.sql.BLOB;

import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 * @author Jagadish.R
 */

@Service
public class DashBoardsConversationService {

	@Value("${jdbc.driver}")
	private String dataBaseDriver;
	@Value("${jdbc.username}")
	private String userName;
	@Value("${jdbc.password}")
	private String password;
	@Value("${jdbc.url}")
	private String dbURL;

	@Value("${file.store.homedirectory}")
	private String fileStoreHomedirectory;

	@Value("${MultipartResolver.fileUploadSize}")
	private long maxFileSize;
	private int maxMemSize;

	@Autowired
	public DashBoardsConversationDAO conversationDAO;

	AuditIdGenerator auditIdGenerator = new AuditIdGenerator();
	private String etlFilePath;
	{
		System.out.print("path :::" + System.getProperty("os.name"));
		if ((System.getProperty("os.name") != null
				&& !(System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")))) {
			etlFilePath = "/u01/";
		} else {
			etlFilePath = "C://";
		}
	}

	public JSONObject getConversationalAIMessage(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = conversationDAO.getConversationalAIMessage(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getUserTableNames(HttpServletRequest request) {

		return conversationDAO.getUserTableNames(request);
	}

	public JSONObject getAILensInsightsUserExistTableNamesData(HttpServletRequest request) {

		return conversationDAO.getAILensInsightsUserExistTableNamesData(request);
	}

	public JSONObject getAILensInsightsAnalyticsQuestions(HttpServletRequest request) {

		return conversationDAO.getAILensInsightsAnalyticsQuestions(request);
	}

	public JSONObject getAILensInsightsAnalyticsInsights(HttpServletRequest request) {

		return conversationDAO.getAILensInsightsAnalyticsInsights(request);
	}

	public JSONObject getUserMergeTableNames(HttpServletRequest request) {

		return conversationDAO.getUserMergeTableNames(request);
	}

	public JSONObject getUserMergeTableNamesColumns(HttpServletRequest request) {

		return conversationDAO.getUserMergeTableNamesColumns(request);
	}

	public JSONObject getUserSearchData(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String message = (String) request.getParameter("message");
			String userName = (String) request.getParameter("userName");
			String lang = (String) request.getParameter("lang");
			String mainDiv = "";
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
			MultiValueMap<String, String> inputMap = new LinkedMultiValueMap();
			inputMap.add("msg", message);
			inputMap.add("user_name", userName);
			inputMap.add("lang", lang);

			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
					headers);
			RestTemplate template = new RestTemplate();
			ResponseEntity<JSONObject> response = template
					.postForEntity("http://idxp.pilogcloud.com:6653/google_search/", entity, JSONObject.class);
			JSONObject apiDataObj = response.getBody();

			resultObj.put("result", apiDataObj);

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getVoiceSuggestedChartsBasedonColumns(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = conversationDAO.getVoiceSuggestedChartsBasedonColumns(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject getInsightsView(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = conversationDAO.getInsightsView(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject executeInsightsSQLQuery(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			resultObj = conversationDAO.executeInsightsSQLQuery(request);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public String showAITypedValueResults(HttpServletRequest request) {
		String response = new String();

		try {
			String url = "https://api.openai.com/v1/chat/completions";
//            String apiKey = "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";
			String apiKey = "";
			String model = "gpt-3.5-turbo";

			try {
				BApplProperties applProperties = (BApplProperties) conversationDAO
						.getApplProperties("AI_LENS_SECRET_KEY");
				if (applProperties != null) {
					apiKey = applProperties.getId().getProcessValue();
				}
				String prompt = request.getParameter("aiTypedValue");

				// URL obj = new URL(url);
				HttpHeaders headers = new HttpHeaders();
				headers.set("Authorization", "Bearer " + apiKey);
				headers.setContentType(MediaType.APPLICATION_JSON);
				/*
				 * String body = "{\"model\": \"" + model +
				 * "\", \"messages\": [{\"role\": \"user\", \"content\": \"" + prompt + "\"}]}";
				 */
				
				String body = "{\"model\": \"" + model + "\", \"messages\": [{\"role\": \"user\", \"content\": \""
						+ prompt + ",don't mention any bi tool references in the response\"}]}";


				HttpEntity<String> req = new HttpEntity<>(body, headers);
				RestTemplate template = new RestTemplate();

				try {
					ResponseEntity<String> resp = template.postForEntity(url, req, String.class);
					if (resp.getStatusCode() == HttpStatus.OK) {
						return response = extractMessageFromJSONResponse(resp.toString());

					} else {
						return "Error: " + resp.getStatusCodeValue();
					}
				} catch (RestClientResponseException e) {
					return "Client-side error: " + e.getMessage();
				} catch (Exception e) {
					System.err.println("Server-side error: " + e.getMessage());
				}

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return response;
	}

	public static String extractMessageFromJSONResponse(String response) {
		int start = response.indexOf("content") + 11;

		int end = response.indexOf("\"", start);

		return response.substring(start, end);
	}

	public JSONArray loadIntialButtonsData(HttpServletRequest request) {
		return conversationDAO.loadIntialButtonsData(request);
	}

	public JSONObject getAILensNotifications(HttpServletRequest request) {
		return conversationDAO.getAILensNotifications(request);
	}

	public JSONObject fetchQuestionsFromDb(HttpServletRequest request) {
		return conversationDAO.fetchQuestionsFromDb(request);
	}

	public JSONObject getResultFromPythonApi(HttpServletRequest request) {
		return conversationDAO.getResultFromPythonApi(request);
	}

	public JSONObject showAITypedValueAnalyticsResults(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String message = (String) request.getParameter("aiTypedValue");
			String tableNameFlag = (String) request.getParameter("tableNameFlag");
			String source_language = (String) request.getParameter("source_language");
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
			String status = ((tableNameFlag != null && "true".equalsIgnoreCase(tableNameFlag)) ? "Table" : "");
			MultiValueMap<String, String> inputMap = new LinkedMultiValueMap();
			inputMap.add("input_str", message);
			inputMap.add("status", status);
			inputMap.add("name", (String) request.getSession(false).getAttribute("ssUsername"));
			inputMap.add("source_language", source_language);
			inputMap.add("session_id", AuditIdGenerator.genRandom32Hex());
			JSONObject dbDetails = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password,
					"DH101102");
			inputMap.add("user_name", userName);
			inputMap.add("password", password);
			inputMap.add("host", (String) dbDetails.get("HOST_NAME"));
			inputMap.add("port", (String) dbDetails.get("CONN_PORT"));
			inputMap.add("access_name", (String) dbDetails.get("CONN_DB_NAME"));

			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
					headers);
			RestTemplate template = new RestTemplate();

			ResponseEntity<JSONObject> response = template.postForEntity("http://idxp.pilogcloud.com:6661/db_search/",
					entity, JSONObject.class);

			/*
			 * ResponseEntity<JSONObject> response =
			 * template.postForEntity("http://172.16.1.62:6661/db_search/", entity,
			 * JSONObject.class);
			 */
			JSONObject apiDataObj = response.getBody();
			if (apiDataObj != null && !apiDataObj.isEmpty()) {
				if (apiDataObj.get("type") != null && "Sql query".equalsIgnoreCase((String) apiDataObj.get("type"))) {
					resultObj.put("chartResult", apiDataObj.get("query"));
					resultObj.put("type", apiDataObj.get("type"));
				} else if (apiDataObj.get("type") != null
						&& "Insights".equalsIgnoreCase((String) apiDataObj.get("type"))) {
					resultObj.put("type", apiDataObj.get("type"));
					if (status != null && !"".equalsIgnoreCase(status)) {
						resultObj.put("chartResult", apiDataObj.get("columns"));
					} else {
						resultObj.put("chartResult", apiDataObj.get("tables"));
					}
					resultObj.put("status", apiDataObj.get("status"));
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject showAILensInsightsResults(HttpServletRequest request) {
		JSONObject resultObj = new JSONObject();
		try {
			String tableName = (String) request.getParameter("tableName");
			String columns = (String) request.getParameter("columns");
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
			JSONObject inputObj = new JSONObject();
			inputObj.put("tableName", tableName);
			inputObj.put("columns", columns);
			MultiValueMap<String, String> inputMap = new LinkedMultiValueMap();

			inputMap.add("input_str", inputObj.toJSONString());
			inputMap.add("status", "True");
			inputMap.add("name", (String) request.getSession(false).getAttribute("ssUsername"));
			JSONObject dbDetails = new PilogUtilities().getDatabaseDetails(dataBaseDriver, dbURL, userName, password,
					"DH101102");
			inputMap.add("user_name", userName);
			inputMap.add("password", password);
			inputMap.add("host", (String) dbDetails.get("HOST_NAME"));
			inputMap.add("port", (String) dbDetails.get("CONN_PORT"));
			inputMap.add("access_name", (String) dbDetails.get("CONN_DB_NAME"));

			HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<MultiValueMap<String, String>>(inputMap,
					headers);
			RestTemplate template = new RestTemplate();
			ResponseEntity<JSONObject> response = template.postForEntity("http://idxp.pilogcloud.com:6661/db_search/",
					entity, JSONObject.class);
			JSONObject apiDataObj = response.getBody();
			if (apiDataObj != null && !apiDataObj.isEmpty()) {
				List insightsList = (List) apiDataObj.entrySet().stream().map(e -> ((Map.Entry) e).getKey())
						.collect(Collectors.toList());
				resultObj.put("insightList", insightsList);
				String insightsMainDiv = "<div class=\"accordion InsightsAccordionClassId\" id=\"accordionInsightsId\">";
				for (int i = 0; i < insightsList.size(); i++) {
					String insightType = (String) insightsList.get(i);
					if (insightType != null && !"".equalsIgnoreCase(insightType)
							&& "Compare".equalsIgnoreCase(insightType)) {
						insightsMainDiv += "<h3 onclick=showInsightsCompareData('" + insightType + "_divId_" + i + "','"
								+ insightType + "','" + tableName + "')>" + insightType + "</h3>";
						insightsMainDiv += "<div id='" + insightType + "_divId_" + i + "' class='insightsCompareClass'>"
								+ "<div id ='" + insightType + "_divId_" + i
								+ "_ddw' class='insightsCompareDropDownClass'></div>" + "<div id ='" + insightType
								+ "_divId_" + i + "_ddw_data' class='insightsCompareDropDownDataClass'></div>"
								+ "</div>";
						resultObj.put(insightType, conversationDAO.getCompareInsightsKeysData(insightType,
								(Map) apiDataObj.get(insightType), insightType, tableName));

					} else {
						insightsMainDiv += "<h3 onclick=showInsightsSummaryData('summary_divId_" + i + "','"
								+ insightType + "','" + tableName + "')>" + insightType + "</h3>";
						insightsMainDiv += "<div id='summary_divId_" + i + "' class='insightsCompareClass'>"
								+ "<div id ='" + insightType + "_divId_" + i
								+ "_data' class='insightsCompareDropDownClass'></div>" + "</div>";
					}
				}
				insightsMainDiv += "</div>";

				resultObj.put("insightsMainDiv", insightsMainDiv);

			}
//			if (apiDataObj != null && !apiDataObj.isEmpty()) {
//				List insightsList = (List) apiDataObj.entrySet().stream().map(e -> ((Map.Entry) e).getKey())
//						.collect(Collectors.toList());
//				resultObj.put("insightList", insightsList);
//			}
			resultObj.put("apiDataObj", apiDataObj);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return resultObj;
	}

	public JSONObject showAILensChartsSuggestions(HttpServletRequest request) {

		return conversationDAO.showAILensChartsSuggestions(request);
	}

	public JSONObject showDataLineageResults(HttpServletRequest request) {

		return conversationDAO.showDataLineageResults(request);
	}

	public JSONObject getAILensInsightsAnalyticsQuestionsData(HttpServletRequest request) {

		return conversationDAO.getAILensInsightsAnalyticsQuestionsData(request);
	}

	public JSONObject getAILensAnalyticsUserExistTableNamesData(HttpServletRequest request) {

		return conversationDAO.getAILensAnalyticsUserExistTableNamesData(request);
	}

	public JSONObject getAILensFirstHeaders(HttpServletRequest request) {
		return conversationDAO.getAILensFirstHeaders(request);
	}

	public void getAILensAnalyticsInsightsAudio(HttpServletRequest request, HttpServletResponse response) {
		conversationDAO.getAILensAnalyticsInsightsAudio(request, response);
	}

	public JSONObject saveSentDashBoardMailLastRun(HttpServletRequest request) {
		return conversationDAO.saveSentDashBoardMailLastRun(request);
	}

	public JSONObject getDashBoardsForMail(HttpServletRequest request) {
		return conversationDAO.getDashBoardsForMail(request);
	}

	public JSONObject importTreeDMPythonPDFFile(HttpServletRequest request, HttpServletResponse response,
			MultipartFile file1, String selectedFiletype) {
		JSONObject resultObj = new JSONObject();
		String filename = "";
		try {
			String excelFilePath = etlFilePath + "Files/TreeDMImport/"
					+ request.getSession(false).getAttribute("ssUsername");
			boolean isMultipart = ServletFileUpload.isMultipartContent(request);
			File file = new File(excelFilePath);
			if (file.exists()) {
				file.delete();
			}
			if (!file.exists()) {
				file.mkdirs();
			}
			DiskFileItemFactory factory = new DiskFileItemFactory();
			// maximum size that will be stored in memory
			factory.setSizeThreshold(maxMemSize);
			ServletFileUpload upload = new ServletFileUpload(factory);
			upload.setSizeMax(maxFileSize);
			List fileItems = upload.parseRequest(request);
			byte[] bytes = file1.getBytes();
			filename = file1.getOriginalFilename();
			System.out.println("filenAME:::" + filename);
			String fileType1 = filename.substring(filename.lastIndexOf(".") + 1, filename.length());
			String mainFileName = "SPIRUploadSheet" + System.currentTimeMillis() + "." + fileType1;
			selectedFiletype = selectedFiletype.toLowerCase();
			fileType1 = fileType1.toLowerCase();

			if (selectedFiletype != null && !"".equalsIgnoreCase(selectedFiletype) && fileType1 != null
					&& !"".equalsIgnoreCase(fileType1) && !selectedFiletype.equalsIgnoreCase(fileType1)) {
				resultObj.put("Message", "Please upload " + selectedFiletype + " files only");

			} else {
				if (filename != null) {
					if (filename.lastIndexOf(File.separator) >= 0) {

						file = new File(filename);
					} else {
						file = new File(excelFilePath + File.separator + mainFileName);
					}

					FileOutputStream osf = new FileOutputStream(file);

					osf.write(bytes);
					osf.flush();
					osf.close();

					HttpHeaders headers = new HttpHeaders();
					// headers.setContentType(MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"));
					headers.setContentType(MediaType.MULTIPART_FORM_DATA);
					headers.setAccept(Collections.singletonList(MediaType.MULTIPART_FORM_DATA));
					FileSystemResource fileData = new FileSystemResource(file);
					MultiValueMap inputMap = new LinkedMultiValueMap();
					inputMap.add("fileName", fileData);
					String dataCorrelaltionApiUrl = "http://apihub.pilogcloud.com:6673/edit_pdf";
					HttpEntity<MultiValueMap<String, Object>> entity = new HttpEntity<MultiValueMap<String, Object>>(
							inputMap, headers);
					RestTemplate template = new RestTemplate();
					ResponseEntity<byte[]> apiResponse = template.postForEntity(dataCorrelaltionApiUrl, entity,
							byte[].class);
					String outputFileName = "SPIRUploadSheet" + System.currentTimeMillis() + "." + fileType1;
					FileOutputStream fos = new FileOutputStream(
							new File(excelFilePath + File.separator + outputFileName));
					byte[] apiDataObj = apiResponse.getBody();
					Blob blobValue = new SerialBlob(apiDataObj);
					byte[] data = blobValue.getBytes(1, (int) ((java.sql.Blob) blobValue).length());
					if (apiDataObj != null) {
						fos.write(data);
						fos.flush();
						fos.close();

						resultObj.put("fileData",
								((etlFilePath != null && !"".equalsIgnoreCase(etlFilePath)
										&& !"".equalsIgnoreCase(etlFilePath) && etlFilePath.startsWith("C"))
												? "C:/Files/TreeDMImport/"
														+ request.getSession(false).getAttribute("ssUsername")
												: excelFilePath)
										+ File.separator + outputFileName);
					}

				} else {
					resultObj.put("Message", "File is not available, please check");
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return resultObj;
	}

	public void getPdfBasedonPath(HttpServletRequest request, HttpServletResponse response) {
		conversationDAO.getPdfBasedonPath(request, response);
	}

	public void downloadDashboardMailChartImageAllPDF(HttpServletRequest request, HttpServletResponse response) {
		OutputStream os = null;
		com.itextpdf.text.Document document = null;
		try {
			String dashBoardName = request.getParameter("chartImagesDashBoardMailName");
			String filename = "ALLCharts_" + dashBoardName + "_SPIRUploadSheet" + System.currentTimeMillis() + ".pdf";
//			String filename = "ALLCharts.pdf";
			File filelocation = new File(
					fileStoreHomedirectory + "/" + request.getSession(false).getAttribute("ssUsername"));
			if (!filelocation.exists()) {
				filelocation.mkdir();
			}
			String filepath = filelocation.getAbsolutePath() + File.separator + filename;

			response.reset();
			response.setContentType("application/pdf");
			response.setCharacterEncoding("UTF-8");
			response.setHeader("Content-disposition", "attachment; filename=\"" + filename + "\"");

			document = new com.itextpdf.text.Document(PageSize.A4, 50, 50, 50, 50);
			PdfWriter.getInstance(document, new FileOutputStream(filepath));
			document.open();

			String imgObjStr = request.getParameter("chartImageObj");

			if (imgObjStr != null && !imgObjStr.isEmpty()) {
				JSONObject imgObj = (JSONObject) JSONValue.parse(imgObjStr);

				if (imgObj != null && !imgObj.isEmpty()) {
					int x = 30;
					int y = 550;
					int key = 1;
					int maxHeight = 250;

					for (int i = 1; i <= imgObj.size(); i++) {
						String image = (String) imgObj.get(String.valueOf(i));
						if (image == null)
							continue;

						Image img = getImage(image);
						float width = img.getWidth();
						boolean largeImage = width > 1000;
						img.scaleAbsoluteWidth(largeImage ? 600f : 250f);
						img.scaleAbsoluteHeight(220f);
						img.setWidthPercentage(100);

						if (y - 30 < 0) {
							document.newPage();
							x = 30;
							y = 550;
						}

						img.setAbsolutePosition(x, y);
						document.add(img);

						x += largeImage ? 0 : 270;
						if (largeImage || key % 2 == 0) {
							y -= maxHeight + 10;
							x = 30;
						}

						key++;
					}

					document.close();

					os = response.getOutputStream();
					try (InputStream fis = new FileInputStream(filepath)) {
						byte[] bufferData = new byte[1024];
						int read;
						while ((read = fis.read(bufferData)) != -1) {
							os.write(bufferData, 0, read);
						}
					}

					try {

						Connection connection = null;
						PreparedStatement statement = null;
						connection = DriverManager.getConnection(dbURL, userName, password);
						File imageFile = new File(filepath);
						byte[] imageBytes = new byte[(int) imageFile.length()];
						FileInputStream fileInputStream = new FileInputStream(imageFile);
						fileInputStream.read(imageBytes);
						Blob blob = BLOB.createTemporary(connection, false, BLOB.DURATION_SESSION);
						blob.setBytes(1, imageBytes);
						String insertQuery = "INSERT INTO USER_DASHBOARD_MAIL(FILE_NAME, ATTACH_EXTENSION, ATTACH_TYPE, FILE_CONTENT, ACTIVE_FLAG, MESSAGE) VALUES (?, ?, ?, ?, ?, ?)";
						statement = connection.prepareStatement(insertQuery);
						statement.setObject(1, filename);// ORGN_ID
						statement.setObject(2, "PDF");// USER_NAME
						statement.setObject(3, "DOCUMENT");// FILE_ORG_NAME
						statement.setBlob(4, blob);// FILE_NAME
						statement.setObject(5, "Y");// FILE_PATH
						statement.setObject(6, "Pending");// FILE_TYPE
						int insertCount = statement.executeUpdate();
						System.out.println("insertCount::" + insertCount);
						fileInputStream.close();

					} catch (IOException | SQLException e) {
						e.printStackTrace();
					}

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (os != null)
					os.close();
				if (document != null && document.isOpen())
					document.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public Image getImage(String src) throws DocumentException {
		int pos = src.indexOf("base64,");
		Image image = null;
		try {
			if (src.startsWith("data") && pos > 0) {
				byte[] img = Base64.decode(src.substring(pos + 7).getBytes());
				image = Image.getInstance(img);

			} else {
				image = Image.getInstance(src);
			}
		} catch (IOException ex) {
			System.out.println("BadElementException :: " + ex.getMessage());
		}
		return image;
	}
	
	public JSONObject getShareDashBoardUsersList(HttpServletRequest request) {
		return conversationDAO.getShareDashBoardUsersList(request);
	}
	public JSONObject getMailShareDashBoardUsersList(HttpServletRequest request) {
		return conversationDAO.getMailShareDashBoardUsersList(request);
	}
	public JSONObject saveDashBoardUsersList(HttpServletRequest request) {
		return conversationDAO.saveDashBoardUsersList(request);
	}
	
	public void getDataBasedOnTemplateId(HttpServletRequest request, HttpServletResponse response) {
		conversationDAO.getDataBasedOnTemplateId(request, response);
    }

}
