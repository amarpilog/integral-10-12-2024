/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.service;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import jxl.format.Colour;
import jxl.write.WritableCellFormat;
import jxl.write.WritableFont;

import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.NumberToTextConverter;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.util.unit.DataSize;
import org.springframework.util.unit.DataUnit;
import org.springframework.web.multipart.MultipartFile;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Ravindar.P
 */
@Service
public class IVisionTransformFileProcessJobService {

    @Value("${file.storeFilePath}")
    private String storeFilePath;
    @Value("${MultipartResolver.fileUploadSize}")
    private long maxFileSize;
    private int maxMemSize;
    private String etlFilePath;
    {
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
            etlFilePath = "C://";
        } else {
            etlFilePath = "/u01/";
        }
    }

    @Autowired
    private IVisionTransformComponentUtilities componentUtilities;

    @Autowired
    private V10GenericDataPipingService dataPipingService;



//    public int insertIntoXLSXFile(
//            HttpServletRequest request,
//            JSONObject toOperator,
//            List<String> columnsList,
//            List totalData,
//            String filePath,
//            String fileName
//    ) {
//        int insertCount = 0;
//        try {
//            if (totalData != null && !totalData.isEmpty()) {
//                XSSFWorkbook wb = new XSSFWorkbook();
//                XSSFSheet sheet = wb.createSheet();
//
//                File file12 = new File(filePath);
//
//                if (!file12.exists()) {
//                    file12.mkdirs();
//                }
//                int cellIdx = 0;
//                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
//
//                XSSFRow hssfHeader = sheet.createRow(0);
//                WritableFont cellFont = new WritableFont(WritableFont.TIMES, 16);
//
//                WritableCellFormat cellFormat = new WritableCellFormat(cellFont);
//                cellFormat.setBackground(Colour.ORANGE);
//                XSSFCellStyle cellStyle = wb.createCellStyle();
//                cellStyle.setAlignment(HSSFCellStyle.ALIGN_CENTER);
//                cellStyle.setWrapText(true);
//                for (int i = 0; i < columnsList.size(); i++) {
//                    String get = columnsList.get(i);
//                    XSSFCell hssfCell = hssfHeader.createCell(cellIdx++);
//                    hssfCell.setCellStyle(cellStyle);
//
//                    hssfCell.setCellValue(String.valueOf(columnsList.get(i)));
//                }
//
//
//                XSSFRow hssfRow = null;
//                XSSFCell hSSFCell = null;
//                Object[] dataObj = new Object[columnsList.size()];
//                List subDataList = new ArrayList();
//
//                for (int k = totalData.size(); k > 0; k = k - 10000) {
//                    System.out.println("file iteration :: " + k);
//
////                    int endIndex = (insertCount + 10000) > totalData.size() ? totalData.size() : (insertCount + 10000);
//                    int startIndex = (k-10000) < 0 ? 0:(k-10000);
//                    int endIndex =k;
//                    subDataList = totalData.subList(startIndex, endIndex);
//                    int index = 0;
//                    for (int i = startIndex; i < endIndex; i++) {
//                        dataObj = (Object[]) subDataList.get(index);
//                        hssfRow = sheet.createRow(i + 1);
//
//                        if (dataObj != null && dataObj.length > 0) {
//                            cellIdx = 0;
//                            for (int j = 0; j < columnsList.size(); j++) {
//                                hSSFCell = hssfRow.createCell(cellIdx++);
//                                if (dataObj[j] != null && !"".equalsIgnoreCase(String.valueOf(dataObj[j]))) {
//                                    hSSFCell.setCellValue(String.valueOf(dataObj[j]));
//                                } else {
//                                    hSSFCell.setCellValue("");
//                                }
//
//                            }
//
//                        }
//
//                        insertCount++;
//                        index++;
//                    }
//
//                }
//                FileOutputStream outs = new FileOutputStream(outputFile);
//                wb.close();
//                outs.close();
//
//            }
//
////                wb.setSheetName(0, sheetName);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return insertCount;
//    }


    public int insertIntoXLSXFile(
            HttpServletRequest request,
            JSONObject toOperator,
            List<String> columnsList,
            List totalData,
            String filePath,
            String fileName
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty()) {
                XSSFWorkbook wb = new XSSFWorkbook();
                XSSFSheet sheet = wb.createSheet();

                File file12 = new File(filePath);

                if (!file12.exists()) {
                    file12.mkdirs();
                }
                int cellIdx = 0;
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);

                XSSFRow hssfHeader = sheet.createRow(0);
                WritableFont cellFont = new WritableFont(WritableFont.TIMES, 16);

                WritableCellFormat cellFormat = new WritableCellFormat(cellFont);
                cellFormat.setBackground(Colour.ORANGE);
                XSSFCellStyle cellStyle = wb.createCellStyle();
                cellStyle.setAlignment(HSSFCellStyle.ALIGN_CENTER);
                cellStyle.setWrapText(true);
                for (int i = 0; i < columnsList.size(); i++) {
                    String get = columnsList.get(i);
                    XSSFCell hssfCell = hssfHeader.createCell(cellIdx++);
                    hssfCell.setCellStyle(cellStyle);

                    hssfCell.setCellValue(String.valueOf(columnsList.get(i)));
                }
                XSSFRow hssfRow = null;
                XSSFCell hSSFCell = null;
                Object[] dataObj = new Object[columnsList.size()];

                List subDataList = new ArrayList();

                for (int k =0; k<totalData.size(); k=k+10000){
                    System.out.println("file iteration :: "+ k);

                    int endIndex = (insertCount+10000) > totalData.size() ? totalData.size() : (insertCount+10000);
                    subDataList = totalData.subList(k, endIndex);
                    int index = 0;
                    for (int i = k; i < endIndex; i++) {
                        dataObj = (Object[]) subDataList.get(index);
                        hssfRow = sheet.createRow(i + 1);

                        if (dataObj != null && dataObj.length > 0) {
                            cellIdx = 0;
                            for (int j = 0; j < columnsList.size(); j++) {
                                hSSFCell = hssfRow.createCell(cellIdx++);
                                if (dataObj[j] != null && !"".equalsIgnoreCase(String.valueOf(dataObj[j]))) {
                                    hSSFCell.setCellValue(String.valueOf(dataObj[j]));
                                } else {
                                    hSSFCell.setCellValue("");
                                }

                            }

                        }

                        insertCount++;
                        index++;
                    }

                }
                FileOutputStream outs = new FileOutputStream(outputFile);
                wb.write(outs);
                outs.close();

            }

//                wb.setSheetName(0, sheetName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return insertCount;
    }

    public int insertIntoXMLFile(HttpServletRequest request,
                                 JSONObject toOperator,
                                 List<String> columnsList,
                                 List totalData,
                                 String filePath,
                                 String fileName
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty()) {

                File file12 = new File(filePath);
                if (!file12.exists()) {
                    file12.mkdirs();
                }
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);
                if (outputFile.exists()) {
                    // file exist
                    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                    DocumentBuilder builder = factory.newDocumentBuilder();
                    Document document = builder.parse(new FileInputStream(outputFile), "UTF-8");
                    document.getDocumentElement().normalize();
                    Element root = document.getDocumentElement();
                    Element rootElement = document.getDocumentElement();
                    for (int i = 0; i < totalData.size(); i++) {
                        Object[] dataObj = (Object[]) totalData.get(i);
                        if (dataObj != null && dataObj.length > 0) {
                            Element itemElement = document.createElement("Item");
                            rootElement.appendChild(itemElement);
                            for (int j = 0; j < columnsList.size(); j++) {// columns data
                                String sourceColumnName = columnsList.get(j);
                                Element sourceColumnNameElement = document.createElement(sourceColumnName.replaceAll(":", "_").replaceAll(" ", "_"));

                                String cellvalue = "";
                                if (dataObj[j] != null && !"".equalsIgnoreCase(String.valueOf(dataObj[j]))) {
                                    cellvalue = String.valueOf(dataObj[j]);
                                }

                                if (cellvalue != null && !"".equalsIgnoreCase(cellvalue)) {
                                    cellvalue = cellvalue.replaceAll("Â", "");
                                    cellvalue = cellvalue.replaceAll("[^\\x00-\\x7F]", " ");
                                    cellvalue = cellvalue.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", " ");
                                    cellvalue = cellvalue.replaceAll("\\p{C}", " ");
                                    //  cellvalue = cellvalue.replaceAll("\\s+", " ");

                                }
                                sourceColumnNameElement.appendChild(document.createTextNode(escape(cellvalue)));
                                itemElement.appendChild(sourceColumnNameElement);

                            }// end of columns loop

                            root.appendChild(itemElement);
                            insertCount++;
                        }
                    }
                    DOMSource source = new DOMSource(document);
                    TransformerFactory transformerFactory = TransformerFactory.newInstance();
                    Transformer transformer = transformerFactory.newTransformer();
                    StreamResult result = new StreamResult(outputFile);
                    transformer.transform(source, result);
                } else {
                    String xmlString = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<PiLog_ETL_Data_Export>\n";
                    for (int i = 0; i < totalData.size(); i++) {
                        Object[] dataObj = (Object[]) totalData.get(i);
                        if (dataObj != null && dataObj.length > 0) {
                            xmlString += "<Item>";
                            for (int j = 0; j < columnsList.size(); j++) {// columns data
                                String sourceColumnName = columnsList.get(j);
                                String cellvalue = "";

                                if (dataObj[j] != null && !"".equalsIgnoreCase(String.valueOf(dataObj[j]))) {
                                    cellvalue = String.valueOf(dataObj[j]);
                                }

                                if (cellvalue != null && !"".equalsIgnoreCase(cellvalue)) {
                                    cellvalue = cellvalue.replaceAll("Â", "");
                                    cellvalue = cellvalue.replaceAll("[^\\x00-\\x7F]", " ");
                                    cellvalue = cellvalue.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", " ");
                                    cellvalue = cellvalue.replaceAll("\\p{C}", " ");
                                    //  cellvalue = cellvalue.replaceAll("\\s+", " ");

                                }
                                xmlString += "<" + sourceColumnName.replaceAll(":", "_").replaceAll(" ", "_") + ">"
                                        + "" + escape(cellvalue) + ""
                                        + "</" + sourceColumnName.replaceAll(":", "_").replaceAll(" ", "_") + ">\n";

                            }// end of columns loop

                            xmlString += "</Item>\n";

                        }
                        insertCount++;
                    }// end of data loop
                    xmlString += "</PiLog_ETL_Data_Export>\n";
                    FileOutputStream outs = new FileOutputStream(outputFile);
                    outs.write(xmlString.getBytes("UTF-8"));
                    outs.close();

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return insertCount;
    }

    public int insertIntoTextOrCSVFile(HttpServletRequest request,
                                       JSONObject toOperator,
                                       List<String> columnsList,
                                       List totalData,
                                       String filePath,
                                       String fileName
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty()) {

                List<String[]> writeFileDataList = new ArrayList<>();
                File file12 = new File(filePath);

                if (!file12.exists()) {
                    file12.mkdirs();
                }
                int cellIdx = 0;
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);

                String columnsString = StringUtils.collectionToDelimitedString(columnsList, ":::");
                writeFileDataList.add(columnsString.split(":::"));

                for (int i = 0; i < totalData.size(); i++) {
                    Object[] dataObj = (Object[]) totalData.get(i);
                    String dataString = "";
                    if (dataObj != null && dataObj.length > 0) {
                        for (int j = 0; j < columnsList.size(); j++) {// columns data
                            String cellvalue = "";
                            if (dataObj[j] != null && !"".equalsIgnoreCase(String.valueOf(dataObj[j]))) {
                                cellvalue = String.valueOf(dataObj[j]);
                            }

                            if (cellvalue != null
                                    && !"".equalsIgnoreCase(cellvalue)
                                    && !"null".equalsIgnoreCase(cellvalue)) {
                                cellvalue = cellvalue.replaceAll("Â", "");
                            }

                            dataString += cellvalue;
                            if (j != columnsList.size() - 1) {
                                dataString += ":::";
                            }
                        }// end of columns loop

                        if (dataString != null && !"".equalsIgnoreCase(dataString)) {
                            writeFileDataList.add(dataString.split(":::"));
                            dataString = "";
                        }

                    }
                    insertCount++;
                }// end of data loop

                FileOutputStream fos = new FileOutputStream(outputFile, true);
                fos.write(0xef);
                fos.write(0xbb);
                fos.write(0xbf);
                CSVWriter writer = new CSVWriter(new OutputStreamWriter(fos, "UTF-8"), '\t');
                writer.writeAll(writeFileDataList, false);
                writer.close();

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return insertCount;
    }

    public int insertIntoJSONFile(HttpServletRequest request,
                                  JSONObject toOperator,
                                  List<String> columnsList,
                                  List totalData,
                                  String filePath,
                                  String fileName
    ) {
        int insertCount = 0;
        try {
            if (totalData != null && !totalData.isEmpty()) {

                List<String[]> writeFileDataList = new ArrayList<>();
                File file12 = new File(filePath);

                if (!file12.exists()) {
                    file12.mkdirs();
                }
                int cellIdx = 0;
                File outputFile = new File(file12.getAbsolutePath() + File.separator + fileName);

                String jsonDataStr = "";
                JSONArray totalDataArray = new JSONArray();

                jsonDataStr += "[";

                for (int i = 0; i < totalData.size(); i++) {
                    Object[] dataObj = (Object[]) totalData.get(i);
                    String dataString = "";
                    if (dataObj != null && dataObj.length > 0) {
                        JSONObject jsonDataObj = new JSONObject();
                        for (int j = 0; j < columnsList.size(); j++) {// columns data
                            String cellvalue = "";
                            if (dataObj[j] != null && !"".equalsIgnoreCase(String.valueOf(dataObj[j]))) {
                                cellvalue = String.valueOf(dataObj[j]);
                            }

                            if (cellvalue != null
                                    && !"".equalsIgnoreCase(cellvalue)
                                    && !"null".equalsIgnoreCase(cellvalue)) {
                                cellvalue = cellvalue.replaceAll("Â", "");
                            }
                            jsonDataObj.put(columnsList.get(j), cellvalue);
                            //
                        }// end of columns loop

                        jsonDataStr += jsonDataObj.toJSONString();
                        if (i != totalData.size() - 1) {
                            jsonDataStr += ",";
                        }

                    }
                    insertCount++;
                }// end of data loop

                FileOutputStream fos = new FileOutputStream(outputFile, true);
                OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
                BufferedWriter writer = new BufferedWriter(osw);
                writer.append(jsonDataStr);
                writer.close();
                osw.close();
                fos.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return insertCount;
    }

    // method for replaceing escape charecters with character references
    public String escape(String string) {
        StringBuffer sb = new StringBuffer();
        int i = 0;
        for (int length = string.length(); i < length; i++) {
            char c = string.charAt(i);
            switch (c) {
                case '&':
                    sb.append("&amp;");
                    break;
                case '<':
                    sb.append("&lt;");
                    break;
                case '>':
                    sb.append("&gt;");
                    break;
                case '"':
                    sb.append("&quot;");
                    break;
                case '\'':
                    sb.append("&apos;");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }

    public List readExcelFile(
            HttpServletRequest request,
            String filepath,
            String fileName
    ) {

        FileInputStream fis = null;
        List dataList = new ArrayList();
        int rowVal = 1;
        try {
            Workbook workBook = WorkbookFactory.create(new File(filepath));
            Sheet sheet = null;
            int noOfSheets = workBook.getNumberOfSheets();

            String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());

            if (workBook.getSheetAt(0) instanceof XSSFSheet) {
                sheet = (XSSFSheet) workBook.getSheetAt(0);
            } else if (workBook.getSheetAt(0) instanceof HSSFSheet) {
                sheet = (HSSFSheet) workBook.getSheetAt(0);
            }
            int lastRowNo = sheet.getLastRowNum();
            int firstRowNo = sheet.getFirstRowNum();
//                System.out.println("firstRowNo::::" + firstRowNo);
            int rowCount = lastRowNo - firstRowNo;
//                System.out.println("rowCount:::::" + rowCount);
            Row headerRow = sheet.getRow(0);
            for (int i = rowVal; i <= lastRowNo; i++) {
                Row row = sheet.getRow(i);
                if (row != null) {
//                        JSONObject dataObject = new JSONObject();
//                    Object[] dataObject = new Object[row.getLastCellNum()];
                    Object[] dataObject = new Object[headerRow.getLastCellNum()];
                    // JSONObject dataObject = new JSONObject();
                    //  dataObject.put("totalrecords", rowCount);
                    for (int cellIndex = 0; cellIndex < headerRow.getLastCellNum(); cellIndex++) {

                        try {
//                            System.out.println("cellIndex::::" + cellIndex);
                            Cell cell = row.getCell(cellIndex);
                            if (cell != null) {
                                switch (cell.getCellType()) {
                                    case Cell.CELL_TYPE_STRING:
                                        String cellValue = cell.getStringCellValue();
                                        if (cellValue != null && !"".equalsIgnoreCase(cellValue) && !"null".equalsIgnoreCase(cellValue)) {
//                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellValue);
                                            dataObject[cellIndex] = cellValue;
                                        } else {
                                            dataObject[cellIndex] = "";
                                        }

                                        break;
                                    case Cell.CELL_TYPE_BOOLEAN:
//                                rowObj.put(header, hSSFCell.getBooleanCellValue());
                                        break;
                                    case Cell.CELL_TYPE_NUMERIC:

                                        if (HSSFDateUtil.isCellDateFormatted(cell)) {
                                            CellStyle cellStyle = cell.getCellStyle();
                                            Date cellDate = cell.getDateCellValue();
                                            String cellDateString = "";
                                            if ((cellDate.getYear() + 1900) == 1899 && (cellDate.getMonth() + 1) == 12 && (cellDate.getDate()) == 31) {
                                                cellDateString = (cellDate.getHours()) + ":" + (cellDate.getMinutes()) + ":" + (cellDate.getSeconds());
//                                                    System.out.println("cellDateString :: "+cellDateString);
                                            } else {
                                                cellDateString = (cellDate.getYear() + 1900) + "-" + (cellDate.getMonth() + 1) + "-" + (cellDate.getDate()) + " " + (cellDate.getHours()) + ":" + (cellDate.getMinutes()) + ":" + (cellDate.getSeconds());
                                            }

//                                                dataObject.put(fileName + ":" + columnList.get(cellIndex), cellDateString);
                                            dataObject[cellIndex] = cellDateString;

                                        } else {
                                            String cellvalStr = NumberToTextConverter.toText(cell.getNumericCellValue());
                                            dataObject[cellIndex] = cellvalStr;
                                        }
                                        break;
                                    case Cell.CELL_TYPE_BLANK:
                                        dataObject[cellIndex] = "";
                                        break;
                                }

                            } else {
                                dataObject[cellIndex] = "";
                            }
                        } catch (Exception e) {
                            dataObject[cellIndex] = "";
                            continue;
                        }

                    }// end of row cell loop
                    dataList.add(dataObject);
                }

            }// row end

            // return result1;
            if (fis != null) {
                fis.close();
            }

        } catch (Exception e) {
            e.printStackTrace();

        }

        return dataList;
    }

    public List readCSVFile(
            HttpServletRequest request,
            String filepath,
            String fileName,
            String fileType,
            List columnList) {
//        List columnList = getHeadersOfImportedFile(filepath, fileType);
        FileInputStream fis = null;
//        System.out.println("Start Date And Time :::" + new Date());
        List dataList = new ArrayList();
        int rowVal = 1;
        try {

            int rowCount = 0;
            //  fis = new FileInputStream(new File(filepath));
//            char columnSeparator = '\t';
//            char columnSeparator = ',';
            CsvParserSettings settings = new CsvParserSettings();
            settings.detectFormatAutomatically();

            CsvParser parser = new CsvParser(settings);
            List<String[]> rows = parser.parseAll(new File(filepath));

            // if you want to see what it detected
            CsvFormat format = parser.getDetectedFormat();
            char columnSeparator = format.getDelimiter();

            if (".json".equalsIgnoreCase(fileType)) {
                columnSeparator = ',';
            }
            CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filepath), "UTF8"), columnSeparator);
            LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(filepath));
            String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
//            System.out.println("fileExtension:::" + fileExtension);

            int stmt = 1;
            String strToDateCol = "";
            // need to write logic for extraction from File
//            CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filepath), "UTF8"), columnSeparator);
//            LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(filepath));
            lineNumberReader.skip(Long.MAX_VALUE);
            long totalRecords = lineNumberReader.getLineNumber();
            if (totalRecords != 0) {
                totalRecords = totalRecords - 1;
            }

            rowVal = 1;

            int skipLines = 0;

            if (skipLines == 0) {
                String[] headers = reader.readNext();
                if (headers.length != 0 && headers[0].contains("" + columnSeparator)) {
                    headers = headers[0].split("" + columnSeparator);
                }
            }
            reader.skip(skipLines);

            String[] nextLine;
            int rowsCount = 1;
            while ((nextLine = reader.readNext()) != null) {// no of rows
                if (nextLine.length != 0 && nextLine[0].contains("" + columnSeparator)) {
                    nextLine = nextLine[0].split("" + columnSeparator);
                }

//                    JSONObject dataObject = new JSONObject();
                Object[] dataObject = new Object[columnList.size()];
                //dataObject.put("totalrecords", totalRecords);
                for (int j = 0; j < columnList.size(); j++) {
                    try {
                        int cellIndex = j;
                        if (cellIndex <= (nextLine.length - 1)) {
                            String token = nextLine[cellIndex];
                            if (token != null
                                    && !"".equalsIgnoreCase(token)) {
                                try {
//                                        dataObject.put(fileName + ":" + columnList.get(j), token);
                                    dataObject[j] = token;
                                } catch (Exception e) {
                                    dataObject[j] = "";
                                    continue;
                                }
                            } else {
                                dataObject[j] = "";
                            }
                        } else {
                            dataObject[j] = "";
                        }
                    } catch (Exception e) {
                        dataObject[j] = "";
                        continue;
                    }

                }

                dataList.add(dataObject);

            }

            if (fis != null) {
                fis.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return dataList;
    }

    public List readXMLFile(
            HttpServletRequest request,
            String filepath,
            String fileName) {

//        columnList = getHeadersOfImportedFile(filePath, fileType);
        FileInputStream fis = null;
        List dataList = new ArrayList();
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new FileInputStream(filepath), "UTF-8");
            int rowCount = 0;
            String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
//            System.out.println("fileExtension:::" + fileExtension);

            document.getDocumentElement().normalize();
            Element root = document.getDocumentElement();

            if (root.hasChildNodes() && root.getChildNodes().getLength() > 1) {
                // nested childs
                String evaluateTagName = "/" + root.getTagName();
                NodeList rootList = root.getChildNodes();
                if (!"#Text".equalsIgnoreCase(rootList.item(0).getNodeName())) {
                    evaluateTagName += "/" + rootList.item(0).getNodeName();
                } else {
                    evaluateTagName += "/" + rootList.item(1).getNodeName();
                }

                System.out.println("evaluateTagName:::" + evaluateTagName);
                XPath xpath = XPathFactory.newInstance().newXPath();
                NodeList dataNodeList = (NodeList) xpath.evaluate(evaluateTagName,
                        //            NodeList nList = (NodeList) xpath.evaluate("/PiLog_Data_Export/Item",
                        document,
                        XPathConstants.NODESET);

                if (dataNodeList != null && dataNodeList.getLength() != 0) {
                    rowCount = dataNodeList.getLength();

//                    Node headerNode = dataNodeList.item(0);
//                    if (headerNode.getNodeType() == Node.ELEMENT_NODE) {
//                        NodeList headerChildNodeList = headerNode.getChildNodes();
//                        int index = 0;
//                        for (int i = 0; i < headerChildNodeList.getLength(); i++) {// Columns
//                            Node childNode = headerChildNodeList.item(i);
//                            if (childNode != null
//                                    && childNode.getNodeType() == Node.ELEMENT_NODE) {
//                                headerData.put(childNode.getNodeName(), i);
//
//                            }
//                        }// end of columns loop
//
//                    }
                    for (int temp = 0; temp < rowCount; temp++) {// Rows
                        Node node = dataNodeList.item(temp);

                        if (node != null && node.getNodeType() == Node.ELEMENT_NODE) {
//                            JSONObject dataObject = new JSONObject();

                            // dataObject.put("totalrecords", rowCount);
                            NodeList childNodeList = node.getChildNodes();
                            Object[] dataObject = new Object[childNodeList.getLength()];
                            for (int j = 0; j < childNodeList.getLength(); j++) {
                                try {
                                    Node childNode = childNodeList.item(j);
//                                    int childNodeIndex = j;
//                                    int nodeListLength = childNodeList.getLength();
                                    if (childNode != null) {
                                        if (childNode != null
                                                && childNode.getNodeType() == Node.ELEMENT_NODE) {
                                            try {
                                                if (childNode.getTextContent() != null
                                                        && !"".equalsIgnoreCase(childNode.getTextContent())
                                                        && !"null".equalsIgnoreCase(childNode.getTextContent())) {
//                                                    dataObject.put(fileName + ":" + childNode.getNodeName(), childNode.getTextContent());
                                                    dataObject[j] = childNode.getTextContent();

                                                } else {
//                                                    dataObject.put(fileName + ":" + childNode.getNodeName(), "");
                                                    dataObject[j] = "";
                                                }

                                            } catch (Exception e) {
                                                dataObject[j] = "";
                                                continue;
                                            }
                                            // Need to set the Data

                                        }
                                    }
                                } catch (Exception e) {

                                    continue;
                                }

                            }// column list loop
                            dataList.add(dataObject);
                        }

                    }// end of rows loop

                }
            } else {
                System.err.println("*** Root Element Not Found ****");
            }

            if (fis != null) {
                fis.close();
            }
        } catch (Exception e) {
            e.printStackTrace();

        }
        return dataList;
    }

    public List readPDF(HttpServletRequest request, String filePath, String fileName) {

        FileInputStream fis = null;

        System.out.println("Start Date And Time :::" + new Date());
        List dataList = new ArrayList();
        int rowVal = 1;
        try {
            // List resultArrayList = componentUtilities.readPDFRestApi(request, filePath);
            String result = componentUtilities.readPDFRestApi(request, filePath);
            JSONObject apiPdfJsonData = (JSONObject) JSONValue.parse(result);
            List<String> headerArray = (List<String>) apiPdfJsonData.get("columns");
            List<List<String>> resultArrayList = (List<List<String>>) apiPdfJsonData.get("data");
            for (int i = 0; i < resultArrayList.size(); i++) {
                LinkedHashMap resultObj = (LinkedHashMap) resultArrayList.get(i);
                Object[] rowData = resultObj.values().toArray();
                dataList.add(rowData);
            }
        } catch (Exception e) {
            e.printStackTrace();

        }

        return dataList;
    }

    public List getFileDataWithOperator(HttpServletRequest request, List columnsList, JSONObject operator, String jobId) {
        List totalData = new ArrayList();
        try {
            JSONObject fileConnObj = (JSONObject) operator.get("connObj");
            String fileName = (String) fileConnObj.get("fileName");
            String filePath = (String) fileConnObj.get("filePath");
            String fileType = (String) fileConnObj.get("fileType");

            List fromColumnsList = componentUtilities.getHeadersOfImportedFile(request, filePath);
            fromColumnsList = componentUtilities.fileHeaderValidations(fromColumnsList);
            String fromColumnsListStr = (String) fromColumnsList.stream().map(column -> column).collect(Collectors.joining(","));
            List<String> fromDataList = new ArrayList();
            if (fileType != null) {
                fileType = fileType.replaceAll("\\.", "");
            }
            if (fileType != null && !"null".equalsIgnoreCase(fileType) && ("XLSX".equalsIgnoreCase(fileType) || "XLS".equalsIgnoreCase(fileType))) {
                fromDataList = readExcelFile(request, filePath, fileName);
            } else if (fileType != null && !"null".equalsIgnoreCase(fileType) && ("CSV".equalsIgnoreCase(fileType) || "TXT".equalsIgnoreCase(fileType) || "JSON".equalsIgnoreCase(fileType))) {
                fromDataList = readCSVFile(request, filePath, fileName, fileType, fromColumnsList);
            } else if (fileType != null && !"null".equalsIgnoreCase(fileType) && "XML".equalsIgnoreCase(fileType)) {
                fromDataList = readXMLFile(request, filePath, fileName);
            } else if (fileType != null && !"null".equalsIgnoreCase(fileType) && "PDF".equalsIgnoreCase(fileType)) {
                fromDataList = readPDF(request, filePath, fileName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
        return totalData;
    }


    public int excelFileRowCount(
            HttpServletRequest request,
            String filepath,
            String fileName
    ) {

        FileInputStream fis = null;
        List dataList = new ArrayList();
        int rowVal = 1;
        int rowCount = 0;
        try {
            Workbook workBook = WorkbookFactory.create(new File(filepath));
            Sheet sheet = null;
            int noOfSheets = workBook.getNumberOfSheets();

            String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());

            if (workBook.getSheetAt(0) instanceof XSSFSheet) {
                sheet = (XSSFSheet) workBook.getSheetAt(0);
            } else if (workBook.getSheetAt(0) instanceof HSSFSheet) {
                sheet = (HSSFSheet) workBook.getSheetAt(0);
            }
            int lastRowNo = sheet.getLastRowNum();
            int firstRowNo = sheet.getFirstRowNum();
//                System.out.println("firstRowNo::::" + firstRowNo);
            rowCount = lastRowNo - firstRowNo;


            // return result1;
            if (fis != null) {
                fis.close();
            }

        } catch (Exception e) {
            e.printStackTrace();

        }

        return rowCount;
    }

    public int csvJsonTxtFileRowCount(
            HttpServletRequest request,
            String filepath,
            String fileName,
            String fileType) {
//        List columnList = getHeadersOfImportedFile(filepath, fileType);
        FileInputStream fis = null;
//        System.out.println("Start Date And Time :::" + new Date());
        List dataList = new ArrayList();
        int rowCount =0;
        int rowVal = 1;
        try {

            //  fis = new FileInputStream(new File(filepath));
//            char columnSeparator = '\t';
//            char columnSeparator = ',';
            CsvParserSettings settings = new CsvParserSettings();
            settings.detectFormatAutomatically();

            CsvParser parser = new CsvParser(settings);
            List<String[]> rows = parser.parseAll(new File(filepath));

            // if you want to see what it detected
            CsvFormat format = parser.getDetectedFormat();
            char columnSeparator = format.getDelimiter();

            if (".json".equalsIgnoreCase(fileType)) {
                columnSeparator = ',';
            }
            CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filepath), "UTF8"), columnSeparator);
            LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(filepath));
            String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
//            System.out.println("fileExtension:::" + fileExtension);

            int stmt = 1;
            String strToDateCol = "";
            // need to write logic for extraction from File
//            CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(filepath), "UTF8"), columnSeparator);
//            LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(filepath));
            lineNumberReader.skip(Long.MAX_VALUE);
            long totalRecords = lineNumberReader.getLineNumber();
            if (totalRecords != 0) {
                totalRecords = totalRecords - 1;
            }
            rowCount =  (int)totalRecords;


            if (fis != null) {
                fis.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return rowCount;
    }

    public int xmlFileRowCount(
            HttpServletRequest request,
            String filepath,
            String fileName) {

//        columnList = getHeadersOfImportedFile(filePath, fileType);
        FileInputStream fis = null;
        List dataList = new ArrayList();
        int rowCount = 0;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(new FileInputStream(filepath), "UTF-8");

            String fileExtension = filepath.substring(filepath.lastIndexOf(".") + 1, filepath.length());
//            System.out.println("fileExtension:::" + fileExtension);

            document.getDocumentElement().normalize();
            Element root = document.getDocumentElement();

            if (root.hasChildNodes() && root.getChildNodes().getLength() > 1) {
                // nested childs
                String evaluateTagName = "/" + root.getTagName();
                NodeList rootList = root.getChildNodes();
                if (!"#Text".equalsIgnoreCase(rootList.item(0).getNodeName())) {
                    evaluateTagName += "/" + rootList.item(0).getNodeName();
                } else {
                    evaluateTagName += "/" + rootList.item(1).getNodeName();
                }

                System.out.println("evaluateTagName:::" + evaluateTagName);
                XPath xpath = XPathFactory.newInstance().newXPath();
                NodeList dataNodeList = (NodeList) xpath.evaluate(evaluateTagName,
                        //            NodeList nList = (NodeList) xpath.evaluate("/PiLog_Data_Export/Item",
                        document,
                        XPathConstants.NODESET);

                if (dataNodeList != null && dataNodeList.getLength() != 0) {
                    rowCount = dataNodeList.getLength();
                }
            } else {
                System.err.println("*** Root Element Not Found ****");
            }

            if (fis != null) {
                fis.close();
            }
        } catch (Exception e) {
            e.printStackTrace();

        }
        return rowCount;
    }

    public String folderFileDMImport(HttpServletRequest request, MultipartFile file1, String fileName) {

        String result = "";
        String filename = "";
        try {

            String excelFilePath = etlFilePath + "FolderFiles/BulkDMImport/" + request.getSession(false).getAttribute("ssUsername");
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

                // need to save
                if ("json".equalsIgnoreCase(fileType1)) {
                    // need to convert from json to CSV
                    try {
                        mainFileName = dataPipingService.convertJSONtoCSV(file, excelFilePath);
                    } catch (Exception e) {
                    }

                }
                result = excelFilePath + "\\" + mainFileName;
            }


        } catch (Exception e) {
            result = "Unexpected error: " + e.getMessage();
            e.printStackTrace();
        }

        return result;
    }


}
