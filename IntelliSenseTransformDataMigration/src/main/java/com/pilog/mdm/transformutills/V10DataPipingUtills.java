/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pilog.mdm.transformutills;


import com.pilog.mdm.utilities.AuditIdGenerator;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.math.NumberUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 * @author sanya
 */
public class V10DataPipingUtills {

    public String getNumGeneration(JSONObject functionObj,
            Connection connection, String dataBaseDriver,
            String dbURL, String dbUserName,
            String dbPassword,
            String loginUserName,
            String loginOrgnId, String lastSeqObj, String toColumnName) {
        String numberStr = "";
        try {
            if (connection != null) {
                numberStr = get((String) functionObj.get("DOMAIN"),
                        (String) functionObj.get("ORGN_ID"),
                        (String) functionObj.get("COLUMN_NAME"), connection);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return numberStr;
    }

    public String getNumSequence(JSONObject functionObj,
            Connection connection, String dataBaseDriver,
            String dbURL, String dbUserName,
            String dbPassword,
            String loginUserName,
            String loginOrgnId, String lastSeqObj, String toColumnName) {
        String numberStr = "";
        try {
            if (connection != null) {
                String lastSeq = "";
                if (lastSeq != null
                        && !"".equalsIgnoreCase(lastSeqObj)
                        && !"null".equalsIgnoreCase(lastSeqObj)) {
                    lastSeq = lastSeqObj;
                } else {
                    lastSeq = getLastNumSeq(functionObj,
                            dataBaseDriver, dbURL, dbUserName, dbPassword, loginUserName, loginOrgnId, toColumnName);
                }
                if (lastSeq != null
                        && !"".equalsIgnoreCase(lastSeq)
                        && !"null".equalsIgnoreCase(lastSeq)
                        && NumberUtils.isNumber(lastSeq)) {
                    int lastSeqNum = Integer.parseInt(lastSeq);
                    lastSeqNum++;
                    numberStr = "" + lastSeqNum;
                    String appendVal = (String) functionObj.get("APPEND_TEXT");
                    String prependVal = (String) functionObj.get("PREPEND_TEXT");
                    if (prependVal != null
                            && !"".equalsIgnoreCase(prependVal)
                            && !"null".equalsIgnoreCase(prependVal)) {
                        numberStr = prependVal + "" + lastSeqNum;
                    }
                    if (appendVal != null
                            && !"".equalsIgnoreCase(appendVal)
                            && !"null".equalsIgnoreCase(appendVal)) {
                        numberStr += appendVal;
                    }

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return numberStr;
    }

    public String getOracleNumSequence(JSONObject functionObj,
            Connection connection, String dataBaseDriver,
            String dbURL, String dbUserName,
            String dbPassword,
            String loginUserName,
            String loginOrgnId, String lastSeqObj, String toColumnName) {
        String numberStr = "";
        PreparedStatement seqStmt = null;
        ResultSet seqSet = null;
        try {
            if (connection != null) {
//                System.out.println("functionObj::"+functionObj);
                String seqQuery = "SELECT " + functionObj.get("SEQUENCE_NAME") + ".NEXTVAL AS SEQUENCE_NO FROM DUAL";
                seqStmt = connection.prepareStatement(seqQuery);
                seqSet = seqStmt.executeQuery();
                if (seqSet.next()) {
                    numberStr = seqSet.getString("SEQUENCE_NO");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            numberStr = e.getMessage();
        } finally {
            try {
                if (seqSet != null) {
                    seqSet.close();
                }
                if (seqStmt != null) {
                    seqStmt.close();
                }
            } catch (Exception e) {
            }
        }
        return numberStr;
    }

    public String getHex32(JSONObject functionObj,
            Connection connection, String dataBaseDriver,
            String dbURL, String dbUserName,
            String dbPassword,
            String loginUserName,
            String loginOrgnId, String lastSeqObj, String toColumnName) {
        String numberStr = "";
        try {
            numberStr = AuditIdGenerator.genRandom32Hex();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return numberStr;
    }

    public String getHex64(JSONObject functionObj,
            Connection connection, String dataBaseDriver,
            String dbURL, String dbUserName,
            String dbPassword,
            String loginUserName,
            String loginOrgnId, String lastSeqObj, String toColumnName) {
        String numberStr = "";
        try {
            numberStr = AuditIdGenerator.genRandom32Hex() + AuditIdGenerator.genRandom32Hex();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return numberStr;
    }

    public synchronized String get(String domainType, String orgnId, String colName, Connection connection)
            throws Exception {

        String unique = null;
        try {

            unique = getRugged(domainType, orgnId, colName, connection);
        } catch (Exception e) {
        }

        return unique;
    }

    // public synchronized String getRugged(String domainType, String orgnId, String colName)
    public synchronized String getRugged(String domainType, String orgnId, String colName, Connection connection)
            throws SQLException {
        String unique = null;

        unique = getUniqueString(domainType, orgnId, colName, connection);
        if (unique == null) {
            //createUNACTEntry(domainType, orgnId, colName, connection);
            unique = getUniqueString(domainType, orgnId, colName, connection);
        }

        incrementUniqueid(domainType, orgnId, colName, connection);
        return unique;
    }

    private synchronized String getUniqueString(String domainType, String orgnId, String colName, Connection connection)
            throws SQLException {

        String rtn = null;
        String prefix = "";
        String padSize = "";
        String number = "";
        PreparedStatement stmt = null;
        ResultSet resultSet = null;
        try {

            String query = "SELECT SEQ_TXT,SEQ_SIZE,STRT_UNIQUE_SEQ FROM DAL_NUMBER_GENERATION"
                    + " WHERE ORGN_ID =? AND TYPE =? AND COLUMN_NAME =?";
//            String query = "FROM DalNumberGeneration WHERE  id.orgnId=:orgnId AND id.type like '%" + domainType + "%' AND id.columnName=:colName";
            stmt = connection.prepareStatement(query);
            stmt.setObject(1, orgnId);
            stmt.setObject(2, domainType);
            stmt.setObject(3, colName);
            resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                prefix = resultSet.getString("SEQ_TXT");
                padSize = resultSet.getString("SEQ_SIZE");
                number = resultSet.getString("STRT_UNIQUE_SEQ");
            } else {
                return new Exception("Unique No Could not be generated").getMessage();
            }

            ////logger.info("prefix:::::"+prefix);
            ////logger.info("padSize:::::"+padSize);
            ////logger.info("number:::::"+number);
            prefix = (prefix == null) || (prefix.equals("null")) ? "" : prefix;
            if ((prefix.contains("<<-")) && (prefix.contains("->>"))) {
                prefix = prefix.replace("<<-", "");
                prefix = prefix.replace("->>", "");
            }

            int currentLength = prefix.length() + number.length();
            int totalLength = 0;
            if ((padSize != null) && (!padSize.equals("null")) && (!padSize.equals("0")) && (!padSize.isEmpty())) {
                totalLength = Integer.parseInt(padSize);
                if ((currentLength == totalLength) && (endOfSequence(number, connection))) {
                    updateEndOfSequence(domainType, connection);
                    rtn = padString(prefix, number, padSize, "0", connection);
                } else {
                    rtn = padString(prefix, number, padSize, "0", connection);
                }
            } else {
                rtn = prefix + number;
            }

            // //logger.info("rtn:::::"+rtn);
        } catch (Exception e) {
            e.printStackTrace();
            // logger.error("Exception:" + e.getLocalizedMessage());
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            } catch (Exception e) {
            }
        }

        return rtn;
    }

    public synchronized String padString(String pre, String suf, String padSize, String padString, Connection connection) {
        try {
            int finalTotal = Integer.parseInt(padSize);
            int beforePad = pre.length() + suf.length();
            for (int x = 0; x < finalTotal - beforePad; x++) {
                pre = pre.concat(padString);
            }
            return pre + suf;
        } catch (Exception ex) {
            ex.printStackTrace();
            // logError("Error occurred during number padding of unique key", ex);
        }
        return pre + suf;
    }

    private void incrementUniqueid(String domainType, String orgnId, String colName, Connection connection)
            throws SQLException {
        PreparedStatement stmt = null;
        try {

            String query = "UPDATE DAL_NUMBER_GENERATION SET STRT_UNIQUE_SEQ =STRT_UNIQUE_SEQ+1"
                    + " WHERE ORGN_ID =? AND TYPE =? AND COLUMN_NAME =?  ";
//            String query = "UPDATE DalNumberGeneration SET id.strtUniqueSeq =id.strtUniqueSeq+1 WHERE id.orgnId =:orgnId "
//                    + "AND id.columnName =:colName AND id.type like '%" + domainType + "%' ";
            stmt = connection.prepareStatement(query);
            stmt = connection.prepareStatement(query);
            stmt.setObject(1, orgnId);
            stmt.setObject(2, domainType);
            stmt.setObject(3, colName);
            stmt.execute();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {

                if (stmt != null) {
                    stmt.close();
                }
            } catch (Exception e) {
            }
        }

    }

    private synchronized void updateEndOfSequence(String code, Connection connection)
            throws SQLException {

        String uniqueText = "";
        String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        try {

//            pstmtUpdateUNQ_UNACT = conn.prepareStatement("UPDATE UNQ_UNACT SET UNACT_LOC = 0 WHERE UNACT_CDE = ? ");
//            pstmtUpdateUNQ_UNACT.setString(1, code);
//            pstmtUpdateUNQ_UNACT.executeUpdate();
//            closeStatement(pstmtUpdateUNQ_UNACT);
//
//            pstmtSelectUNQ_UNACT = conn.prepareStatement("SELECT UNACT_TXT FROM UNQ_UNACT WHERE UNACT_CDE = ? ");
//            pstmtSelectUNQ_UNACT.setString(1, code);
//            rsSelectUNQ_UNACT = pstmtSelectUNQ_UNACT.executeQuery();
//            if (rsSelectUNQ_UNACT.next()) {
//                uniqueText = rsSelectUNQ_UNACT.getString("UNACT_TXT");
//                if (uniqueText.contains("<<-")) {
//                    String letter = "";
//                    letter = uniqueText.substring(uniqueText.indexOf("<<-") + 3, uniqueText.indexOf("->>"));
//                    if (!letter.equals("Z")) {
//                        int indx = alphabet.indexOf(letter);
//                        uniqueText = uniqueText.replaceFirst("<<-" + letter + "->>", "<<-" + alphabet.charAt(indx + 1) + "->>");
//                    } else {
//                        ////logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//                        ////logger.info("END OF NUMBER RANGE!!! PLEASE CHECK UNQ_UNACT!");
//                        ////logger.info("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//                    }
//                } else {
//                    uniqueText = uniqueText + "<<-A->>";
//                }
//            }
//            pstmtUpdateUNQ_UNACT = conn.prepareStatement("UPDATE UNQ_UNACT SET UNACT_TXT = ? WHERE UNACT_CDE = ? ");
//            pstmtUpdateUNQ_UNACT.setString(1, uniqueText);
//            pstmtUpdateUNQ_UNACT.setString(2, code);
//            pstmtUpdateUNQ_UNACT.executeUpdate();
        } catch (Exception e) {

        }

    }

    private synchronized boolean endOfSequence(String suf, Connection connection) {
        while (suf.length() > 0) {
            if (!suf.endsWith("9")) {
                return false;
            }
            if (suf.length() == 1) {
                return true;
            }
            suf = suf.substring(0, suf.length() - 1);
        }
        return true;
    }

    public String getLastNumSeq(JSONObject functionObj, String dataBaseDriver,
            String dbURL, String dbUserName,
            String dbPassword,
            String loginUserName,
            String loginOrgnId, String toColumnName) {
        String lastSeq = "";
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Connection connection = null;
        try {
            String logQuery = "SELECT DM_LOG_CUST_COL1 FROM RECORD_DM_PROCESS_LOG"
                    + " WHERE USER_NAME =? AND ORGN_ID =? AND DM_LOG_CUST_COL1 IS NOT NULL AND DM_LOG_CUST_COL1 != '{}' ORDER BY SEQUENCE_NO DESC";
            Class.forName(dataBaseDriver);
            connection = DriverManager.getConnection(dbURL, dbUserName, dbPassword);
            preparedStatement = connection.prepareStatement(logQuery);
            preparedStatement.setObject(1, loginUserName);
            preparedStatement.setObject(2, loginOrgnId);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String lastSeqStr = resultSet.getString("DM_LOG_CUST_COL1");
                String appendVal = (String) functionObj.get("APPEND_TEXT");
                String prependVal = (String) functionObj.get("PREPEND_TEXT");
                if (lastSeqStr != null
                        && !"".equalsIgnoreCase(lastSeqStr)
                        && !"null".equalsIgnoreCase(lastSeqStr)
                        && !"{}".equalsIgnoreCase(lastSeqStr)) {
                    JSONObject lastSeqObj = new JSONObject();
                    try {
                        lastSeqObj = (JSONObject) JSONValue.parse(lastSeqStr);
                        if (lastSeqObj != null && !lastSeqObj.isEmpty() && lastSeqObj.containsKey(toColumnName)) {
                            lastSeqStr = (String) lastSeqObj.get(toColumnName);
                        }

                    } catch (Exception e) {
                    }
                    if (lastSeqStr != null
                            && !"".equalsIgnoreCase(lastSeqStr)
                            && !"null".equalsIgnoreCase(lastSeqStr)) {
                        if (prependVal != null
                                && !"".equalsIgnoreCase(prependVal)
                                && !"null".equalsIgnoreCase(prependVal)) {
                            lastSeqStr = lastSeqStr.substring(prependVal.length() - 1, lastSeqStr.length());
                        }
                        if (appendVal != null
                                && !"".equalsIgnoreCase(appendVal)
                                && !"null".equalsIgnoreCase(appendVal)) {
                            lastSeqStr = lastSeqStr.substring(0, lastSeqStr.indexOf(appendVal));
                        }
                        if (lastSeqStr != null
                                && !"".equalsIgnoreCase(lastSeqStr)
                                && !"null".equalsIgnoreCase(lastSeqStr)) {
                            lastSeq = lastSeqStr;
                        } else {
                            lastSeq = "1";
                        }
                    } else {
                        lastSeq = "1";
                    }

                } else {
                    lastSeq = "1";
                }
            } else {
                lastSeq = "1";
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            } catch (Exception e) {
            }
        }
        return lastSeq;
    }

    public Object convertIntoDBValue(String columnType,
            Object value,
            String columnName,
            String fromDateFormate
    ) {
        try {

            if (value != null && columnType != null
                    && !"".equalsIgnoreCase(columnType)
                    && !"null".equalsIgnoreCase(columnType)) {
                if ("DATE".equalsIgnoreCase(columnType)
                        || "TIMESTAMP".equalsIgnoreCase(columnType)
                        || "DATETIME".equalsIgnoreCase(columnType)) {
                    try {
                        JSONObject funObj = new JSONObject();
                        if (fromDateFormate != null
                                && !"".equalsIgnoreCase(fromDateFormate)
                                && !"null".equalsIgnoreCase(fromDateFormate)
                                && fromDateFormate.contains("{")) {
                            try {
                                JSONObject coluClauseObj = (JSONObject) JSONValue.parse(fromDateFormate);
                                String funobjstr = (String) coluClauseObj.get("funobjstr");
                                if (funobjstr != null
                                        && !"".equalsIgnoreCase(funobjstr)
                                        && !"null".equalsIgnoreCase(funobjstr)) {
                                    funObj = (JSONObject) JSONValue.parse(funobjstr);
                                    if (funObj != null && !funObj.isEmpty()) {
                                        if (funObj.get("funStr") != null
                                                && !"".equalsIgnoreCase(String.valueOf(funObj.get("funStr")))
                                                && !"null".equalsIgnoreCase(String.valueOf(funObj.get("funStr")))) {
                                            fromDateFormate = String.valueOf(funObj.get("funStr"));
                                        } else {
                                            fromDateFormate = (String) funObj.get("functionName");
                                        }

                                    }
                                }

                            } catch (Exception e) {
                            }
                        }
                        if (!(fromDateFormate != null
                                && !"".equalsIgnoreCase(fromDateFormate)
                                && !"null".equalsIgnoreCase(fromDateFormate))) {
                            fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                        }

                        //DATE_FORMAT('dd-MM-yyyy HH:mm:ss.sss','COLUMNS','undefined','undefined')
                        if (fromDateFormate.contains("DATE_FORMAT(")) {
                            if (funObj != null
                                    && !funObj.isEmpty()) {//FROM_DATE_FORMATE
                                if (funObj.get("FROM_DATE_FORMATE") != null
                                        && !"".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))
                                        && !"null".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))) {
                                    fromDateFormate = String.valueOf(funObj.get("FROM_DATE_FORMATE"));
                                } else {
                                    fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                                }

                            } else {
                                fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                            }
//                            fromDateFormate = fromDateFormate.substring(fromDateFormate.indexOf("'") + 1,
//                                    fromDateFormate.indexOf("'", fromDateFormate.indexOf("'") + 1));
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(fromDateFormate);
                            Date localDate = simpleDateFormat.parse(String.valueOf(value));
                            if (localDate != null) {
                                java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
                                value = sqlDate;
                            }
                        } else if (fromDateFormate.contains("SYSDATE")
                                || fromDateFormate.contains("CURDATE")) {//
                            if (funObj != null
                                    && !funObj.isEmpty()) {//FROM_DATE_FORMATE
                                if (funObj.get("FROM_DATE_FORMATE") != null
                                        && !"".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))
                                        && !"null".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))) {
                                    fromDateFormate = String.valueOf(funObj.get("FROM_DATE_FORMATE"));
                                } else {
                                    fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                                }

                            } else {
                                fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                            }
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(fromDateFormate);
                            Date localDate = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
                            if (localDate != null) {
                                java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
                                value = sqlDate;
                            }
                        } else {
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
                            Date localDate = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
                            if (localDate != null) {
                                java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
                                value = sqlDate;
                            }
                        }

                    } catch (Exception e) {
                    }

                } else if ("NUMBER".equalsIgnoreCase(columnType)
                        || "NUMERIC".equalsIgnoreCase(columnType)
                        || "INTEGER".equalsIgnoreCase(columnType)
                        || "INT".equalsIgnoreCase(columnType)
                        || "BIGINT".equalsIgnoreCase(columnType)
                        || "TINYINT".equalsIgnoreCase(columnType)
                        || "SMALLINT".equalsIgnoreCase(columnType)
                        || "MEDIUMINT".equalsIgnoreCase(columnType)) {
                    
                     BigInteger integerObj = null; //  // ravi etl integration
                    try{
                     integerObj = new BigInteger(String.valueOf(value));
                    
                    } catch(Exception e){
                    value = 0;
                    }
                    if (integerObj != null) {
                        value = integerObj.intValue();
                    }	
                    
//                    BigInteger integerObj = new BigInteger(String.valueOf(value));
//                    if (integerObj != null) {
//                        value = integerObj.intValue();
//                    }
                } else if ("FLOAT".equalsIgnoreCase(columnType)
                        || "DECIMAL".equalsIgnoreCase(columnType)
                        || "DOUBLE".equalsIgnoreCase(columnType)) {
                    
                     BigInteger integerObj = null; // ravi etl integration
                    try{
                     integerObj = new BigInteger(String.valueOf(value));
                    
                    } catch(Exception e){
                    value = 0;
                    }
                    if (integerObj != null) {
                        value = integerObj.intValue();
                    }
                    
//                    BigDecimal integerObj = new BigDecimal(String.valueOf(value));
//                    if (integerObj != null) {
//                        value = integerObj.doubleValue();
//                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

    public Object convertIntoDBValue(
            Object value,
            String columnName,
            String fromDateFormate
    ) {
        try {
            if (value != null) {
                if (fromDateFormate != null
                        && !"".equalsIgnoreCase(fromDateFormate)
                        && !"null".equalsIgnoreCase(fromDateFormate)) {
                    JSONObject funObj = new JSONObject();
                    if (fromDateFormate.contains("{")) {
                        try {
                            JSONObject coluClauseObj = (JSONObject) JSONValue.parse(fromDateFormate);
                            String funobjstr = (String) coluClauseObj.get("funobjstr");
                            if (funobjstr != null
                                    && !"".equalsIgnoreCase(funobjstr)
                                    && !"null".equalsIgnoreCase(funobjstr)) {
                                funObj = (JSONObject) JSONValue.parse(funobjstr);
                                if (funObj != null && !funObj.isEmpty()) {
                                    if (funObj.get("funStr") != null
                                            && !"".equalsIgnoreCase(String.valueOf(funObj.get("funStr")))
                                            && !"null".equalsIgnoreCase(String.valueOf(funObj.get("funStr")))) {
                                        fromDateFormate = String.valueOf(funObj.get("funStr"));
                                    } else {
                                        fromDateFormate = (String) funObj.get("functionName");
                                    }

                                }
                            }

                        } catch (Exception e) {
                        }
                    }
                    //DATE_FORMAT('dd-MM-yyyy HH:mm:ss.sss','COLUMNS','undefined','undefined')
                    if (fromDateFormate.contains("DATE_FORMAT(")) {
//                        fromDateFormate = fromDateFormate.substring(fromDateFormate.indexOf("'") + 1,
//                                fromDateFormate.indexOf("'", fromDateFormate.indexOf("'") + 1));
                        if (funObj != null
                                && !funObj.isEmpty()) {//FROM_DATE_FORMATE
                            if (funObj.get("FROM_DATE_FORMATE") != null
                                    && !"".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))
                                    && !"null".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))) {
                                fromDateFormate = String.valueOf(funObj.get("FROM_DATE_FORMATE"));
                            } else {
                                fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                            }

                        } else {
                            fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                        }
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(fromDateFormate);
                        Date localDate = simpleDateFormat.parse(String.valueOf(value));
                        if (localDate != null) {
                            java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
                            value = sqlDate;
                        }
                    } else if (fromDateFormate.contains("SYSDATE")
                            || fromDateFormate.contains("CURDATE")
                            || fromDateFormate.contains("CURRENT_DATE")) {//
                        if (funObj != null
                                && !funObj.isEmpty()) {//FROM_DATE_FORMATE
                            if (funObj.get("FROM_DATE_FORMATE") != null
                                    && !"".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))
                                    && !"null".equalsIgnoreCase(String.valueOf(funObj.get("FROM_DATE_FORMATE")))) {
                                fromDateFormate = String.valueOf(funObj.get("FROM_DATE_FORMATE"));
                            } else {
                                fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                            }

                        } else {
                            fromDateFormate = "yyyy-MM-dd HH:mm:ss.sss";
                        }
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(fromDateFormate);
                        Date localDate = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
                        if (localDate != null) {
                            java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
                            value = sqlDate;
                        }
                    } else {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
                        Date localDate = simpleDateFormat.parse(simpleDateFormat.format(new Date()));
                        if (localDate != null) {
                            java.sql.Timestamp sqlDate = new java.sql.Timestamp(localDate.getTime());
                            value = sqlDate;
                        }
                    }
                    value = value.toString();
                } else {
                    value = String.valueOf(value);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

    public List<Map> getFromOperators(Map mappedData, Object mappedKey) {
        List<Map> fromOperatorList = new ArrayList<>();
        try {
            Map operatorsMap = (Map) mappedData.get("operators");
            Map linksMap = (Map) mappedData.get("links");
            for (Object linkKey : linksMap.keySet()) {
                Map linkMap = (Map) linksMap.get(linkKey);
                if (linkMap != null
                        && !linkMap.isEmpty()
                        && String.valueOf(mappedKey)
                                .equalsIgnoreCase(String.valueOf(linkMap.get("toOperator")))) {
//                                System.out.println("linkMap.get(\"fromOperator\")::" + linkMap.get("fromOperator"));
                    String fromOperatorId = String.valueOf(linkMap.get("fromOperator"));
                    Map fromOperator = (Map) operatorsMap.get(String.valueOf(linkMap.get("fromOperator")));
//                                fromOperator.put("operatorId", fromOperatorId);
                    fromOperatorList.add(fromOperator);

                }

            }// end loop linksMap
        } catch (Exception e) {
        }
        return fromOperatorList;
    }

    public String generateMultiColumnsObj(JSONObject multiColumnsObj, String functionName) {
        String multiColumnsStr = "";
        try {
            if (multiColumnsObj != null && !multiColumnsObj.isEmpty()) {
                LinkedHashMap<Integer, Object> multiColumnsMap = new LinkedHashMap();
                LinkedHashMap multiColumnsMapUpdated = new LinkedHashMap();
                multiColumnsMap.putAll(multiColumnsObj);
                multiColumnsMapUpdated = multiColumnsMap.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (oldValue, newValue) -> oldValue, LinkedHashMap::new));
                if ("CASE".equalsIgnoreCase(functionName)) {
                    multiColumnsStr += " CASE ";
                    for (Object orderKey : multiColumnsMap.keySet()) {
                        Map columnsObj = (Map) multiColumnsMap.get(orderKey);
                        if (columnsObj != null && !columnsObj.isEmpty()) {
                            multiColumnsStr += " " + String.valueOf(columnsObj.get("CASE_COND")).toUpperCase();
                            if (columnsObj.get("COLUMN_NAME") != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnsObj.get("COLUMN_NAME")))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get("COLUMN_NAME")))) {//COLUMN_NAME
                                multiColumnsStr += " " + String.valueOf(columnsObj.get("COLUMN_NAME")).replace(":", ".");
                            }
                            if (columnsObj.get("OPERATOR") != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnsObj.get("OPERATOR")))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get("OPERATOR")))) {//OPERATOR
                                multiColumnsStr += " " + String.valueOf(columnsObj.get("OPERATOR")).toUpperCase();
                            }

                            if (columnsObj.get("COLUMN_VALUE") != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnsObj.get("COLUMN_VALUE")))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get("COLUMN_VALUE")))) {//COLUMN_VALUE
                                if (String.valueOf(columnsObj.get("COLUMN_VALUE")).contains(".")) {
                                    multiColumnsStr += " " + String.valueOf(columnsObj.get("COLUMN_VALUE")).toUpperCase().replace(":", ".");
                                } else {

                                    multiColumnsStr += " '" + String.valueOf(columnsObj.get("COLUMN_VALUE")) + "'";
                                }

                            }
                            if ("WHEN".equalsIgnoreCase(String.valueOf(columnsObj.get("CASE_COND")))) {
                                multiColumnsStr += " THEN ";
                            }

                            if (columnsObj.get("RESULT_COLUMN") != null
                                    && !"".equalsIgnoreCase(String.valueOf(columnsObj.get("RESULT_COLUMN")))
                                    && !"null".equalsIgnoreCase(String.valueOf(columnsObj.get("RESULT_COLUMN")))) {//RESULT_COLUMN
                                if (String.valueOf(columnsObj.get("RESULT_COLUMN")).contains(".")) {
                                    multiColumnsStr += " " + String.valueOf(columnsObj.get("RESULT_COLUMN")).toUpperCase().replace(":", ".");
                                } else {

                                    multiColumnsStr += " '" + String.valueOf(columnsObj.get("RESULT_COLUMN")) + "'";
                                }

                            }

                        }
                    }
                    multiColumnsStr += " END ";
                }

            }
//            System.out.println("multiColumnsStr:::"+multiColumnsStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return multiColumnsStr;
    }
}
