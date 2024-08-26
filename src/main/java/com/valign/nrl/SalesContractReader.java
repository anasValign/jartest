package com.valign.nrl;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class SalesContractReader {

    public static void main(String[] args) throws IOException, CsvException {

        String access_token = null;
        String versionString = null;
        String pathToCsv = null;

        //to load application's properties, we use this class
        Properties mainProperties = new Properties();

        FileInputStream file;

        //String path =  "./main.properties";
       String path =  "./src/main/resources/main.properties";
        //load the file handle for main.properties
          file = new FileInputStream(path);

        //load all the properties from this file
         mainProperties.load(file);

        //we have loaded the properties, so close the file handle
            file.close();


            versionString = mainProperties.getProperty("app.version");
        String path1 = mainProperties.getProperty("app.pathToCsv");
        java.util.List<String> path1List = new java.util.ArrayList<String>();

        File folder = new File(path1);
        for ( File fileEntry : folder.listFiles()) {
            path1List.add(path1 + fileEntry.getName());
        }



        access_token = getAccessTokenForZohoCRM();

      //  String fileName = pathToCsv;

        for (String pathFile : path1List) {
            if (pathFile.contains("contract")) {

                String logFileName = pathFile;
                String logFileName1 = logFileName.replace("csv", "log");
                String logFileName2 = null;
                System.out.println("logFileName " + logFileName);
                System.out.println("logFileName1 " + logFileName1);
                if (logFileName1.contains("JobQueuefiles")) {
                    logFileName2 = logFileName1.replaceAll("JobQueuefiles", "Logs");
                    System.out.println("log filename is " + logFileName2);
                }
                System.out.println("logFileName2 " + logFileName2);
                File fileLog = new File(logFileName2);

                //Create the file
                if (fileLog.createNewFile())
                {
                    System.out.println("File is created!");
                } else {
                    System.out.println("File already exists.");
                }

                //Write Content
                FileWriter writer = new FileWriter(fileLog);
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = new Date();
                System.out.println("line 84: " + dateFormat.format(date)); //2016/11/16 12:08:43
                writer.write("File created on " + dateFormat.format(date) + "\n");

                try (CSVReader reader = new CSVReader(new FileReader(pathFile))) {

                    List<String[]> r = reader.readAll();

                    int rowIndex = 0;
                    Map<Integer, String> headerRowMap = new LinkedHashMap<>();
                    int headerMapIndex = 0;

                    List<Map<String, String>> listOfRowValueMap = new ArrayList<>();
                    List<Map<String, String>> finalListOfRowValueMap1 = null;
                    Map<String, List<Map<String, String>>> saleOrderToOrderItems = new LinkedHashMap();
                    for (String[] s1 : r) {
                        Map<String, String> rowValueMap = new LinkedHashMap<>();
                        int valueMapIndex = 0;
                        for (String s2 : s1) {

                            if (rowIndex == 0) {
                                headerRowMap.put(headerMapIndex, s2);
                                headerMapIndex++;
                            } else {
                                rowValueMap.put(headerRowMap.get(valueMapIndex), s2);
                                valueMapIndex++;
                            }
                        }
                        listOfRowValueMap.add(rowValueMap);
                        rowIndex++;
                    }

                    headerRowMap.forEach((key, value) -> System.out.println(key + " " + value));
//                    int saleOrderCount = 0;
//                    String SaleOrderValue = "";
//                    for (Map<String, String> rowValueMap : listOfRowValueMap) {
//                        Set<String> keySet = rowValueMap.keySet();
//                        for (String key : keySet){
//                            String value =   rowValueMap.get(key);
//                            if (key.equals("SO_Number")) {
//                                if (!saleOrderToOrderItems.keySet().contains(value)) {
//                                    saleOrderCount++;
//                                    finalListOfRowValueMap1 = new ArrayList<>();
//                                    finalListOfRowValueMap1.add(rowValueMap);
//                                    saleOrderToOrderItems.put(value,finalListOfRowValueMap1);
//                                }else {
//                                    finalListOfRowValueMap1 = saleOrderToOrderItems.get(value);
//                                    finalListOfRowValueMap1.add(rowValueMap);
//                                }
//                            }
//                        }
//                    }
//
//                    Set<String> saleOrderSet = saleOrderToOrderItems.keySet();
//                    System.out.println("Count of Distinct Sale orders " + saleOrderSet.size());
//                    for (String saleOrderNumber : saleOrderSet) {
//                        System.out.println("Sale Order Number to Insert in Zoho "+ saleOrderNumber);
//                        List<Map<String, String>> finalListOfRowValueMap2 = saleOrderToOrderItems.get(saleOrderNumber);
//                        Map<String, String> eachRow = finalListOfRowValueMap2.get(0);
//                        System.out.println (" Billing country "+ eachRow.get("Billing_Country")  );
//                        //Start to create Input Body for Insert Sale Order API



//                    String inputBody =
//                            "{\n" +
//                                    "    \"data\": [\n";
//
//                    boolean isFirstRecord = true;
//
//
//                    for (Map<String, String> rowValueMap : listOfRowValueMap) {
//                        if(!isFirstRecord) {
//                            inputBody += "        {\n";
//                        }
//                        inputBody += "{\n";
//
//                        for (Map.Entry<String, String> entry : rowValueMap.entrySet()) {
//                            String key = entry.getKey();
//                            String value = entry.getValue();
//
//                            inputBody += "            \"" + key + "\": \"" + value + "\",\n";
//                        }
//                        inputBody = inputBody.substring(0, inputBody.length() - 2) + "\n";  // Remove the trailing comma
//                        inputBody += "        }";
//                        isFirstRecord = false;
//
////                        inputBody += "        },\n";
//                    }
//     //               inputBody += "        },\n";
//
//// Remove the trailing comma from the last object if needed
////                    if (!listOfRowValueMap.isEmpty()) {
////                        inputBody = inputBody.substring(0, inputBody.length() - 2) + "\n";
////                    }
//
//                    inputBody +=
//                            "\n    ]\n" + "}";

                    String inputBody = "{\n" +
                            "    \"data\": [\n";

                    boolean isFirstRecord = true;
                    boolean hasRecords = false; // Flag to check if there are any non-empty records

                    for (Map<String, String> rowValueMap : listOfRowValueMap) {
                        boolean isEmptyRecord = true; // Flag to check if the current record is empty

                        for (Map.Entry<String, String> entry : rowValueMap.entrySet()) {
                            String value = entry.getValue();

                            if (value != null && !value.isEmpty()) {
                                isEmptyRecord = false;
                                hasRecords = true;

                                break; // No need to check the other fields for this record
                            }
                        }

                        if (!isEmptyRecord) {
                            if (!isFirstRecord) {
                                inputBody += ",\n";
                            }
                            inputBody += "        {\n";

                            boolean isFirstField = true;

                            for (Map.Entry<String, String> entry : rowValueMap.entrySet()) {
                                String key = entry.getKey();
                                String value = entry.getValue();

                                if (value != null && !value.isEmpty()) {
                                    if (!isFirstField) {
                                        inputBody += ",\n";
                                    }
                                    inputBody += "            \"" + key + "\": \"" + value + "\"";
                                    isFirstField = false;
                                }
                            }

                            inputBody += "\n        }";
                            isFirstRecord = false;
                        }
                    }

                    if (hasRecords) {
                        inputBody += "\n    ]\n" +
                                "}";
                    } else {
                        // Handle the case when there are no non-empty records
                        inputBody = "{}";
                    }



//                    String inputBody =
//                                "{\n" +
//                                        "    \"data\": [\n" +
//                                        "        {\n"  ;

//                                        "            \"Billing_Block\": \"bb\",\n" +
//                                        "            \"Billing_City\": \"b city\",\n" +
//                                        "            \"Billing_Code\": \"b code\",\n" +
//                                        "            \"Billing_Country\": \"b country\",\n" +
//                                        "            \"Billing_Street\": \"b street\",\n" +
//                                        "            \"Billing_State\": \"b state\",\n" +
//                                        "            \"Account_Name\": \"acc name\",\n" +


//                        if (eachRow.get("Customer_No") != null && !eachRow.get("Customer_No").equals("")) {
//
//                            inputBody = inputBody + "            \"Customer_No\": \"" +  eachRow.get("Customer_No") +"\",\n";
//                        }else {
//                            inputBody = inputBody + "            \"Customer_No\":  null ,\n";
//                        }

//                                        "            \"Customer_No\": \"200080\",\n" +
//                                        "            \"Customer_Reference\": \"4000003997/P351/Washoil\",\n" +
//                                        "            \"Distribution_Channel\": \"12\",\n" +
//                                        "            \"Division\": \"22\",\n" +
//                                        "            \"Master_Contract\": \"mas con\",\n" +
//                                        "            \"Mat_Freight_Group\": \"MHNS\",\n" +
//                                        "            \"MnsofTrans_Type\": \"null\",\n" +
//                                        "            \"Value\": \"775$$407$$375.00\",\n" +
//                                        "            \"Plant\": \"3100\",\n" +
//                                        "            \"Pricing_Date\": \"12.11.2021\",\n" +
//                                        "            \"Route\": \"DEL\",\n" +
//                                        "            \"Sales_Group\": \"S G\",\n" +
//                                        "            \"Sales_Organization\": \"SO\",\n" +
//                                        "            \"Sales_Office\": \"null\",\n" +
//                                        "            \"Ship_to_party_SAP\": \"null\",\n" +
//                                        "            \"Shipping_City\": \"sh city\",\n" +
//                                        "            \"Shipping_Country\": \"cnt\",\n" +
//                                        "            \"Shipping_Street\": \"101\",\n" +
//                                        "            \"Shipping_Plant\": \"shp pl\",\n" +
//                                        "            \"Shipping_State\": \"sh state\",\n" +
//                                        "            \"Shipping_Type\": \"shp type\",\n" +
//                                        "            \"Shp_Cond\": \"shp con\",\n" +
//                                        "            \"SO_Number_SAP\": \"40083915\",\n" +
//                                        "            \"Storage_Location\": \"str loca\",\n" +
//                                        "            \"Valid_From\": \"va from\",\n" +
//                                        "            \"Valid_Till\": \"va till\",\n" +
//                                        "            \"Valuation_Type\": \"val type\",\n" +
   //                                     "            \"Description\": \"des\",\n" +
//                                        "            \"Product_Name\": \"SKO02\",\n" +
//                                        "            \"Quantity\": \"0\",\n" +
//                                        "            \"UoM\": \"KL\"\n" +
//                    inputBody +=
//                                        "        }\n" +
//                                        "    ]\n" +
//                                        "}";

                        System.out.println(inputBody);




//
                        //Start Call the Zoho Insert API to insert Sales Order

//                        System.out.println(inputBody);

                        try {

                            URL url = new URL("https://www.zohoapis.in/crm/v4/NRL_Sales_Contracts");
                            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                            conn.setDoOutput(true);
                            conn.setRequestMethod("POST");
                            conn.setRequestProperty("Content-Type", "application/json");
                            conn.setRequestProperty("Authorization", "Zoho-oauthtoken " + access_token);

   //                         System.out.println("final JSON is for saleOrder Number  " + saleOrderNumber + " \n " + inputBody);

                            OutputStream os = conn.getOutputStream();
                            os.write(inputBody.getBytes());
                            os.flush();

                            if (conn.getResponseCode() != 201) {
                                dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                                date = new Date();
                                System.out.println("line 323: " + dateFormat.format(date)); //2016/11/16 12:08:43
                                writer.write("Insert API to Zoho failed on " + dateFormat.format(date) + "\n");
                                writer.write("Failed : HTTP error code : " + conn.getResponseCode());

                                String errorMessage = "Failed : HTTP error code : " + conn.getResponseCode();
                                InputStream errorStream = conn.getErrorStream();
                                String errorResponse = "";
                                if (errorStream != null) {
                                    errorResponse = new BufferedReader(new InputStreamReader(errorStream))
                                            .lines().collect(Collectors.joining("\n"));
                                }
                                System.out.println(errorMessage);
                                System.out.println("Error response from server: " + errorResponse);
                                throw new RuntimeException(errorMessage + "\n" + errorResponse);

//                                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode() + "\n");
                             }

                            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

                            String output;

                            while ((output = br.readLine()) != null) {

                                System.out.println("Inserted Record in Zoho CRM with values   "+ inputBody + " response from Zoho CRM " + output +" \n" );
                                writer.write("Inserted Record in Zoho CRM with values   "+ inputBody + " response from Zoho CRM " + output +" \n" );
                            }

                            conn.disconnect();

                        } catch (MalformedURLException e) {

                            e.printStackTrace();

                        } catch (IOException e) {

                            e.printStackTrace();

                        }
                        catch (RuntimeException ex) {
                            if (ex.getMessage().contains("HTTP error code : 400")) {
                                // Skip or ignore the exception and continue execution
                                System.out.println("HTTP error 400 occurred. Skipping..." + ex.getMessage());
                            } else {
                                // Handle other types of RuntimeExceptions
                                // or rethrow the exception if needed
                                throw ex;
                            }
                        }

                        //End Call the Zoho Insert API to insert Sales Order
                    }
                    writer.close();
                    String destFile = pathFile.replace("JobQueuefiles", "CompletedinZoho");
                    moveFile(pathFile,destFile);
                }
            }
     //   }




    }
    private static void moveFile(String sourceFile, String destFile) {

        InputStream inStream = null;
        OutputStream outStream = null;

        try{

            File afile =new File(sourceFile);
            File bfile =new File(destFile);

            inStream = new FileInputStream(afile);
            outStream = new FileOutputStream(bfile);

            byte[] buffer = new byte[1024];

            int length;
            //copy the file content in bytes
            while ((length = inStream.read(buffer)) > 0){

                outStream.write(buffer, 0, length);

            }

            inStream.close();
            outStream.close();

            //delete the original file
            afile.delete();

            System.out.println("File is copied successful!");

        }catch(IOException e){
            e.printStackTrace();
        }

    }
    private static String getAccessTokenForZohoCRM() {

        String access_token = null;

        try {

            URL url = new URL(
                    "https://accounts.zoho.in/oauth/v2/token?refresh_token=1000.af21a70c873d15fc2995b33aec0bd96e.ff356f1ee2b71a5325f24432c7ad0228&client_id=1000.1NTMMZMZNO97I9STVJULF3AW8FLIEP&client_secret=95479c46c31f9452add75082d932478b7ca63d8775&grant_type=refresh_token");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");

            String input = "";

            OutputStream os = conn.getOutputStream();
            os.write(input.getBytes());
            os.flush();

            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

            String output;
            String test = null;

            while ((output = br.readLine()) != null) {
                test = output;
                // System.out.println(output);
            }

            JSONParser jsonParser = new JSONParser();
            // System.out.println(test);
            try {
                // Read JSON file
                JSONObject obj = (JSONObject) jsonParser.parse(test);
                access_token = (String) obj.get("access_token");
                System.out.println("access token is " + access_token);

                Long expires_in_sec = (Long) obj.get("expires_in");
                System.out.println("expires_in_sec token is " + expires_in_sec);

            } catch (ParseException e) {
                e.printStackTrace();
            }

            conn.disconnect();

        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (IOException e) {

            e.printStackTrace();

        }
        return access_token;
    }

}
