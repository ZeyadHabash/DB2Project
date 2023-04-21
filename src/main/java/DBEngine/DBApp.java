package DBEngine;

import Exceptions.DBAppException;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class DBApp {

    public static int intMaxRows;
    private Vector<Table> tables;
    private File metadataFile;

    public static void main(String[] args) throws DBAppException, IOException, CsvValidationException, ParseException {

      //  String strTableName = "Student";


        String strTableName = "Student";
        DBApp dbApp = new DBApp( );
        dbApp.init();
       /* Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        Hashtable htblColNameMin = new Hashtable();
        htblColNameMin.put("id", "0");
        htblColNameMin.put("name", "A" ) ;
        htblColNameMin.put("gpa", "0.0" );
        Hashtable htblColNameMax = new Hashtable( );
        htblColNameMax.put("id", "1000000");
        htblColNameMax.put("name","ZZZZZZZ");
        htblColNameMax.put("gpa","4.0" );
        dbApp.createTable( strTableName, "id", htblColNameType , htblColNameMin,htblColNameMax);


        Hashtable htblColNameValue = new Hashtable( );
        htblColNameValue.put("id", new Integer( 9 ));
        htblColNameValue.put("name", new String("Ahmed Noor" ) );
        htblColNameValue.put("gpa", new Double( 0.95 ) );
        dbApp.insertIntoTable( strTableName , htblColNameValue );*/

        Hashtable htblColNameValue = new Hashtable( );
        htblColNameValue.put("id", new Integer( 1 ));
        htblColNameValue.put("name", new String("Mahmoud Khaled" ) );
        htblColNameValue.put("gpa", new Double( 2.0 ) );
        dbApp.insertIntoTable( strTableName , htblColNameValue );
    }

    public void init() throws DBAppException {
        // create data folder if it doesn't exist
        File dataFolder = new File("data");
        if (!dataFolder.exists()) {
            dataFolder.mkdir();
        }
        // go to data folder and create metadata.csv if it doesn't exist
        metadataFile = new File("data/metadata.csv");
        if (!metadataFile.exists()) {
            try {
                metadataFile.createNewFile();
            } catch (Exception e) {
                throw new DBAppException("Error creating metadata file");
            }
        }
        // TODO: store min and max values in config file
        // get max rows from config file
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("src/main/resources/DBApp.config"));
            intMaxRows = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
        } catch (IOException e) {
            e.printStackTrace();
        }



        // Read metadata.csv and add tables to tables vector
        tables = new Vector<Table>(); //initially empty since it is volatile
        try {
            BufferedReader br = new BufferedReader(new FileReader(metadataFile));
            String line = br.readLine();
            while (line != null) { //populate table vector from meta data file (is this loading pointers only)?
                String[] lineData = line.split(",");
                String tableName = lineData[0];
                // check if table already exists
                boolean tableExists = false;
                for (Table table : tables) {
                    if (table.get_strTableName().equals(tableName)) {
                        tableExists = true;
                        break;
                    }
                }//.class thing in split.
                if (!tableExists) { //test with print line
                    //String datapath= "data/" +tableName+".class";
                    String datapath= "data/" +tableName;
                    Table newTable = new Table(tableName, "data/");
                    tables.add(newTable);

                }
                line = br.readLine();
            }
            br.close();
        } catch (Exception e) {
            throw new DBAppException("Error reading metadata file");
        }

        // get max octree nodes from config file

        // do the rest of the initialization (Still need to figure out what that is)
    }






    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType,
                            Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax) throws DBAppException, IOException {


        if (strTableName == null) {
            throw new DBAppException("Table name is null");
        }
        if (strClusteringKeyColumn == null) {
            throw new DBAppException("Clustering key is null");
        }
       if (htblColNameType.get(strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering key column not found");
        }
        if (htblColNameMin.get(strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering key column min value not found");
        }
        if (htblColNameMax.get(strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering key column max value not found");
        }
        //check that all columns in hts exist in the rest of the hts
        // verify datatype of all hashtables



        Set<Entry<String, String>> entrySet = htblColNameType.entrySet();
        for (Entry<String, String> entry : entrySet) {
            String columnName = entry.getKey();  // ID --> value
            String columnType = entry.getValue(); // Integer
            if (!(columnType.equals("java.lang.Integer") || columnType.equals("java.lang.Double") || columnType.equals("java.lang.String") || columnType.equals("java.util.Date"))) {
                throw new DBAppException("Invalid data type");
            } else {
                if (htblColNameMin.get(columnName) == null) { //checks if column in min ht also value
                    throw new DBAppException("Column min value or Column not found in min HT");
                }
                if (htblColNameMax.get(columnName) == null) {
                    throw new DBAppException("Column max value or Column not found in max HT");
                }

            }
        }
        if(htblColNameMax.size() != htblColNameMin.size() || htblColNameMax.size() != htblColNameType.size() || htblColNameMin.size() != htblColNameType.size()){
            throw new DBAppException("Columns in HTs are not equal (size)");
        }



        // check if table already exists
        BufferedReader br = new BufferedReader(new FileReader(metadataFile)); // read csv file
        String line = br.readLine();
        while (line != null) { // loop over all lines
            String[] values = line.split(",");
            if (values[0].equals(strTableName)) { // check if table name already exists
                throw new DBAppException("Table already exists"); // if it does, throw exception
            }
            line = br.readLine();
        }
        br.close();

        // write to metadata file
        CSVWriter writer = new CSVWriter(new FileWriter(metadataFile, true));
        //String csvEntry = strTableName;
        entrySet = htblColNameType.entrySet(); // what is ht???
        for (Entry<String, String> entry : entrySet) {
            //csvEntry = strTableName;
            String columnName = entry.getKey();
            String columnType = entry.getValue();
            boolean clusteringKey = columnName.equals(strClusteringKeyColumn);
            //csvEntry += ", " + columnName + "," + columnType + "," + clusteringKey + ",null,null,";
            String min =  htblColNameMin.get(columnName);
            String max = htblColNameMax.get(columnName);
            //csvEntry += min + "," + max;
            String[] csvEntry = {strTableName, columnName, columnType, Boolean.toString(clusteringKey), "null", "null", min, max};
            // create csv entry
            writer.writeNext(csvEntry); // write csv entry to file
            writer.flush();
        }
        //pathname??? . class????
        Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin,
                htblColNameMax, "data/"); // not sure about the path
        tables.add(table); // add table to tables vector
        //table.unloadTable(); // unload table from memory
    }

    // following method creates an octree
    // depending on the count of column names passed.
    // If three column names are passed, create an octree.
    // If only one or two column names is passed, throw an Exception.
    public void createIndex(String strTableName, String strarrColName) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, create a new index
        // Create a new index object
        // Add the index to the table
        // Save the table
    }


    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, CsvValidationException , ParseException {


       /* for(Table table: tables) { //check table name is valid
            if(table.get_strTableName().equals(strTableName))
            {
                tableNameExists = true;
                tableColumnCount= table.get_htblColNameType().size();
                break;
            }
        }*/



        boolean tableNameExists = false;

        Set<Entry<String, Object>> entrySet = htblColNameValue.entrySet();

        for(Entry<String, Object> c:entrySet){ //loops on the row htbl to be inserted
            boolean colFound=false;
            String colName=c.getKey();
            Object colVal=c.getValue();
            CSVReader reader = new CSVReader(new FileReader(metadataFile));
            String[] values = reader.readNext();
            int tableColumnCount=0;
            while(values != null)
            { //check table name is valid
                //String[] values = line.split(",");
               // line= line.replaceAll("\"", ""); should we do replace all like in values?
                String tblname= values[0];
                if(tblname.equals(strTableName)) //to go over the meta data file lines related to the table
                {
                    tableNameExists = true;
                    tableColumnCount++;
                    if (values[1].equals(colName))//colName exists next step check valid value 3 cases
                    // min max and data type mismatch
                    // if it is pk check value not null
                    {

                        colFound = true;
                        if (values[3].equals("True")) {
                            if (colVal == null)
                                throw new DBAppException("primary key can not be null");
                        }
                        String type = values[2];
                        switch (type) {
                            case "java.lang.Integer":
                                if (!(colVal instanceof Integer))
                                    throw new DBAppException("Data type mismatch of Integer");
                                break;
                            case "java.lang.String":
                                if (!(colVal instanceof String))
                                    throw new DBAppException("Data type mismatch of String");
                                break;
                            case "java.lang.Double":
                                if (!(colVal instanceof Double))
                                    throw new DBAppException("Data type mismatch of Double");
                                break;
                            case "java.lang.Date":
                                if (!(colVal instanceof Date))
                                    throw new DBAppException("Data not instance of Date");
                                break;
                            default:

                        }
                        String min = values[6];
                        String max = values[7];
                        if (colVal instanceof Integer) {
                            if ((Integer) colVal < Integer.parseInt(min) || (Integer) colVal > Integer.parseInt(max)) {
                                throw new DBAppException("Integer Value out of range");
                            }
                        } else {
                            if (colVal instanceof String) {
                                if (((String) colVal).compareTo(min) < 0 || ((String) colVal).compareTo(max) > 0) {
                                    throw new DBAppException("String Value out of range");

                                }
                            } else {
                                if (colVal instanceof Double) {
                                    if ((Double) colVal < Double.parseDouble(min) || (Double) colVal > Double.parseDouble(max)) {
                                        throw new DBAppException("Double Value out of range");
                                    }
                                } else {
                                    if (colVal instanceof Date) {  //  Date date1=new SimpleDateFormat("dd/MM/yyyy").parse(sDate1);

                                        DateFormat formatter = new SimpleDateFormat("YYYY-MM-DD");
                                        Date dateMin = formatter.parse(min);
                                        Date dateMax = formatter.parse(max);

                                        if (((Date) colVal).compareTo(dateMin) < 0 || ((Date) colVal).compareTo(dateMin) > 0) {
                                            throw new DBAppException("Date Value out of range");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                values = reader.readNext();
            }

            if(!tableNameExists)
                throw new DBAppException("Table name does not exist");

            if(!colFound)
                throw new DBAppException("Column name does not exist");

            if(htblColNameValue.size() != tableColumnCount)
            {
                throw new DBAppException("Number of columns in row does not match number of columns in table");}

            reader.close();
        }

        Table t= new Table(strTableName, "data/");
        t.loadTable();
        binarySearchAndInsert(t, htblColNameValue);

        t.unloadTable();



    }

    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, update the record
        // Create a new record object
        // Add the record to the table
        // Save the table

        // TODO: sort table after updating (?)
    }

    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue entries are ANDed together
    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, delete the record
        // Create a new record object
        // Add the record to the table
        // Save the table

        //exceptions when deleting
        /*
        1- no such row
        ???
         */


        // find indexes of rows to delete
        // how to search? binary search? but it's unsorted??? idk tbh
        // store all indexes to delete in an array the delete them? idk aswell

    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, select the records
        // Create a new record object
        // Add the record to the table
        // Save the table
        // Return the iterator
        return null;
    }

    // Helper methods

    private Table getTableFromName(String strTableName) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, return the table
        for (Table table : tables) {
            if (table.get_strTableName().equals(strTableName)) {
                return table;
            }
        }
        throw new DBAppException("Table not found");
    }

    private void verifyRow(Table table, Hashtable<String, Object> htblRow) throws DBAppException, IOException {
        // verify that the input row violates no constraints
        Set<Entry<String, Object>> entrySet = htblRow.entrySet();
        BufferedReader br = new BufferedReader(new FileReader(metadataFile)); // read csv file
        for (Entry<String, Object> entry : entrySet) {
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();
            String columnType = table.get_htblColNameType().get(columnName);

            // check if data type matches within the row
            if (columnType.equals("java.lang.Integer")) {
                if (!(columnValue instanceof Integer)) {
                    throw new DBAppException("Data type mismatch");
                }
            } else if (columnType.equals("java.lang.Double")) {
                if (!(columnValue instanceof Double)) {
                    throw new DBAppException("Data type mismatch");
                }
            } else if (columnType.equals("java.lang.String")) {
                if (!(columnValue instanceof String)) {
                    throw new DBAppException("Data type mismatch");
                }
            } else if (columnType.equals("java.util.Date")) {
                if (!(columnValue instanceof Date)) {
                    throw new DBAppException("Data type mismatch");
                }
            }

            // check if data types match the table's data types
            String line = br.readLine();
            while (line != null) { // loop over all lines
                String[] values = line.split(","); // split line into values
                if (values[0].equals(table.get_strTableName()) && values[1].equals(columnName)) { // check if table name and column name match
                    if (!values[2].equals(columnType)) { // check if data types match
                        throw new DBAppException("Data type mismatch"); // if they don't, throw exception
                    }
                    break; // if column name and table name match, break out of loop
                }
                line = br.readLine(); // read next line if column name and table name don't match
            }

            // check if all columns are within min and max
            if (columnValue instanceof Integer) {
                int value = (int) columnValue;
                int min = Integer.parseInt(table.get_htblColNameMin().get(columnName));
                int max = Integer.parseInt(table.get_htblColNameMax().get(columnName));
                if (value < min || value > max) {
                    throw new DBAppException("Value out of range");
                }
            } else if (columnValue instanceof Double) {
                double value = (double) columnValue;
                double min = Double.parseDouble(table.get_htblColNameMin().get(columnName));
                double max = Double.parseDouble(table.get_htblColNameMax().get(columnName));
                if (value < min || value > max) {
                    throw new DBAppException("Value out of range");
                }
            } else if (columnValue instanceof String) {
                String value = (String) columnValue;
                String min = table.get_htblColNameMin().get(columnName);
                String max = table.get_htblColNameMax().get(columnName);
                if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
                    throw new DBAppException("Value out of range");
                }
            } else if (columnValue instanceof Date) {
                Date value = (Date) columnValue;
                Date min = new Date(table.get_htblColNameMin().get(columnName));
                Date max = new Date(table.get_htblColNameMax().get(columnName));
                if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
                    throw new DBAppException("Value out of range");
                }
            }
        }
        br.close();
    }


    private void binarySearchAndInsert(Table tableToInsertTo, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        int left = 0; // Initialize left index to 0
        int right = tableToInsertTo.get_intNumberOfRows() - 1; // Initialize right index to last index of the array
        int mid;
        Object midClusteringKey;
        Object newRowClusteringKey = tableToInsertTo.getClusteringKeyFromRow(htblColNameValue);

        // Perform binary search to check if element exists
        while (left <= right) { // Loop until left index becomes greater than right index
            mid = (left + right) / 2; // Find the middle index
            midClusteringKey = tableToInsertTo.getClusteringKeyFromRow(tableToInsertTo.getRowFromIndex(mid));
            if (midClusteringKey.equals(newRowClusteringKey)) { // If the middle element is equal to the given element
                throw new DBAppException("Primary key duplicated"); // Throw an exception
            } else if (((Comparable) midClusteringKey).compareTo(newRowClusteringKey) < 0) { // If the middle element is less than the given element
                left = mid + 1; // Update left index to mid+1
            } else { // If the middle element is greater than the given element
                right = mid - 1; // Update right index to mid-1
            }
        }
        // Element doesn't exist in the array, insert it at the correct position
        tableToInsertTo.insertRow(htblColNameValue, left);
    }


    // This method performs a binary search on a table object
    private int binarySearch(Table tableToSearchIn, String strClusteringKeyValue) {
        // Call the recursive helper method
        return binarySearchHelper(tableToSearchIn, 0, tableToSearchIn.get_intNumberOfRows() - 1, strClusteringKeyValue);
    }
    private int binarySearchHelper(Table tableToSearchIn, int intMinIndex, int intMaxIndex, String strClusteringKeyValue) {
        // Calculate the middle index of the table
        int mid = (intMinIndex + intMaxIndex) / 2;
        // Get the clustering key value of the row at the middle index
        Object midClusteringKey = tableToSearchIn.getClusteringKeyFromRow(tableToSearchIn.getRowFromIndex(mid));
        // Check if the minimum index is greater than the maximum index, or if the middle index is out of bounds
        if (intMinIndex > intMaxIndex || mid < 0 || mid >= tableToSearchIn.get_intNumberOfRows())
            return -1; // Return -1 to indicate that the key value was not found

        // Check if the clustering key value at the middle index matches the target value
        if (midClusteringKey.equals(strClusteringKeyValue))
            return mid; // Return the middle index as the result
        // Check if the clustering key value at the middle index is greater than the target value
        else if (((Comparable) midClusteringKey).compareTo(strClusteringKeyValue) > 0)
            return binarySearchHelper(tableToSearchIn, intMinIndex, mid - 1, strClusteringKeyValue); // Recursively search in the left half of the table
        else
            return binarySearchHelper(tableToSearchIn, mid + 1, intMaxIndex, strClusteringKeyValue); // Recursively search in the right half of the table
    }
}
