package DBEngine;

import Exceptions.DBAppException;
import com.opencsv.CSVWriter;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class DBApp {

    public static int intMaxRows;
    private final String strDataFolderPath = "src/main/resources/data";
    private final String dateFormat = "yyyy-MM-dd";
    private Vector<Table> tables;
    private File metadataFile;


    public static void main(String[] args) throws DBAppException {

    }

    private static void wrapNull(Hashtable<String, Object> htblColNameValue, Table table) throws DBAppException {
        Set<Entry<String, String>> entrySet = ((table.get_htblColNameType())).entrySet();
        int colsize = table.get_htblColNameType().size();  // are we sure never returns null?
        if (htblColNameValue.size() == colsize) //all columns are instantiated
            return;

        for (Entry<String, String> entry : entrySet) {
            String columnNameOriginal = entry.getKey(); //column name from table
            if (!htblColNameValue.containsKey(columnNameOriginal)) //if the column does not exist in the hashtable
                if (columnNameOriginal.equals(table.get_strClusteringKeyColumn()))
                    throw new DBAppException("Cannot insert row with primary key null");
            String columnType = entry.getValue();
            htblColNameValue.put(columnNameOriginal, new NullObject()); //wrap the null value
        }

    }

    public void init() {
        // create data folder if it doesn't exist
        File dataFolder = new File(strDataFolderPath);
        if (!dataFolder.exists()) {
            dataFolder.mkdir();
        }
        // go to data folder and create metadata.csv if it doesn't exist
        metadataFile = new File("src/main/resources/metadata.csv");
        if (!metadataFile.exists()) {
            try {
                metadataFile.createNewFile();
            } catch (Exception e) {
                System.out.println("Error creating metadata file");
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
        tables = new Vector<Table>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(metadataFile));
            String line = br.readLine();
            while (line != null) {
                String[] lineData = line.split(",");
                String tableName = lineData[0];
                // check if table already exists
                boolean tableExists = false;
                for (Table table : tables) {
                    if (table.get_strTableName().equals(tableName)) {
                        tableExists = true;
                        break;
                    }
                }
                if (!tableExists) {
                    Table newTable = new Table(tableName, strDataFolderPath + "/");
                    tables.add(newTable);
                }
                line = br.readLine();
            }
            br.close();
        } catch (Exception e) {
            System.out.println("Error reading metadata file");
        }

        // get max octree nodes from config file

        // do the rest of the initialization (Still need to figure out what that is)
    }

    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    // htblColNameMin and htblColNameMax for passing minimum and maximum values
    // for data in the column. Key is the name of the column
    //
    //create min and max hashtables in main before create table
    public void createTable(String strTableName, String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax) throws DBAppException {

        //min/max values based on what?
        //add constraint to config file?

        // verify that none of the inputs are null
        if (strTableName == null) {
            throw new DBAppException("Table name is null");
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

        // verify that table name is unique
        for (Table table : tables) {
            if (table.get_strTableName().equals(strTableName)) throw new DBAppException("Table name already exists");
        }

        // verify datatype of all hashtables
        Set<Entry<String, String>> entrySet = htblColNameType.entrySet();
        for (Entry<String, String> entry : entrySet) {
            String columnName = entry.getKey();
            String columnType = entry.getValue();
            if (!(columnType.equals("java.lang.Integer") || columnType.equals("java.lang.Double") ||
                    columnType.equals("java.lang.String") || columnType.equals("java.util.Date"))) {
                throw new DBAppException("Invalid data type");
            } else {
                if (htblColNameMin.get(columnName) == null) {
                    throw new DBAppException("Column min value not found");
                }
                if (htblColNameMax.get(columnName) == null) {
                    throw new DBAppException("Column max value not found");
                }
            }
        }

        try {
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
        } catch (IOException e) {
            throw new DBAppException("Error reading metadata file");
        }

        try {
            // write to metadata file
            CSVWriter writer = new CSVWriter(new FileWriter(metadataFile, true), CSVWriter.DEFAULT_SEPARATOR,
                    CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END); // open csv file
            entrySet = htblColNameType.entrySet(); // what is ht???
            for (Entry<String, String> entry : entrySet) {
                String columnName = entry.getKey(); // get column name
                String columnType = entry.getValue(); // get column type
                boolean clusteringKey = columnName.equals(strClusteringKeyColumn); // check if column is clustering key
                String min = htblColNameMin.get(columnName); // get min value
                String max = htblColNameMax.get(columnName); // get max value
                String[] csvEntry = {strTableName, columnName, columnType, Boolean.toString(clusteringKey), "null", "null", min, max}; // create csv entry
                writer.writeNext(csvEntry); // write csv entry to csv file
            }
            writer.close(); // close csv file
            Table table = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax, strDataFolderPath + "/"); // not sure about the path

            tables.add(table); // add table to tables vector
            table.unloadTable(); // unload table from memory
        } catch (IOException e) {
            throw new DBAppException("Error writing to metadata file");
        }
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

    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, insert the record
        // Create a new record object
        // Add the record to the table
        // Save the table

        //exceptions when inserting
        /*
        1-data type mismatch when comparing to csv
        2-primary key duplicated
         */

        Table tableToInsertInto = getTableFromName(strTableName); // get reference to table
        tableToInsertInto.loadTable(); // load the table into memory

        htblColNameValue = castToLowerCase(htblColNameValue, tableToInsertInto);
        wrapNull(htblColNameValue, tableToInsertInto); //wrap Null method call in case not all attributes are included in the hashtable

        // verify that the input row violates no constraints
        verifyRow(tableToInsertInto, htblColNameValue);
       /* try {
            verifyRow(tableToInsertInto, htblColNameValue);
        } catch (DBAppException e) {
            tableToInsertInto.unloadTable(); // unload the table
            throw e;
        }*/

        tableToInsertInto.insertRow(htblColNameValue);

        tableToInsertInto.unloadTable(); // unload the table
    }

    // Helper methods

    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName, String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, update the record
        // Create a new record object
        // Add the record to the table
        // Save the table

        if (strClusteringKeyValue == null)
            throw new DBAppException("Clustering key is null");

        Table tableToUpdate = getTableFromName(strTableName); // get reference to table
        tableToUpdate.loadTable(); // load the table into memory

        // verify that the input row violates no constraints
        try {
            htblColNameValue = castToLowerCase(htblColNameValue, tableToUpdate);
            verifyRow(tableToUpdate, htblColNameValue);
        } catch (DBAppException e) {
            tableToUpdate.unloadTable();
            throw e;
        }

        // cast the clustering key value to the correct type
        String clusteringKeyDataType = tableToUpdate.get_htblColNameType().get(tableToUpdate.get_strClusteringKeyColumn());
        Object adjustedClusteringKeyValue = castValue(clusteringKeyDataType, strClusteringKeyValue);

        // update the row
        tableToUpdate.updateRow(htblColNameValue, adjustedClusteringKeyValue);
        tableToUpdate.unloadTable(); // unload the table
    }

    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue entries are ANDed together
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
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

        Table tableToDeleteFrom = getTableFromName(strTableName); // get reference to table
        tableToDeleteFrom.loadTable(); // load the table into memory

        // verify that the input row violates no constraints, if it does, do nothing
        try {
            htblColNameValue = castToLowerCase(htblColNameValue, tableToDeleteFrom);
            verifyRow(tableToDeleteFrom, htblColNameValue);
        } catch (DBAppException e) {
            if (e.getMessage().contains("Value out of range")) { // if the error is that the value is out of range, unload the table and return
                tableToDeleteFrom.unloadTable();
                return;
            }
            throw e; // if the error is anything else, throw it
        }

        if (htblColNameValue.containsKey(tableToDeleteFrom.get_strClusteringKeyColumn())) {
            // if the clustering key is in the hashtable, delete only one row
            Object clusteringKeyValue = htblColNameValue.get(tableToDeleteFrom.get_strClusteringKeyColumn());
            Page page = tableToDeleteFrom.getPageFromClusteringKey(clusteringKeyValue); // get the page that contains the row to delete
            int intRowID = tableToDeleteFrom.getRowIDFromClusteringKey(page, clusteringKeyValue);// find the index of the row to delete using binary search
            if (intRowID == -1 || page == null) // if the row/page is not found don't delete
                return;
            tableToDeleteFrom.deleteRow(page, intRowID); // delete the row at that index
        } else {
            // if the clustering key is not in the hashtable, delete all rows that match the other conditions
            // currently it goes through the rows linearly
            for (int i = 0; i < tableToDeleteFrom.get_pagesID().size(); i++) {
                String currPageID = tableToDeleteFrom.get_pagesID().get(i);
                Page currPage = Page.loadPage(tableToDeleteFrom.get_strPath(), tableToDeleteFrom.get_strTableName(), currPageID);
                for (int j = 0; j < currPage.get_intNumberOfRows(); j++) { // loop through all rows in the table
                    boolean toDelete = true; // assume the row should be deleted
                    for (Entry<String, Object> entry : htblColNameValue.entrySet()) { // loop through all entries in the hashtable
                        if (!currPage.get_rows().get(j).get(entry.getKey()).equals(entry.getValue())) { // check if the row value matches the hashtable value for each column name
                            toDelete = false; // if not, set toDelete to false and break the loop
                            break;
                        }
                    }
                    if (toDelete) { // if toDelete is still true after checking all columns
                        tableToDeleteFrom.deleteRow(currPage, j); // delete the row
                        j--; // decrement i to account for the deleted row
                    }
                }
                if (tableToDeleteFrom.get_pagesID().get(i) != currPageID) // if the page was deleted, decrement i to account for the deleted page
                    i--;
                currPage.unloadPage();
            }
        }
        tableToDeleteFrom.unloadTable(); // unload the table
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, select the records
        // Create a new record object
        // Add the record to the table
        // Save the table
        // Return the iterator
        return null;
    }

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

    private void verifyRow(Table table, Hashtable<String, Object> htblRow) throws DBAppException {
        try {
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
                    try {
                        Date min = new SimpleDateFormat(dateFormat).parse(table.get_htblColNameMin().get(columnName));
                        Date max = new SimpleDateFormat(dateFormat).parse(table.get_htblColNameMax().get(columnName));
                        if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
                            throw new DBAppException("Value out of range");
                        }
                    } catch (ParseException e) {
                        throw new DBAppException("Date format is incorrect");
                    }
                }
            }
            br.close();
        } catch (IOException e) {
            throw new DBAppException("Error reading metadata file");
        }
    }

    private Object castValue(String type, String value) throws DBAppException {
        if (type.equals("java.lang.Integer")) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new DBAppException("Invalid integer value");
            }
        } else if (type.equals("java.lang.Double")) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                throw new DBAppException("Invalid double value");
            }
        } else if (type.equals("java.lang.String")) {
            return value;
        } else if (type.equals("java.util.Date")) {
            try {
                return new SimpleDateFormat(dateFormat).parse(value);
            } catch (ParseException e) {
                throw new DBAppException("Invalid date format (yyyy-MM-dd)");
            }
        }
        return null;
    }


    private Hashtable<String, Object> castToLowerCase(Hashtable<String, Object> htblColNameValue, Table table) throws DBAppException {
        Set<Entry<String, Object>> entrySet = htblColNameValue.entrySet();
        Hashtable<String, Object> newRow = new Hashtable<String, Object>();
        for (Entry<String, Object> entry : entrySet) {
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();
            String columnType = table.get_htblColNameType().get(columnName);

            // check if column exists in table
            if (columnType == null)
                throw new DBAppException("Column" + columnName + "does not exist in the table");

            if (columnType.equals("java.lang.String"))
                newRow.put(columnName, ((String) columnValue).toLowerCase());
            else
                newRow.put(columnName, columnValue);
        }
        return newRow;
    }
}
