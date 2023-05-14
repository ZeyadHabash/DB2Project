package DBEngine;

import DBEngine.Octree.Octree;
import DBEngine.Octree.OctreeEntry;
import Exceptions.DBAppException;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class DBApp {

    public static final String dateFormat = "yyyy-MM-dd";
    public static int intMaxRows;
    public static int intMaxEntriesPerNode;
    private final String strDataFolderPath = "resources/data";
    private Vector<Table> tables;
    private File metadataFile;


    public static void main(String[] args) throws DBAppException {
        String strTableName = "Student";
        DBApp dbApp = new DBApp();

        dbApp.init();

//        Hashtable htblColNameType = new Hashtable();
//
//        Hashtable htblColNameMin = new Hashtable();
//        Hashtable htblColNameMax = new Hashtable();
//
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameMin.put("id", "0");
//        htblColNameMax.put("id", "99999999");
//
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameMin.put("name", "a");
//        htblColNameMax.put("name", "zzzzzzzzzzzzzzzzzzzzzzz");
//
//        htblColNameType.put("gpa", "java.lang.Double");
//        htblColNameMin.put("gpa", "0.0");
//        htblColNameMax.put("gpa", "5.0");
//
//
//        dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);
//        dbApp.createIndex(strTableName, new String[]{"gpa", "name", "id"});
//
//
//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("id", new Integer(2343432));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(453455));
//        htblColNameValue.put("name", new String("Ahmed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(5674567));
//        htblColNameValue.put("name", new String("Dalia Noor"));
//        htblColNameValue.put("gpa", new Double(1.25));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(23498));
//        htblColNameValue.put("name", new String("John Noor"));
//        htblColNameValue.put("gpa", new Double(1.5));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new Integer(78452));
//        htblColNameValue.put("name", new String("Zaky Noor"));
//        htblColNameValue.put("gpa", new Double(0.88));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//        Table table = dbApp.getTableFromName(strTableName);
//        table.loadTable();
////        System.out.println(table.get_indices());
//        Octree index = table.getIndexOnRows(new String[] {"id", "name", "gpa"});
////        System.out.println(index);
//
//
//
//        SQLTerm[] arrSQLTerms;
//        arrSQLTerms = new SQLTerm[3];
//        arrSQLTerms[0] = new SQLTerm("Student", "id", ">=", 78452);
//        arrSQLTerms[1] = new SQLTerm("Student", "name", "=", "Zaky noor");
//        arrSQLTerms[2] = new SQLTerm("Student", "gpa", ">=", 0.88);
//        String[] strarrOperators = new String[2];
//        strarrOperators[0] = "AND";
//        strarrOperators[1] = "AND";
////        strarrOperators[2] = "AND";
//
//
//        // select * from Student where name = “John Noor” or gpa = 1.5;
//        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//
//        while (resultSet.hasNext()) {
//            System.out.println(resultSet.next());
//        }

        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("gpa", new Double(1.5));
        htblColNameValue.put("id", new Integer(23498));
        htblColNameValue.put("name", new String("john noor"));
        System.out.println(htblColNameValue);
        dbApp.deleteFromTable(strTableName, htblColNameValue);

        Table table = dbApp.getTableFromName(strTableName);
        table.loadTable();
        Octree index = table.getIndexOnRows(new String[] {"id", "name", "gpa"});
        System.out.println(table);
        System.out.println("--------------------------------------------------------------------------------------------------------------------------------------------");
        System.out.println(index);
    }

    private static void detectNulls(Hashtable<String, Object> htblColNameValue, Table table) throws DBAppException {
        Set<Entry<String, String>> entrySet = ((table.get_htblColNameType())).entrySet();
        int colsize = table.get_htblColNameType().size();  // are we sure never returns null?
        if (htblColNameValue.size() == colsize) //all columns are instantiated
            return;

        for (Entry<String, String> entry : entrySet) {
            String columnNameOriginal = entry.getKey(); //column name from table
            if (!htblColNameValue.containsKey(columnNameOriginal)) {  //if the column does not exist in the hashtable
                throw new DBAppException("Cannot insert row with null value in column " + columnNameOriginal);
//                if (columnNameOriginal.equals(table.get_strClusteringKeyColumn()))
//                    throw new DBAppException("Cannot insert row with primary key null");
//                String columnType = entry.getValue();
//                htblColNameValue.put(columnNameOriginal, new NullObject()); //wrap the null value
            }
        }
    }

    public static void updateCSV(File fileToUpdate, String replaceCol1, String replaceCol2,
                                 int[] rows, int col1, int col2) throws IOException, CsvException {


        // Read existing file
        CSVReader reader = new CSVReader(new FileReader(fileToUpdate));
        List<String[]> csvBody = reader.readAll();
        // get CSV row column  and replace with by using row and column
        for (int row : rows) {
            csvBody.get(row)[col1] = replaceCol1;
            csvBody.get(row)[col2] = replaceCol2;
        }
//        csvBody.get(row)[col] = replace;
        reader.close();

        // Write to CSV file which is open
        CSVWriter writer = new CSVWriter(new FileWriter(fileToUpdate), CSVWriter.DEFAULT_SEPARATOR,
                CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
        writer.writeAll(csvBody);
        writer.flush();
        writer.close();
    }

    public void init() {
        // create data folder if it doesn't exist
        File dataFolder = new File(strDataFolderPath);
        if (!dataFolder.exists()) {
            dataFolder.mkdir();
        }
        // go to data folder and create metadata.csv if it doesn't exist
        metadataFile = new File("resources/metadata.csv");
        if (!metadataFile.exists()) {
            try {
                metadataFile.createNewFile();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error creating metadata file");
            }
        }

        // get max rows from config file
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("resources/DBApp.config"));
            intMaxRows = Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage"));
            intMaxEntriesPerNode = Integer.parseInt(prop.getProperty("MaximumEntriesinOctreeNode"));
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
            e.printStackTrace();
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

    // Helper methods

    // following method creates an octree
    // depending on the count of column names passed.
    // If three column names are passed, create an octree.
    // If only one or two column names is passed, throw an Exception.
    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, create a new index
        // Create a new index object
        // Add the index to the table
        // Save the table

        if (strTableName == null)
            throw new DBAppException("Table name is null");
        if (strarrColName == null)
            throw new DBAppException("Column name is null");
        if (strarrColName.length < 3)
            throw new DBAppException("Not enough columns to create an octree");

        //write to csv after creating the index
        Table tableToCreateIndexOn = getTableFromName(strTableName); // get reference to table
        tableToCreateIndexOn.loadTable(); // load the table into memory

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();

        int[] rows = new int[strarrColName.length];

        try {
            BufferedReader br = new BufferedReader(new FileReader(metadataFile)); // read csv file
            String line = br.readLine();
            int currentRowInCSV = 0;
            int rowsFound = 0;
            while (line != null) { // loop over all lines
                String[] values = line.split(",");
                if (values[0].equals(strTableName)) {
                    boolean alreadyIndexed = false;
                    for (int i = 0; i < strarrColName.length; i++) {
                        if (values[1].equals(strarrColName[i])) {
                            alreadyIndexed = true;
                            if (values[4].equals("null")) {
                                alreadyIndexed = false;
                                htblColNameType.put(values[1], values[2]);
                                rows[rowsFound] = currentRowInCSV;
                                rowsFound++;
                            }
                            break; // if column name is found, break out of loop
                        }
                    }
                    if (alreadyIndexed)
                        throw new DBAppException("Column name not found or already indexed");
                }

                if (htblColNameType.size() == strarrColName.length) // if all columns are found
                    break; // break out of loop

                line = br.readLine();
                currentRowInCSV++;
            }
            br.close();
        } catch (IOException e) {
            throw new DBAppException("Error reading metadata file");
        }

        String strIndexName = "";
        for (int i = 0; i < strarrColName.length; i++) {
            strIndexName += strarrColName[i];
        }
        strIndexName += "Index";

        // create index
        Octree index = new Octree(tableToCreateIndexOn, htblColNameType, strIndexName);

        // add index to table
        tableToCreateIndexOn.addAndPopulateIndex(index);

        // edit csv
        try {
            updateCSV(metadataFile, strIndexName, "Octree", rows, 4, 5);
        } catch (IOException | CsvException e) {
            throw new DBAppException("Error writing to metadata file");
        }

        // save and unload table
        tableToCreateIndexOn.unloadTable();
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

        if (strTableName == null)
            throw new DBAppException("Table name is null");
        if (htblColNameValue == null)
            throw new DBAppException("Row is null");

        Table tableToInsertInto = getTableFromName(strTableName); // get reference to table
        tableToInsertInto.loadTable(); // load the table into memory

        htblColNameValue = castToLowerCase(htblColNameValue, tableToInsertInto);
        detectNulls(htblColNameValue, tableToInsertInto); //wrap Null method call in case not all attributes are included in the hashtable

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

        if (strTableName == null)
            throw new DBAppException("Table name is null");
        if (htblColNameValue == null)
            throw new DBAppException("Row is null");
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

        Page page = null;
        // check if clustering key is indexed
        Octree index = tableToUpdate.columnHasIndex(tableToUpdate.get_strClusteringKeyColumn());
        if (index == null)  // if not indexed
            page = tableToUpdate.getPageFromClusteringKey(adjustedClusteringKeyValue);
        else { // if indexed
            // put the clustering key value in a hashtable
            Hashtable<String, Object> clusteringKey = new Hashtable<>();
            clusteringKey.put(tableToUpdate.get_strClusteringKeyColumn(), adjustedClusteringKeyValue);

            Vector<OctreeEntry> entries = index.getRowsFromCondition(clusteringKey);
            int i = entries.get(0).get_objVectorEntryPk().indexOf(adjustedClusteringKeyValue);
            String pageID = entries.get(0).get_strVectorPages().get(i);
            page = Page.loadPage(tableToUpdate.get_strPath(), tableToUpdate.get_strTableName(), pageID);

        }

        tableToUpdate.updateRow(htblColNameValue, adjustedClusteringKeyValue, page); // update the row

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

        if (strTableName == null)
            throw new DBAppException("Table name is null");
        if (htblColNameValue == null)
            throw new DBAppException("Row is null");

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

        // find all indexed columns passed in the hashtable
        Vector<HashMap> indexedUnindexedColumns = getIndexedColumns(htblColNameValue, tableToDeleteFrom);
        HashMap<Octree, Hashtable<String, Object>> htblIndexColNameValue = indexedUnindexedColumns.get(0);
        HashMap<String, Object> htblColNameValueUnindexed = indexedUnindexedColumns.get(1);


        if (htblIndexColNameValue.isEmpty()) {
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
                    if (!Objects.equals(tableToDeleteFrom.get_pagesID().get(i), currPageID)) // if the page was deleted, decrement i to account for the deleted page
                        i--;
                    currPage.unloadPage();
                }
            }
        } else {
            Set<Entry<Octree, Hashtable<String, Object>>> indexedColumns = htblIndexColNameValue.entrySet();
            Vector<OctreeEntry> rowsFromIndices = getRowsFromIndices(indexedColumns);
            deleteUnindexedIndexedIntersection(rowsFromIndices, htblColNameValueUnindexed, tableToDeleteFrom);
        }
        tableToDeleteFrom.unloadTable(); // unload the table
    }

    private Vector<HashMap> getIndexedColumns(Hashtable<String, Object> htblColNameValue, Table table) throws DBAppException {
        HashMap<Octree, Hashtable<String, Object>> indexedColumns = new HashMap<Octree, Hashtable<String, Object>>();
        HashMap<String, Object> unindexedColumns = new HashMap<String, Object>();
        for (Entry<String, Object> entry : htblColNameValue.entrySet()) {
            String colName = entry.getKey();
            Object colValue = entry.getValue();
            System.out.println("column name:" + colName);
            Octree index = table.columnHasIndex(colName);
            System.out.println("index:" + index.get_strIndexName());
            if (index != null) {
                boolean found = false;
                Set<Entry<Octree, Hashtable<String, Object>>> h = indexedColumns.entrySet();
                for (Entry<Octree, Hashtable<String, Object>> entry1 : h) {
                    if (entry1.getKey().get_strIndexName().equals(index.get_strIndexName())) {
                        index = entry1.getKey();
                        found = true;
                        break;
                    }
                }
                if (found) {
                    System.out.println("putting in old index");
                    indexedColumns.get(index).put(colName, colValue);
                }
                else {
                    System.out.println("putting in new index");
                    indexedColumns.put(index, new Hashtable<String, Object>() {{
                        put(colName, colValue);
                    }});
                }
            } else {
                unindexedColumns.put(colName, colValue);
            }
        }
        return new Vector<HashMap>() {{
            add(indexedColumns);
            add(unindexedColumns);
        }};
    }

    private Vector<OctreeEntry> getRowsFromIndices(Set<Entry<Octree, Hashtable<String, Object>>> indexedColumns) {
        Vector<OctreeEntry> rowsFromIndices = new Vector<OctreeEntry>();
        for (Entry<Octree, Hashtable<String, Object>> entry : indexedColumns) {
            Octree index = entry.getKey();
            Hashtable<String, Object> colNameValue = entry.getValue();
            index.loadOctree();
            if (rowsFromIndices.isEmpty())
                rowsFromIndices = index.getRowsFromCondition(colNameValue);
            else {
                Vector<OctreeEntry> tmpVec = index.getRowsFromCondition(colNameValue);
                for (int i = 0; i < rowsFromIndices.size(); i++) {
                    if (!tmpVec.contains(rowsFromIndices.get(i))) {
                        rowsFromIndices.remove(i);
                        i--;
                    }
                }
            }
            index.unloadOctree();
        }
        return rowsFromIndices;
    }

    private void deleteUnindexedIndexedIntersection(Vector<OctreeEntry> rowsFromIndices,
                                                    HashMap<String, Object> htblColNameValueUnindexed,
                                                    Table tableToDeleteFrom) throws DBAppException {
        for (int i = 0; i < rowsFromIndices.size(); i++) {
            OctreeEntry currEntry = rowsFromIndices.get(i);
            for (int j = 0; j < currEntry.get_strVectorPages().size(); j++) {
                int currEntrySize = currEntry.get_strVectorPages().size(); // size before deleting
                String currPageID = currEntry.get_strVectorPages().get(j);
                Page currPage = Page.loadPage(tableToDeleteFrom.get_strPath(), tableToDeleteFrom.get_strTableName(), currPageID);
                Object clusteringKey = currEntry.get_objVectorEntryPk().get(j);
                int intRowID = tableToDeleteFrom.getRowIDFromClusteringKey(currPage, clusteringKey);
                boolean toDelete = true;
                for (Entry<String, Object> entry : htblColNameValueUnindexed.entrySet()) {
                    String colName = entry.getKey();
                    Object colValue = entry.getValue();
                    Hashtable<String, Object> row = currPage.get_rows().get(intRowID);
                    if (!row.get(colName).equals(colValue)) {
                        toDelete = false;
                        break;
                    }
                }
                if (toDelete) {
                    tableToDeleteFrom.deleteRow(currPage, intRowID);
                    if (currEntrySize != currEntry.get_strVectorPages().size()) // if something was deleted from the entry
                        j--;
                }
                currPage.unloadPage();
            }
        }
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        // Check if the table exists
        // If it doesn't, throw an exception
        // If it does, select the records
        // Create a new record object
        // Add the record to the table
        // Save the table
        // Return the iterator

        if (arrSQLTerms == null)
            throw new DBAppException("SQLTerms is null");
        if (strarrOperators == null)
            throw new DBAppException("Operators is null");
        if (arrSQLTerms.length == 0)
            throw new DBAppException("SQLTerms is empty");
        if (strarrOperators.length != arrSQLTerms.length - 1)
            throw new DBAppException("Operator amount doesn't match SQLTerm amount");

        // verify that the operators are valid
        for (String operator : strarrOperators) {
            if (operator != "AND" && operator != "OR" && operator != "XOR")
                throw new DBAppException("Invalid operator");
        }
        // verify that the SQLTerms are valid
        arrSQLTerms = verifySQLTerm(arrSQLTerms);

        // get the table
        Table tableToSelectFrom = getTableFromName(arrSQLTerms[0]._strTableName);

        // load the table
        tableToSelectFrom.loadTable();

        // loop over SQLTerms and find 3 ANDed terms that are indexed
        // if found, use the index to find the rows
        Vector<Hashtable<String, Object>> rows = getSelectRows(arrSQLTerms, strarrOperators, tableToSelectFrom);
        Iterator result = rows.iterator();
        tableToSelectFrom.unloadTable();
        return result;
    }


    private SQLTerm[] verifySQLTerm(SQLTerm[] arrSQLTerms) throws DBAppException {
        SQLTerm[] newSQLTerms = new SQLTerm[arrSQLTerms.length];

        for (int i = 0; i < arrSQLTerms.length; i++) {

            SQLTerm sqlTerm = arrSQLTerms[i];

            // verify that none of the SQLTerm fields are null
            if (sqlTerm._strTableName == null)
                throw new DBAppException("Table name is null");
            if (sqlTerm._strColumnName == null)
                throw new DBAppException("Column name is null");
            if (sqlTerm._objValue == null)
                throw new DBAppException("Value is null");
            if (sqlTerm._strOperator == null)
                throw new DBAppException("Operator is null");

            // verify that the operator is valid
            if (sqlTerm._strOperator != "=" && sqlTerm._strOperator != "<" && sqlTerm._strOperator != ">"
                    && sqlTerm._strOperator != "<=" && sqlTerm._strOperator != ">=" && sqlTerm._strOperator != "!=")
                throw new DBAppException("Invalid operator");

            // verify that the table exists
            Table table = getTableFromName(sqlTerm._strTableName);
            table.loadTable();

            // verify that the column exists
            if (!table.get_htblColNameType().containsKey(sqlTerm._strColumnName))
                throw new DBAppException("Column name not found");

            // verify that the value is of the correct type
            String columnType = table.get_htblColNameType().get(sqlTerm._strColumnName);
            String valueType = sqlTerm._objValue.getClass().getName();
            if (!columnType.equals(valueType))
                throw new DBAppException("Value type doesn't match column type");

            if (arrSQLTerms[i]._objValue instanceof String)
                newSQLTerms[i] = new SQLTerm(arrSQLTerms[i]._strTableName, arrSQLTerms[i]._strColumnName, arrSQLTerms[i]._strOperator, ((String) arrSQLTerms[i]._objValue).toLowerCase());
            else
                newSQLTerms[i] = arrSQLTerms[i];
        }
        return newSQLTerms;
    }


    private Vector<Hashtable<String, Object>> getSelectRows(SQLTerm[] arrSQLTerms, String[] strarrOperators, Table table) throws DBAppException {
        // loop over SQLTerms and find 3 ANDed terms that are indexed
        // if found, use the index to find the rows

        Vector<Vector<Hashtable<String, Object>>> currentSetOfRows = new Vector<Vector<Hashtable<String, Object>>>();

        Vector<String> operators = new Vector<String>();

        boolean flag = false;

        for (int i = 0; i < strarrOperators.length; i++) {
            if (strarrOperators[i].equals("AND")) {
                if (i != strarrOperators.length - 1 && strarrOperators[i + 1].equals("AND")) {
                    String[] strarrColumns = {arrSQLTerms[i]._strColumnName, arrSQLTerms[i + 1]._strColumnName, arrSQLTerms[i + 2]._strColumnName};
                    Octree index = table.getIndexOnRows(strarrColumns);
                    if (index == null) {
                        // no index found so and 3ady
                        currentSetOfRows.add(table.getRowsFromSQLTerm(arrSQLTerms[i]));
                        operators.add(strarrOperators[i]);
                        continue;
                    } else {
                        Vector<Hashtable<String, Object>> listOfIndexedRows = new Vector<Hashtable<String, Object>>();
                        // use the index to find the rows
                        Vector<OctreeEntry> entriesFromIndex = index.getRowsFromCondition(new SQLTerm[]{arrSQLTerms[i],
                                arrSQLTerms[i + 1], arrSQLTerms[i + 2]});
                        for (OctreeEntry entry : entriesFromIndex) {
                            listOfIndexedRows.addAll(table.getRowsfromEntry(entry));
                        }
                        index.unloadOctree();
                        currentSetOfRows.add(listOfIndexedRows);
                        i += 2; // skip the next 2 terms as they're already indexed
                        if (i < strarrOperators.length)
                            operators.add(strarrOperators[i]);
                        else
                            flag = true;
                    }
                } else { // no AND after (including last AND)
                    currentSetOfRows.add(table.getRowsFromSQLTerm(arrSQLTerms[i]));
                    operators.add(strarrOperators[i]);
                }
            } else { // OR or XOR
                currentSetOfRows.add(table.getRowsFromSQLTerm(arrSQLTerms[i]));
                operators.add(strarrOperators[i]);
            }
        }
        // if last term wasnt part of index add it
        if (!flag)
            currentSetOfRows.add(table.getRowsFromSQLTerm(arrSQLTerms[arrSQLTerms.length - 1]));

        // now we have all the rows and operators
        // we need to perform the operations
        Vector<Hashtable<String, Object>> result = currentSetOfRows.get(0);
        for (int i = 0; i < operators.size(); i++) {
            result = performOperation(result, currentSetOfRows.get(i + 1), operators.get(i));
        }

        return result;
    }

    private Vector<Hashtable<String, Object>> performOperation(Vector<Hashtable<String, Object>> rows1, Vector<Hashtable<String, Object>> rows2, String operator) {
        Vector<Hashtable<String, Object>> result = new Vector<Hashtable<String, Object>>();
        if (operator.equals("AND")) {
            for (Hashtable<String, Object> row1 : rows1) {
                for (Hashtable<String, Object> row2 : rows2) {
                    if (row1.equals(row2)) {
                        if (!result.contains(row1))
                            result.add(row1);
                    }
                }
            }
        } else if (operator.equals("OR")) {
            for (Hashtable<String, Object> row1 : rows1) {
                if (!result.contains(row1))
                    result.add(row1);
            }
            for (Hashtable<String, Object> row2 : rows2) {
                if (!result.contains(row2))
                    result.add(row2);
            }
        } else if (operator.equals("XOR")) {
            for (Hashtable<String, Object> row1 : rows1) {
                if (!rows2.contains(row1)) {
                    if (!result.contains(row1))
                        result.add(row1);
                }
            }
            for (Hashtable<String, Object> row2 : rows2) {
                if (!rows1.contains(row2)) {
                    if (!result.contains(row2))
                        result.add(row2);
                }
            }
        }
        return result;
    }


    public Table getTableFromName(String strTableName) throws DBAppException {
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
