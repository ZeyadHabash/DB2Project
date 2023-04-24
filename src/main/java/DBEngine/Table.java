package DBEngine;

import Exceptions.DBAppException;

import java.io.*;
import java.sql.SQLOutput;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    private String _strTableName;
    private String _strClusteringKeyColumn;
    private String _strPath;
    private Hashtable<String, String> _htblColNameType;
    private Hashtable<String, String> _htblColNameMin;
    private Hashtable<String, String> _htblColNameMax;
    private Vector<Page> _pages;
    private int _intNumberOfRows;

    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax, String strPath) {
        _strTableName = strTableName;
        _strClusteringKeyColumn = strClusteringKeyColumn;
        _htblColNameType = htblColNameType;
        _htblColNameMin = htblColNameMin;
        _htblColNameMax = htblColNameMax;
        _strPath = strPath;
        _pages = new Vector<Page>();
        _intNumberOfRows = 0;
        saveTable();
    }

    public Table(String strTableName, String strPath) {
        _strTableName = strTableName;
        _strPath = strPath;
    }

    public void insertRow(Hashtable<String, Object> htblNewRow) throws DBAppException {
        // TODO: have an external method to find the page to insert in
        // TODO add this following commented code to said method
//        if (_pages.size() == 0) { // if no pages exist yet, create one and add the row to it
//            Page page = new Page(0, _strPath, _strTableName);
//            page.addRow(htblNewRow);
//            _pages.add(page); }
//        if (intPageID >= _pages.size()) { // if the page doesn't exist yet, create it and add the row to it
//            Page page = new Page(intPageID, _strPath, _strTableName);
//            page.addRow(htblNewRow);
//            _pages.add(page); }
        //     if the page exists, add the row to it
        Object objClusteringKeyValue = htblNewRow.get(_strClusteringKeyColumn);
        Page page = getPageFromClusteringKey(objClusteringKeyValue);

        if (page == null) { // if page not found, create a new page
            page = new Page(_pages.size(), _strPath, _strTableName);
            page.addRow(htblNewRow);
            _pages.add(page);
        } else {
            int intRowID = page.binarySearchForInsertion(objClusteringKeyValue); // get the row id to insert the row in
            page.addRow(htblNewRow, intRowID); // add the row to the page
            if (page.get_intNumberOfRows() > DBApp.intMaxRows) // if the page is full, split it
                splitPage(page, page.get_intPageID());

            page.unloadPage();
        }
        _intNumberOfRows++;
        unloadAllPages();
    }


    public void deleteRow(Page page, int intRowID) {
        page.deleteRow(intRowID); // delete the row from the page
        if (page.get_intNumberOfRows() == 0) { // delete the page if it is empty
            _pages.remove(page.get_intPageID());
            page.deletePage();
        }
        _intNumberOfRows--;
        page.unloadPage();
    }

    public void updateRow(Hashtable<String, Object> htblNewRow) throws DBAppException {
        Object objClusteringKeyValue = htblNewRow.get(_strClusteringKeyColumn);
        Page page = getPageFromClusteringKey(objClusteringKeyValue);
        int intRowID = getRowIDFromClusteringKey(page, objClusteringKeyValue);
        if(page == null || intRowID == -1)
            throw new DBAppException("Row not found");
        page.updateRow(intRowID, htblNewRow);
        unloadAllPages();
    }

    public void splitPage(Page currPage, int intCurrPageID) { // splits page if it is full
        int lastRowIDinPage = currPage.get_rows().size() - 1; // get the id of the last row in the page
        Hashtable<String, Object> lastRow = currPage.get_rows().get(lastRowIDinPage); // get the last row in the page

        if (intCurrPageID == _pages.size() - 1) { // if the page is the last page in the table
            Page newPage = new Page(_pages.size(), _strPath, _strTableName); // create a new page
            newPage.addRow(lastRow);
            _pages.add(newPage);
        } else { // if the page is not the last page in the table
            int intNextPageID = intCurrPageID + 1; // get the id of the next page
            Page nextPage = _pages.get(intNextPageID); // get the next page
            nextPage.loadPage(); // load the next page
            nextPage.addRow(lastRow, 0); // add the last row to the next page as the first row
            if (nextPage.get_intNumberOfRows() > DBApp.intMaxRows) // if the next page is full, split it
                splitPage(nextPage, intNextPageID);
        }
        currPage.deleteRow(lastRowIDinPage); // delete the last row from the current page

        unloadAllPages();
    }

    // FIXME: this method is not working anymore due to changing the way deletion works
/*    // Returns the index of the row in the table
    public int getIndexFromRow(Hashtable<String, Object> htblColNameValue) {
        int intPageID = 0;
        int intRowID = 0;
        for (Page page : _pages) { // loop over all pages
            page.loadPage();
            for (Hashtable<String, Object> row : page.get_rows()) { // loop over all rows in a page
                if (row.equals(htblColNameValue)) { // if row is equal to given row
                    page.unloadPage(); // unload the page before returning
                    return intPageID * DBApp.intMaxRows + intRowID; // return the index
                }
                intRowID++; // if row does not match check the next row
            }
            page.unloadPage(); // unload the current page before moving on to the next one
            intPageID++; // if row not found in current page check next page
        }
        return -1; // if row not found return -1
    }
*/
    //so what does null signify? last page needs to be added' value index greater than all values

    public Page getPageFromClusteringKey(Object objClusteringKeyValue) {
        for (int i = 0; i < _pages.size(); i++) {
            Page page = _pages.get(i);
            page.loadPage();
            Object firstRowClusteringKey = page.get_rows().get(0).get(_strClusteringKeyColumn); // get the clustering key of the first row in the page
            Object lastRowClusteringKey = page.get_rows().get(page.get_rows().size() - 1).get(_strClusteringKeyColumn); // get the clustering key of the last row in the page
            if (((Comparable) firstRowClusteringKey).compareTo(objClusteringKeyValue) >= 0)  // if the clustering key of the first row is greater than or equal to the clustering key of the row to be inserted
                return page;
            if (((Comparable) lastRowClusteringKey).compareTo(objClusteringKeyValue) >= 0) // if the clustering key of the last row is greater than or equal to the clustering key of the row to be inserted
                return page; // return the page
            if (page.get_intNumberOfRows() < DBApp.intMaxRows) { // if the page is not full and in between the clustering keys of the first and last rows
                if (i == _pages.size() - 1)
                    return page;
                Page nextPage = _pages.get(i + 1); // get the next page
                nextPage.loadPage(); // load the next page
                Object nextPageFirstRowClusteringKey = nextPage.get_rows().get(0).get(_strClusteringKeyColumn); // get the clustering key of the first row in the next page
                nextPage.unloadPage();
                if (((Comparable) nextPageFirstRowClusteringKey).compareTo(objClusteringKeyValue) > 0) // if the clustering key of the first row in the next page is greater than the clustering key of the row to be inserted
                    return page;
            }
            page.unloadPage(); // unload the page before moving on to the next one
        }
        return null; // if the page is not found return null ( TODO: use this to create a new page when inserting)
    }

    public int getRowIDFromClusteringKey(Page page, Object objClusteringKeyValue) throws DBAppException {
        if (page == null)
            return -1; // if the page is not found return null ( TODO: use this to create a new page when inserting)
        int rowId = page.getRowID(objClusteringKeyValue, _strClusteringKeyColumn);
        return rowId;


        // FIXME: outdated due to changing the way rows are deleted
        /*int intPageID = 0;
        int intRowID = 0;
        for (Page page : _pages) { // loop over all pages
            page.loadPage();
            for (Hashtable<String, Object> row : page.get_rows()) { // loop over all rows in a page
                if (row.get(_strClusteringKeyColumn).equals(objClusteringKeyValue)) { // if row is equal to given row
                    page.unloadPage(); // unload the page before returning
                    return row; // return the row
                }
                intRowID++; // if row does not match check the next row
            }
            page.unloadPage(); // unload the current page before moving on to the next one
            intPageID++; // if row not found in current page check next page
        }
        return null; // if row not found return null
         */
    }

    public Object getClusteringKeyFromRow(Hashtable<String, Object> htblColNameValue) {
        return htblColNameValue.get(_strClusteringKeyColumn);
    }

    // FIXME: this method is not working anymore due to changing the way deletion works
    /*// Returns the row of a given index
    public Hashtable<String, Object> getRowFromIndex(int intRowIndex) throws DBAppException {
        int intPageID = intRowIndex / DBApp.intMaxRows;
        int intRowID = intRowIndex % DBApp.intMaxRows;
        if (intPageID >= _pages.size()) throw new DBAppException("row does not exist");
        Page page = _pages.get(intPageID);
        page.loadPage();
        Hashtable<String, Object> row = page.get_rows().get(intRowID);
        page.unloadPage();
        return row;
    }

     */

    // FIXME: this method is not working anymore due to changing the way deletion works
    /*
    public Object getClusteringKeyFromIndex(int intRowIndex) throws DBAppException {
        return getRowFromIndex(intRowIndex).get(_strClusteringKeyColumn);
    }

     */


    // should we have save table and load table methods?
    public void saveTable() {
        File file = new File(_strPath + _strTableName + ".ser");
        try {
            FileOutputStream fos = new FileOutputStream(file);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this);
            oos.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadTable() {
        File file = new File(_strPath + _strTableName + ".ser");
        try {
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);
            Table table = (Table) ois.readObject();
            ois.close();
            fis.close();
            _strClusteringKeyColumn = table.get_strClusteringKeyColumn();
            _htblColNameType = table.get_htblColNameType();
            _htblColNameMin = table.get_htblColNameMin();
            _htblColNameMax = table.get_htblColNameMax();
            _pages = table.get_pages();
            _intNumberOfRows = table.get_intNumberOfRows();
            System.out.println("Table loaded successfully");
            System.out.println("Number of rows in table: " + _intNumberOfRows);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void unloadTable() {
        saveTable();
        _strClusteringKeyColumn = null;
        _htblColNameType = null;
        _htblColNameMin = null;
        _htblColNameMax = null;
        _pages = null;
        _intNumberOfRows = 0;
        System.out.println("Table unloaded ");

    }

    private void deleteTableFile() {
        File file = new File(_strPath + _strTableName + ".ser");
        file.delete();
    }

    public void unloadAllPages() {
        for (Page page : _pages) {
            page.unloadPage();
        }
    }

    @Override
    public String toString() {
        String pages = "";
        for (Page page : _pages) {
            page.loadPage();
            pages += page + "\n";
        }
        unloadAllPages();
        return pages;

    }


    // getters and setters


    public String get_strTableName() {
        return _strTableName;
    }

    public void set_strTableName(String _strTableName) {
        this._strTableName = _strTableName;
    }

    public String get_strClusteringKeyColumn() {
        return _strClusteringKeyColumn;
    }

    public void set_strClusteringKeyColumn(String _strClusteringKeyColumn) {
        this._strClusteringKeyColumn = _strClusteringKeyColumn;
    }

    public Hashtable<String, String> get_htblColNameType() {
        return _htblColNameType;
    }

    public void set_htblColNameType(Hashtable<String, String> _htblColNameType) {
        this._htblColNameType = _htblColNameType;
    }

    public Hashtable<String, String> get_htblColNameMin() {
        return _htblColNameMin;
    }

    public void set_htblColNameMin(Hashtable<String, String> _htblColNameMin) {
        this._htblColNameMin = _htblColNameMin;
    }

    public Hashtable<String, String> get_htblColNameMax() {
        return _htblColNameMax;
    }

    public void set_htblColNameMax(Hashtable<String, String> _htblColNameMax) {
        this._htblColNameMax = _htblColNameMax;
    }

    public String get_strPath() {
        return _strPath;
    }

    public void set_strPath(String _strPath) {
        this._strPath = _strPath;
    }

    public Vector<Page> get_pages() {
        return _pages;
    }

    public int get_intNumberOfRows() {
        return _intNumberOfRows;
    }

}
