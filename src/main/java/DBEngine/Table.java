package DBEngine;

import Exceptions.DBAppException;

import java.io.*;
import java.util.Hashtable;
import java.util.Vector;


public class Table implements Serializable {
    private transient String _strTableName;
    private String _strClusteringKeyColumn;
    private transient String _strPath;
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

    public void insertRow(Hashtable<String, Object> htblNewRow, int intRowIndex) {
        if (_pages.size() == 0) { // if no pages exist yet, create one and add the row to it
            Page page = new Page(0, _strPath, _strTableName);
            page.addRow(htblNewRow);
            _pages.add(page);
        } else {
            int intPageID = intRowIndex / DBApp.intMaxRows; // get the page id which contains the row
            int intRowID = intRowIndex % DBApp.intMaxRows; // get the row id in the page
            if (intPageID >= _pages.size()) { // if the page doesn't exist yet, create it and add the row to it
                Page page = new Page(intPageID, _strPath, _strTableName);
                page.addRow(htblNewRow);
                _pages.add(page);
            } else { // if the page exists, add the row to it
                Page page = _pages.get(intPageID);
                page.loadPage();
                page.addRow(htblNewRow, intRowID);
                if (page.get_intNumberOfRows() > DBApp.intMaxRows) // if the page is full, split it
                    splitPage(page, intPageID);
            }
        }
        _intNumberOfRows++;
        unloadAllPages();
        saveTable();
    }


    public void deleteRow(int intRowIndex) {
        int intPageID = intRowIndex / DBApp.intMaxRows; // get the page id which contains the row
        int intRowID = intRowIndex % DBApp.intMaxRows; // get the row id in the page
        Page page = _pages.get(intPageID); // get the page
        page.loadPage(); // load the page from the disk
        page.deleteRow(intRowID); // delete the row from the page
        if (page.get_intNumberOfRows() == 0) { // delete the page if it is empty
            _pages.remove(intPageID);
            page.deletePage();
        } else if (page.get_intNumberOfRows() < DBApp.intMaxRows && intPageID < _pages.size() - 1) { // if the page is not full, and it is not the last page, merge it with the next page
            Page nextPage = _pages.get(intPageID + 1);
            nextPage.loadPage(); // load next page from disk
            page.addRow(nextPage.get_rows().get(0));
            nextPage.deleteRow(0);
            if (nextPage.get_intNumberOfRows() == 0) {
                _pages.remove(intPageID + 1);
                nextPage.deletePage();
            }
        }
        _intNumberOfRows--;
        unloadAllPages();
        saveTable();
    }

    public void updateRow(int intRowIndex, Hashtable<String, Object> htblNewRow) {
        int intPageID = intRowIndex / DBApp.intMaxRows;
        int intRowID = intRowIndex % DBApp.intMaxRows;
        Page page = _pages.get(intPageID);
        page.loadPage();
        page.updateRow(intRowID, htblNewRow);
        unloadAllPages();
        saveTable();
    }

    public void splitPage(Page currPage, int intCurrPageID) {
        int lastRowIndex = currPage.get_rows().size() - 1;
        Hashtable<String, Object> lastRow = currPage.get_rows().get(lastRowIndex);

        if (intCurrPageID == _pages.size() - 1) {
            Page newPage = new Page(_pages.size(), _strPath, _strTableName);
            newPage.addRow(lastRow);
            _pages.add(newPage);
        } else {
            int intNextPageID = intCurrPageID + 1;
            Page nextPage = _pages.get(intNextPageID);
            nextPage.loadPage();
            nextPage.addRow(lastRow, 0);
            if (nextPage.get_intNumberOfRows() > DBApp.intMaxRows) splitPage(nextPage, intNextPageID);
        }
        currPage.deleteRow(lastRowIndex);

        unloadAllPages();
        saveTable();
    }

    // Returns the index of the row in the table
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

    public Hashtable<String, Object> getRowFromClusteringKey(Object objClusteringKeyValue) {
        int intPageID = 0;
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
    }

    public Object getClusteringKeyFromRow(Hashtable<String, Object> htblColNameValue) {
        return htblColNameValue.get(_strClusteringKeyColumn);
    }

    // Returns the row of a given index
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

    public Object getClusteringKeyFromIndex(int intRowIndex) throws DBAppException {
        return getRowFromIndex(intRowIndex).get(_strClusteringKeyColumn);
    }


    // should we have save table and load table methods?
    public void saveTable() {
        deleteTableFile();
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
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void unloadTable() {
        _strClusteringKeyColumn = null;
        _htblColNameType = null;
        _htblColNameMin = null;
        _htblColNameMax = null;
        _pages = null;
        _intNumberOfRows = 0;
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
