package DBEngine;

import com.sun.rowset.internal.Row;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    private String _strTableName;
    private String _strClusteringKeyColumn;
    private String _strPath;
    private Hashtable<String,String> _htblColNameType;
    private Hashtable<String,String> _htblColNameMin;
    private Hashtable<String,String> _htblColNameMax;
    private Vector<Page> _pages;
    private int _intNumberOfRows;


    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                 Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax, String strPath) {
        _strTableName = strTableName;
        _strClusteringKeyColumn = strClusteringKeyColumn;
        _htblColNameType = htblColNameType;
        _htblColNameMin = htblColNameMin;
        _htblColNameMax = htblColNameMax;
        _strPath = strPath;
        _pages = new Vector<Page>();
        _intNumberOfRows = 0;
        for(Page page : _pages){
            _intNumberOfRows += page.get_intNumberOfRows();
        }
    }

    // TODO : sort rows after adding
    // TODO : function should take clustering key value as a parameter and insert the row in the correct place
    public void insertRow(Hashtable<String,Object> htblNewRow){
        if(_pages.size() == 0){ // if no pages exist yet, create one and add the row to it
            Page page = new Page(0, _strPath, _strTableName);
            page.addRow(htblNewRow);
            _pages.add(page);
        }else{
            Page page = _pages.get(_pages.size() - 1);
            if(page.get_intNumberOfRows() == DBApp.intMaxRows){ // if the last page is full, create a new one
                Page newPage = new Page(_pages.size(), _strPath, _strTableName);
                newPage.addRow(htblNewRow);
                _pages.add(newPage);
            }else{ // if the last page is not full, add the row to it
                page.addRow(htblNewRow);
            }
        }
        _intNumberOfRows++;
    }


    //TODO : handle the case where the row is not found (?)
    //TODO : handle deleting from a middle page
    //TODO : sort rows after deleting
    public void deleteRow(int intRowIndex){
        int intPageID = (int) (intRowIndex / DBApp.intMaxRows); // get the page id which contains the row
        int intRowID = intRowIndex % DBApp.intMaxRows; // get the row id in the page
        Page page = _pages.get(intPageID); // get the page
        page.loadPage(); // load the page from the disk
        page.deleteRow(intRowID); // delete the row from the page
        if(page.get_intNumberOfRows() == 0) { // delete the page if it is empty
            _pages.remove(intPageID);
            page.deletePage();
        }else if(page.get_intNumberOfRows() < DBApp.intMaxRows && intPageID < _pages.size() - 1){ // if the page is not full and it is not the last page, merge it with the next page
            Page nextPage = _pages.get(intPageID + 1);
            nextPage.loadPage(); // load next page from disk
            page.addRow(nextPage.get_rows().get(0));
            nextPage.deleteRow(0);
            if(nextPage.get_intNumberOfRows() == 0){
                _pages.remove(intPageID + 1);
                nextPage.deletePage();
            }
        }
        _intNumberOfRows--;
    }

    //TODO : sort rows after updating
    public void updateRow(int intRowIndex, Hashtable<String,Object> htblNewRow){
        int intPageID = intRowIndex / DBApp.intMaxRows;
        int intRowID = intRowIndex % DBApp.intMaxRows;
        Page page = _pages.get(intPageID);
        page.loadPage();
        page.updateRow(intRowID, htblNewRow);
    }


    public int getRowIndex(Hashtable<String,Object> htblColNameValue){
        int intPageID = 0;
        int intRowID = 0;
        for(Page page : _pages){ // loop over all pages
            page.loadPage();
            for(Hashtable<String,Object> row : page.get_rows()){ // loop over all rows in a page
                if(row.equals(htblColNameValue)){ // if row is equal to given row
                    return intPageID * DBApp.intMaxRows + intRowID; // return the index
                }
                intRowID++; // if row does not match check the next row
            }
            intPageID++; // if row not found in current page check next page
        }
        return -1; // if row not found return -1
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
    public int get_intNumberOfRows() {return _intNumberOfRows;}

}
