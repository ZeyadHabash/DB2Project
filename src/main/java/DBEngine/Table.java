package DBEngine;

import Exceptions.DBAppException;

import java.io.*;
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
        saveTable();
    }


    public Table(String strTableName, String strPath) {
        _strTableName = strTableName;
        _strPath = strPath;
    }


    // TODO : sort rows after adding
    // TODO : function should take clustering key value as a parameter and insert the row in the correct place
    public void insertRow(Hashtable<String,Object> htblNewRow, int intRowIndex){
        if(_pages.size() == 0) { // if no pages exist yet, create one and add the row to it
            Page page = new Page(0, _strPath, _strTableName);
            page.addRow(htblNewRow);
            _pages.add(page);


            _intNumberOfRows++;
             saveTable();
              page=null;
        }
        else{



            Page page = _pages.get(intPageID); // get the page
            //extra page will be needed
            if(intPageID==_pages.size()) {
                page = new Page(intPageID, _strPath, _strTableName);
                page.addRow(htblNewRow);
                _pages.add(page);

            }
            else
            {// insert in existing pages
                int intRowID = intRowIndex % DBApp.intMaxRows; // get the row id in the page
                page.loadPage(); // load the page from the disk
                page.addRow(htblNewRow, intRowID); // add the row to the page
                if (page.get_intNumberOfRows() > DBApp.intMaxRows) // if the page is full, split it
                    splitPage(page, intPageID);

            }
            _intNumberOfRows++;
             saveTable();
              page=null;
        }
     //   _intNumberOfRows++;
      //  saveTable();
      //  page=null;

        System.out.println(_pages.get(0).get_rows().get(0));
       // unloadAllPages();
       // System.out.println(_pages.get(0).get_rows().get(0));
        //System.out.println(_pages.size());
        _pages.get(0).loadPage();
        System.out.println(_pages.get(0).get_rows().get(0));

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
        }else if(page.get_intNumberOfRows() < DBApp.intMaxRows && intPageID < _pages.size() - 1){ // if the page is not full, and it is not the last page, merge it with the next page
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
       // unloadAllPages();
        saveTable();
    }

    public void updateRow(int intRowIndex, Hashtable<String,Object> htblNewRow){
        int intPageID = intRowIndex / DBApp.intMaxRows;
        int intRowID = intRowIndex % DBApp.intMaxRows;
        Page page = _pages.get(intPageID);
        page.loadPage();
        page.updateRow(intRowID, htblNewRow);
        //unloadAllPages();
        saveTable();
    }


    public void splitPage(Page currPage, int intCurrPageID){
        int lastRowIndex = currPage.get_rows().size()-1;
        Hashtable<String,Object> lastRow = currPage.get_rows().get(lastRowIndex);

        // fix path name?
        if(intCurrPageID == _pages.size() - 1){
            Page newPage = new Page(_pages.size(), _strPath, _strTableName);
            newPage.addRow(lastRow);
            _pages.add(newPage);
        }else{
            int intNextPageID = intCurrPageID + 1;
            Page nextPage = _pages.get(intNextPageID);
            nextPage.loadPage();
            nextPage.addRow(lastRow, 0);
            if (nextPage.get_intNumberOfRows() > DBApp.intMaxRows)
                splitPage(nextPage, intNextPageID);
        }
        currPage.deleteRow(lastRowIndex);

       // unloadAllPages();
        saveTable();
    }

    // Returns the index of the row in the table
    public int getIndexFromRow(Hashtable<String,Object> htblColNameValue){
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

    public Hashtable<String,Object> getRowFromClusteringKey(Object objClusteringKeyValue){
        int intPageID = 0;
        int intRowID = 0;
        for(Page page : _pages){ // loop over all pages
            page.loadPage();
            for(Hashtable<String,Object> row : page.get_rows()){ // loop over all rows in a page
                if(row.get(_strClusteringKeyColumn).equals(objClusteringKeyValue)){ // if row is equal to given row
                    return row; // return the row
                }
                intRowID++; // if row does not match check the next row
            }
            intPageID++; // if row not found in current page check next page
        }
        return null; // if row not found return null
    }

    public Object getClusteringKeyFromRow(Hashtable<String,Object> htblColNameValue){
        return htblColNameValue.get(_strClusteringKeyColumn);
    }

    // Returns the row of a given index
    public Hashtable<String,Object> getRowFromIndex(int intRowIndex){
        int intPageID = intRowIndex / DBApp.intMaxRows;
        int intRowID = intRowIndex % DBApp.intMaxRows;
        Page page = _pages.get(intPageID);
        page.loadPage();
       // page.get_rows();
        return (page.get_rows()).get(intRowID);
    }
    public Object getClusteringKeyFromIndex(int intRowIndex){
        return  getRowFromIndex(intRowIndex).get(_strClusteringKeyColumn);
    }


    // should we have save table and load table methods?
    public void saveTable(){
        //path name???????
        File file = new File(_strPath + _strTableName + ".class");
       // File file = new File("data/Student.class");
        try {
            FileOutputStream fos = new FileOutputStream(file);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(this);
            oos.flush();
            oos.close();
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadTable(){
        File file = new File(_strPath + _strTableName + ".class");
       // File file = new File("data/Student.class");
        try {
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);
           // Object o= ois.readObject();
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
            System.out.println("PROBLEM IS HERE IN LOAD TABLE");
        }
    }

    public void unloadTable(){
        _strClusteringKeyColumn = null;
        _htblColNameType = null;
        _htblColNameMin = null;
        _htblColNameMax = null;
        _pages = null;
        _intNumberOfRows = 0;
    }

    /*public void unloadAllPages(){
        for(Page page : _pages){
        page.unloadPage();
        }
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
