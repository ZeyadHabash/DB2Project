package DBEngine;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

import DBEngine.Page;
import DBEngine.Index;

public class Table implements Serializable {
    private String _strTableName;
    private String _strClusteringKeyColumn;
    private String _strPath;
    private Hashtable<String,String> _htblColNameType;
    private Hashtable<String,String> _htblColNameMin;
    private Hashtable<String,String> _htblColNameMax;

   // private Vector<Page> _pages;
   // private Vector<Index> _indexes;
   // private int _intNumberOfRows;
    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax, strPath) {
        _strTableName = strTableName;
        _strClusteringKeyColumn = strClusteringKeyColumn;
        _htblColNameType = htblColNameType;
        _htblColNameMin = htblColNameMin;
        _htblColNameMax = htblColNameMax;
        _strPath = strPath;

        /*_pages = new Vector<Page>();
        _indexes = new Vector<Index>();
        _intNumberOfRows = 0;*/
    }

}
