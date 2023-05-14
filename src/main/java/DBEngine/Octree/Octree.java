package DBEngine.Octree;

import DBEngine.DBApp;
import DBEngine.SQLTerm;
import DBEngine.Table;
import Exceptions.DBAppException;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class Octree implements Serializable {

    private Node _nodeRoot;
    private Table _table;
    private Map<String, String> _htblColNameType;
    private String _strIndexName;
    private String _strPath;

    public Octree(Table table, Hashtable<String, String> htblColNameType, String strIndexName) throws DBAppException {
        _table = table;
        _htblColNameType = arrangeColNameType(htblColNameType); // arrange colnametype in table order
        _strIndexName = strIndexName;
        _nodeRoot = new Node(getMinValues(), getMaxValues(), getColTypes());


        _strPath = table.get_strPath() + "_" + _strIndexName + ".ser";
        saveOctree();
    }

    public Octree(Table table, String strIndexName) {
        _table = table;
        _strIndexName = strIndexName;

        _strPath = table.get_strPath() + "_" + _strIndexName + ".ser";
    }

    public void insertRow(Object[] objarrEntry, String strPageName, Object objEntryPk) throws DBAppException {
        Node nodeToInsertIn = _nodeRoot.searchChildren(objarrEntry);
        if (nodeToInsertIn == null) {
            System.out.println("Insert index: row values out of range");

            return;
        }
        nodeToInsertIn.addEntry(objarrEntry, strPageName, objEntryPk);
    }

    public void deleteEntry(Object[] objarrEntry) {
        Node nodeToDeleteFrom = _nodeRoot.searchChildren(objarrEntry);
        if (nodeToDeleteFrom == null) {
            System.out.println("Delete index: row values out of range");
            return;
        }
        if (!nodeToDeleteFrom.isEntryInNode(objarrEntry)) {
            System.out.println("should not happen , every row has index");
            return;
        }
        nodeToDeleteFrom.removeEntry(objarrEntry);
        if (nodeToDeleteFrom.isEmpty() && nodeToDeleteFrom != _nodeRoot) {
            if (nodeToDeleteFrom.get_nodeParent().childrenEmpty())
                nodeToDeleteFrom.get_nodeParent().setNodeAsLeaf();
        }
    }

    public void deleteRow(Object[] objarrEntry, Object objEntryPk) {
        Node nodeToDeleteFrom = _nodeRoot.searchChildren(objarrEntry);
        if (nodeToDeleteFrom == null) {
            System.out.println("Delete index: row values out of range");
            return;
        }
        if (!nodeToDeleteFrom.isEntryInNode(objarrEntry)) {
            System.out.println("Delete index: should not happen , every row has index");
            return;
        }
        nodeToDeleteFrom.removeRow(objarrEntry, objEntryPk);
        if (nodeToDeleteFrom.isEmpty() && nodeToDeleteFrom != _nodeRoot) {
            if (nodeToDeleteFrom.get_nodeParent().childrenEmpty())
                nodeToDeleteFrom.get_nodeParent().setNodeAsLeaf();
        }
    }


    public OctreeEntry searchEntry(Object[] objarrEntry) {
        Node nodeToSearchIn = _nodeRoot.searchChildren(objarrEntry);
        if (nodeToSearchIn == null) {
            System.out.println("Search index: row values out of range");
            return null;
        }
        if (!nodeToSearchIn.isEntryInNode(objarrEntry)) {
            System.out.println("Search index: should not happen , every row has index");
            return null;
        }
        return nodeToSearchIn.getEntry(objarrEntry);
    }

    public Vector<OctreeEntry> getRowsFromCondition(SQLTerm[] arrSQLTerm) {
        SQLTerm[] arrSQLTermArranged = arrangeTerms(arrSQLTerm);


        Object[] objarrValues = new Object[arrSQLTermArranged.length];
        String[] strarrOperators = new String[arrSQLTermArranged.length];

        for (int i = 0; i < arrSQLTermArranged.length; i++) {
            objarrValues[i] = arrSQLTermArranged[i]._objValue;
            strarrOperators[i] = arrSQLTermArranged[i]._strOperator;
        }

        Vector<OctreeEntry> entries = _nodeRoot.getRowsFromCondition(objarrValues, strarrOperators);
        return entries;
    }

    public Vector<OctreeEntry> getRowsFromCondition(Hashtable<String, Object> htblColNameValue) {
        System.out.println(htblColNameValue);
        System.out.println("size: " + htblColNameValue.size());
        Integer[] dimensions = arrangeDimensions(htblColNameValue);
        Object[] objarrValues = new Object[dimensions.length];


        Set<Entry<String, Object>> entrySet = htblColNameValue.entrySet();

        int j = 0;
        for (Entry<String, Object> entry : entrySet) {
            objarrValues[j] = entry.getValue();
            j++;
        }

        for (int i = 0; i < dimensions.length; i++) {
            System.out.print("Dimension(in octree) " + dimensions[i] + " ");
        }
        System.out.println();
        for (int i = 0; i < objarrValues.length; i++) {
            System.out.print("Value(in octree) " + objarrValues[i] + " ");
        }
        System.out.println();

        return _nodeRoot.getRowsFromCondition(objarrValues, dimensions);
    }

    private Integer[] arrangeDimensions(Hashtable<String, Object> htblColNameValue) {
        Integer[] dimensions = new Integer[htblColNameValue.size()];

        Set<Entry<String, Object>> entrySet = htblColNameValue.entrySet();
        Set<Entry<String, String>> entrySetColType = _htblColNameType.entrySet();

        int i = 0;
        for (Entry<String, Object> entry : entrySet) {
            String strColName = entry.getKey();
            int dimension = 0;
            for (Entry<String, String> entryColType : entrySetColType) {
                String strColNameFromTable = entryColType.getKey();
                if (strColNameFromTable.equals(strColName)) {
                    dimensions[i] = dimension;
                    break;
                }
                dimension++;
            }
            i++;
        }
        return dimensions;
    }

    private SQLTerm[] arrangeTerms(SQLTerm[] arrSQLTerm) {
        SQLTerm[] arrSQLTermArranged = new SQLTerm[arrSQLTerm.length];
        Set<Entry<String, String>> entrySet = _htblColNameType.entrySet();
        int currentColumn = 0;
        for (Entry<String, String> entry : entrySet) {
            String strColName = entry.getKey();
            for (int i = 0; i < arrSQLTerm.length; i++) {
                if (arrSQLTerm[i]._strColumnName.equals(strColName)) {
                    arrSQLTermArranged[currentColumn] = arrSQLTerm[i];
                    currentColumn++;
                    break;
                }
            }
        }
        return arrSQLTermArranged;
    }

    // multiple columns
    public boolean isIndexOn(String[] strarrColNames) {
        Set<Entry<String, String>> entrySet = _htblColNameType.entrySet();
        for (Entry<String, String> entry : entrySet) {
            String strColName = entry.getKey();
            boolean bFound = false;
            for (String strColNameToCheck : strarrColNames) {
                if (strColName.equals(strColNameToCheck)) {
                    bFound = true;
                    break;
                }
            }
            if (!bFound)
                return false;
        }
        return true;
    }

    // single column
    public boolean isIndexOn(String strColName) {
        Set<Entry<String, String>> entrySet = _htblColNameType.entrySet();
        for (Entry<String, String> entry : entrySet) {
            String strColNameInIndex = entry.getKey();
            if (strColName.equals(strColNameInIndex))
                return true;
        }
        return false;
    }

    public void updateEntryPage(Object[] objarrEntry, Object objEntryPk, String strNewPageName) {
        Node nodeToUpdateIn = _nodeRoot.searchChildren(objarrEntry);
        if (nodeToUpdateIn == null) {
            System.out.println("Update Page index: row values out of range");
            return;
        }
        if (!nodeToUpdateIn.isEntryInNode(objarrEntry)) {
            System.out.println("Update Page index: should not happen , every row has index");
            return;
        }
        nodeToUpdateIn.updateEntryPage(objarrEntry, objEntryPk, strNewPageName);
    }


    private Object[] getMinValues() throws DBAppException {
        Object[] objarrMinValues = new Object[3];
        Set<Entry<String, String>> entrySet = _htblColNameType.entrySet();
        int i = 0;
//        _table.loadTable();

        for (Entry<String, String> entry : entrySet) {
            String strColName = entry.getKey();
            String strColType = entry.getValue();
            // type cast each value to its type
            if (strColType.equals("java.lang.Integer"))
                objarrMinValues[i] = Integer.parseInt(_table.get_htblColNameMin().get(strColName));
            else if (strColType.equals("java.lang.Double"))
                objarrMinValues[i] = Double.parseDouble(_table.get_htblColNameMin().get(strColName));
            else if (strColType.equals("java.util.Date")) {
                try {
                    objarrMinValues[i] = new SimpleDateFormat(DBApp.dateFormat).parse(_table.get_htblColNameMin().get(strColName));
                } catch (ParseException e) {
                    throw new DBAppException("Error parsing date");
                }
            } else if (strColType.equals("java.lang.String"))
                objarrMinValues[i] = _table.get_htblColNameMin().get(strColName);

            i++;
        }
//        _table.unloadTable();
        return objarrMinValues;
    }

    private Object[] getMaxValues() throws DBAppException {
        Object[] objarrMaxValues = new Object[3];
        Set<Entry<String, String>> entrySet = _htblColNameType.entrySet();
        int i = 0;
//        _table.loadTable();
        for (Entry<String, String> entry : entrySet) {
            String strColName = entry.getKey();
            String strColType = entry.getValue();
            // type cast each value to its type
            if (strColType.equals("java.lang.Integer"))
                objarrMaxValues[i] = Integer.parseInt(_table.get_htblColNameMax().get(strColName));
            else if (strColType.equals("java.lang.Double"))
                objarrMaxValues[i] = Double.parseDouble(_table.get_htblColNameMax().get(strColName));
            else if (strColType.equals("java.util.Date")) {
                try {
                    objarrMaxValues[i] = new SimpleDateFormat(DBApp.dateFormat).parse(_table.get_htblColNameMax().get(strColName));
                } catch (ParseException e) {
                    throw new DBAppException("Error parsing date");
                }
            } else if (strColType.equals("java.lang.String"))
                objarrMaxValues[i] = _table.get_htblColNameMax().get(strColName);

            i++;

        }
//        _table.unloadTable();
        return objarrMaxValues;
    }

    private String[] getColTypes() {
        String[] strarrColTypes = new String[3];
        Set<Entry<String, String>> entrySet = _htblColNameType.entrySet();
        int i = 0;
        for (Entry<String, String> entry : entrySet) {
            strarrColTypes[i] = entry.getValue();
            i++;
        }
        return strarrColTypes;
    }

    private Map<String, String> arrangeColNameType(Hashtable<String, String> htblColNameTypeIndex) {
        Map<String, String> newHtblColNameType = new LinkedHashMap<String, String>();
        Set<Entry<String, String>> tableEntrySet = _table.get_htblColNameType().entrySet();
        Set<Entry<String, String>> indexEntrySet = htblColNameTypeIndex.entrySet();
        int newHtbCounter = 0;
        for (Entry<String, String> entryTable : tableEntrySet) {
            for (Entry<String, String> entryIndex : indexEntrySet) {
                if (entryTable.getKey().equals(entryIndex.getKey())) {
                    newHtblColNameType.put(entryTable.getKey(), entryTable.getValue());
                    newHtbCounter++;
                    break;
                }
            }
            if (newHtbCounter == 3) {
                break;
            }
        }
        return newHtblColNameType;
    }

    private void saveOctree() {
        File file = new File(_strPath);
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

    public void loadOctree() {
        File file = new File(_strPath);
        try {
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);
            Octree octree = (Octree) ois.readObject();
            this._nodeRoot = octree.get_nodeRoot();
            this._htblColNameType = octree.get_htblColNameType();
            ois.close();
            fis.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void unloadOctree() {
        saveOctree();
        _nodeRoot = null;
        _htblColNameType = null;
    }

    public String toString() {
        return "Octree{" +
                ", _table=" + _table.get_strTableName() +
                ", _htblColNameType=" + _htblColNameType +
                ", _strPath='" + _strPath + '\'' +
                ", _strIndexName='" + _strIndexName + '\'' +
                "_nodeRoot=" + _nodeRoot.toString() +
                '}';
    }


    // Getters and Setters
    public Node get_nodeRoot() {
        return _nodeRoot;
    }

    public Table get_table() {
        return _table;
    }

    public Map<String, String> get_htblColNameType() {
        return _htblColNameType;
    }

    public String get_strPath() {
        return _strPath;
    }

    public String get_strIndexName() {
        return _strIndexName;
    }
}
