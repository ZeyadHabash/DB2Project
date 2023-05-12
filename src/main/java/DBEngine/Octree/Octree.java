package DBEngine.Octree;

import DBEngine.Table;

import java.io.*;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Set;

public class Octree implements Serializable {

    private Node _nodeRoot;
    private Table _table;
    private Hashtable<String, String> _htblColNameType;
    private String _strIndexName;
    private String _strPath;

    public Octree(Table table, Hashtable<String, String> htblColNameType, String strIndexName) {
        _table = table;
        _htblColNameType = htblColNameType;
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

    public void insertRow(Object[] objarrEntry, String strPageName, Object objEntryPk) {
        Node nodeToInsertIn = _nodeRoot.searchChildren(objarrEntry);
        if (nodeToInsertIn == null) {
            System.out.println("Insert index: row values out of range");

            //TODO: if null then node not found in octree
            return;
        }
        nodeToInsertIn.addEntry(objarrEntry, strPageName, objEntryPk);
    }

    public void deleteEntry(Object[] objarrEntry) {
        Node nodeToDeleteFrom = _nodeRoot.searchChildren(objarrEntry);
        if (nodeToDeleteFrom == null) {
            //TODO: if null then node not found in octree
            System.out.println("Delete index: row values out of range");
            return;
        }
        if (!nodeToDeleteFrom.isEntryInNode(objarrEntry)) {
            //TODO: if false then entry not found in octree
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
            //TODO: if null then node not found in octree
            System.out.println("Delete index: row values out of range");
            return;
        }
        if (!nodeToDeleteFrom.isEntryInNode(objarrEntry)) {
            //TODO: if false then entry not found in octree
            System.out.println("should not happen , every row has index");
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


    private Object[] getMinValues() {
        Object[] objarrMinValues = new Object[3];
        Set<Entry<String, String>> entrySet = _htblColNameType.entrySet();
        int i = 0;
        _table.loadTable();
        for (Entry<String, String> entry : entrySet) {
            String strColName = entry.getKey();
            objarrMinValues[i] = _table.get_htblColNameMin().get(strColName);
            i++;
        }
        _table.unloadTable();
        return objarrMinValues;
    }

    private Object[] getMaxValues() {
        Object[] objarrMaxValues = new Object[3];
        Set<Entry<String, String>> entrySet = _htblColNameType.entrySet();
        int i = 0;
        _table.loadTable();
        for (Entry<String, String> entry : entrySet) {
            String strColName = entry.getKey();
            objarrMaxValues[i] = _table.get_htblColNameMax().get(strColName);
            i++;
        }
        _table.unloadTable();
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

    // TODO: add tostring method

    // Getters and Setters
    public Node get_nodeRoot() {
        return _nodeRoot;
    }

    public Table get_table() {
        return _table;
    }

    public Hashtable<String, String> get_htblColNameType() {
        return _htblColNameType;
    }

    public String get_strPath() {
        return _strPath;
    }

    public String get_strIndexName() {
        return _strIndexName;
    }
}
