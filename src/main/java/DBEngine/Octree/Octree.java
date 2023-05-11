package DBEngine.Octree;

import DBEngine.Table;

import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Set;

public class Octree {

    private Node _nodeRoot;
    private Table _table;
    private Hashtable<String, String> _htblColNameType;

    public Octree(Table table, Hashtable<String, String> htblColNameType) {
        _table = table;
        _htblColNameType = htblColNameType;
        _nodeRoot = new Node(getMinValues(), getMaxValues(), getColTypes());
    }

    public void insert(Object[] objarrEntry, String strPageName) {
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

    //TODO: search
    public OctreeEntry searchEntry(Object[] objarrEntry) {
        Node nodeToSearchIn = _nodeRoot.searchChildren(objarrEntry);
        if (nodeToSearchIn == null){
            System.out.println("Search index: row values out of range");
            return null;
        }
        if(!nodeToSearchIn.isEntryInNode(objarrEntry)){
            System.out.println("Search index: should not happen , every row has index");
            return null;
        }
        return nodeToSearchIn.getEntry(objarrEntry);
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

    public Node get_nodeRoot() {
        return _nodeRoot;
    }

    public Table get_table() {
        return _table;
    }

    public Hashtable<String, String> get_htblColNameType() {
        return _htblColNameType;
    }
}
