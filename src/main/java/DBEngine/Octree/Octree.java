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
        if (nodeToInsertIn == null)
            //TODO: if null then entry not found in octree
        nodeToInsertIn.addEntry(objarrEntry, strPageName);
    }

    public void delete(Object[] objarrEntry) {
        Node nodeToDeleteFrom = _nodeRoot.searchChildren(objarrEntry);
        if (nodeToDeleteFrom == null)
            //TODO: if null then entry not found in octree
        nodeToDeleteFrom.removeEntry(objarrEntry);
        if (nodeToDeleteFrom.isEmpty() && nodeToDeleteFrom != _nodeRoot) {
            if (nodeToDeleteFrom.get_nodeParent().childrenEmpty())
                nodeToDeleteFrom.get_nodeParent().setNodeAsLeaf();
        }
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
