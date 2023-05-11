package DBEngine.Octree;

import DBEngine.DBApp;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

public class Node {
    private Vector<OctreeEntry> _octreeEntryEntries;
    private Node[] _nodearrChildren;
    private Node _nodeParent;
    private int _intEntriesCount;
    private Object[] _objarrMinValues;
    private Object[] _objarrMaxValues;
    private Object[] _objarrMidValues;
    private String[] _strarrColTypes;

    public Node(Object[] objarrMinValues, Object[] objarrMaxValues, String[] strarrColTypes) {
        _objarrMinValues = objarrMinValues;
        _objarrMaxValues = objarrMinValues;
        _strarrColTypes = strarrColTypes;

        _octreeEntryEntries = new Vector<OctreeEntry>();
        _nodearrChildren = null; // Leaf node by default
        _intEntriesCount = 0;
        _objarrMidValues = getMidValues();
    }

    public void addEntry(Object[] objarrEntry, String strPageName, Object objEntryPk) {
        // if entry already exists then add duplicate
        for (OctreeEntry entry : _octreeEntryEntries) {
            if (entry.get_objarrEntryValues().equals(objarrEntry)) {
                entry.addDuplicate(strPageName, objEntryPk);
                return;
            }
        }
        // Creating and adding the new entry
        OctreeEntry newEntry = new OctreeEntry(objEntryPk, objarrEntry, strPageName);
        _octreeEntryEntries.add(newEntry);
        _intEntriesCount++;
        // if node is full then create children and distribute entries
        if (_intEntriesCount > DBApp.intMaxEntriesPerNode) {
            _nodearrChildren = new Node[8]; // Create 8 children
            for (int i = 0; i < 8; i++) {
                Object objMinValues[] = setChildMin(i);
                Object objMaxValues[] = setChildMax(i);
                _nodearrChildren[i] = new Node(objMinValues, objMaxValues, _strarrColTypes);
                _nodearrChildren[i].set_nodeParent(this);

                // distribute entries to children
                for (int j = 0; j < _intEntriesCount; j++) {
                    OctreeEntry entryToDistribute = _octreeEntryEntries.get(j);
                    if (_nodearrChildren[i].entryFits(entryToDistribute.get_objarrEntryValues())) {
                        _nodearrChildren[i].addEntry(entryToDistribute);
                        removeEntry(entryToDistribute.get_objarrEntryValues());
                        j--;
                    }
                }
            }
        }
    }

    // Overloaded method to add an OctreeEntry to children
    private void addEntry(OctreeEntry entry) {
        _octreeEntryEntries.add(entry);
        _intEntriesCount++;
    }

    public void removeRow(Object[] objarrEntry, Object objEntryPk) {
        for (OctreeEntry entry : _octreeEntryEntries) {
            if (entry.get_objarrEntryValues().equals(objarrEntry)) {
                entry.removeDuplicate(objEntryPk);
                if (entry.isEmpty()) {
                    _octreeEntryEntries.remove(entry);
                    _intEntriesCount--;
                }
                return;
            }
        }
    }

    public void removeEntry(Object[] objarrEntry) {
        for (OctreeEntry entry : _octreeEntryEntries) {
            if (entry.get_objarrEntryValues().equals(objarrEntry))
                _octreeEntryEntries.remove(entry);
        }
        _intEntriesCount--;
    }

    public OctreeEntry getEntry(Object[] objarrEntry){
        for (OctreeEntry entry : _octreeEntryEntries) {
            if (entry.get_objarrEntryValues().equals(objarrEntry))
                return entry;
        }
        return null;
    }

    public Node searchChildren(Object[] objarrEntry) {
        for (Node node : _nodearrChildren) {
            if (node.entryFits(objarrEntry)) {
                if (node.isLeaf())
                    return node;
                else
                    return node.searchChildren(objarrEntry);
            }
        }
        if (entryFits(objarrEntry))
            return this;

        return null; // entry not found
    }


    public boolean isEntryInNode(Object[] objarrEntry) {
        for (OctreeEntry entry : _octreeEntryEntries) {
            if (entry.get_objarrEntryValues().equals(objarrEntry))
                return true;
        }
        return false;
    }

    public boolean entryFits(Object[] objarrEntry) {
        for (int i = 0; i < _objarrMinValues.length; i++) {
            if (((Comparable) objarrEntry[i]).compareTo(_objarrMinValues[i]) < 0 || ((Comparable) objarrEntry[i]).compareTo(_objarrMaxValues[i]) >= 0) {
                return false;
            }
        }
        return true;
    }

    private Object[] setChildMin(int childNum) {
        Object[] objMinValues = new Object[_objarrMinValues.length];

        // set min x value
        if (childNum % 2 == 0)
            objMinValues[0] = _objarrMinValues[0];
        else
            objMinValues[0] = _objarrMidValues[0];

        // set min y value
        if (childNum % 4 <= 1)
            objMinValues[1] = _objarrMinValues[1];
        else
            objMinValues[1] = _objarrMidValues[1];

        // set min z value
        if (childNum < 4)
            objMinValues[2] = _objarrMinValues[2];
        else
            objMinValues[2] = _objarrMidValues[2];

        return objMinValues;
    }

    private Object[] setChildMax(int childNum) {
        Object[] objMaxValues = new Object[_objarrMaxValues.length];
        // set max x value
        if (childNum % 2 == 0)
            objMaxValues[0] = _objarrMidValues[0];
        else
            objMaxValues[0] = _objarrMaxValues[0];

        // set max y value
        if (childNum % 4 <= 1)
            objMaxValues[1] = _objarrMidValues[1];
        else
            objMaxValues[1] = _objarrMaxValues[1];

        // set max z value
        if (childNum < 4)
            objMaxValues[2] = _objarrMidValues[2];
        else
            objMaxValues[2] = _objarrMaxValues[2];

        return objMaxValues;
    }

    private Object[] getMidValues() {
        Object[] objMidValues = new Object[_objarrMaxValues.length];
        for (int i = 0; i < objMidValues.length; i++) {
            if (_strarrColTypes[i].equals("java.lang.String")) {
                objMidValues[i] = getMiddleString(_objarrMaxValues[i].toString(), _objarrMinValues[i].toString());
            } else if (_strarrColTypes[i].equals("java.lang.Integer")) {
                objMidValues[i] = ((Integer) _objarrMinValues[i] + (Integer) _objarrMaxValues[i]) / 2;
            } else if (_strarrColTypes[i].equals("java.lang.Double")) {
                objMidValues[i] = ((Double) _objarrMinValues[i] + (Double) _objarrMaxValues[i]) / 2;
            } else if (_strarrColTypes[i].equals("java.util.Date")) {
                Date midDate = new Date(((Date) _objarrMinValues[i]).getTime() +
                        ((Date) _objarrMaxValues[i]).getTime() / 2);
                objMidValues[i] = new SimpleDateFormat("yyyy-MM-dd").format(midDate);
            }
        }
        return objMidValues;
    }

    private String getMiddleString(String S, String T) {

        // Cast the strings to lowercase
        S = S.toLowerCase();
        T = T.toLowerCase();


        // Handle strings of different lengths
        if (S.length() > T.length()) {
            String extraLetters = S.substring(T.length());
            T = T + extraLetters;
        } else if (T.length() > S.length()) {
            String extraLetters = T.substring(S.length());
            S = S + extraLetters;
        }

        // Stores the length of the string
        int N = T.length();
        if (S.length() < T.length())
            N = S.length();

        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];

        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int) S.charAt(i) - 97
                    + (int) T.charAt(i) - 97;
        }

        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int) a1[i] / 26;
            a1[i] %= 26;
        }

        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i <= N; i++) {

            // If current value is odd,
            // carry 26 to the next index value
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }

            a1[i] = (int) a1[i] / 2;
        }

        String newString = "";
        for (int i = 1; i <= N; i++) {
            newString += (char) (a1[i] + 97);
            System.out.print((char) (a1[i] + 97));
        }

        return newString;
    }

    public boolean childrenEmpty() {
        for (int i = 0; i < _nodearrChildren.length; i++) {
            if (!_nodearrChildren[i].isEmpty())
                return false;
        }
        return true;
    }

    public void setNodeAsLeaf() {
        _nodearrChildren = null;
    }

    public boolean isLeaf() {
        return _nodearrChildren == null;
    }

    public boolean isRoot() {
        return _nodeParent == null;
    }

    public boolean isEmpty() {
        return _intEntriesCount == 0;
    }


    // Getters and setters

    public Node[] get_nodearrChildren() {
        return _nodearrChildren;
    }

    public void set_nodearrChildren(Node[] _nodearrChildren) {
        this._nodearrChildren = _nodearrChildren;
    }

    public Node get_nodeParent() {
        return _nodeParent;
    }

    public void set_nodeParent(Node _nodeParent) {
        this._nodeParent = _nodeParent;
    }

    public int get_intEntriesCount() {
        return _intEntriesCount;
    }

    public Object[] get_objarrMinValues() {
        return _objarrMinValues;
    }

    public Object[] get_objarrMaxValues() {
        return _objarrMaxValues;
    }

    public String[] get_strarrColTypes() {
        return _strarrColTypes;
    }

    public Object[] get_objarrMidValues() {
        return _objarrMidValues;
    }

    public Vector<OctreeEntry> get_octreeEntryEntries() {
        return _octreeEntryEntries;
    }

    public void set_octreeEntryEntries(Vector<OctreeEntry> _octreeEntryEntries) {
        this._octreeEntryEntries = _octreeEntryEntries;
    }
}
