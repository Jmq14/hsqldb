/*
 * For work developed by the HSQL Development Group:
 *
 * Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *
 *
 * For work originally developed by the Hypersonic SQL Group:
 *
 * Copyright (c) 1995-2000, The Hypersonic SQL Group.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 */


package org.hsqldb.index;

import java.util.Stack;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb.Constraint;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.OpTypes;
import org.hsqldb.Row;
import org.hsqldb.RowBPlus;
import org.hsqldb.SchemaObject;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.Tokens;
import org.hsqldb.TransactionManager;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.ObjectComparator;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.ReadWriteLockDummy;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rights.Grantee;
import org.hsqldb.types.Type;

// fredt@users 20020221 - patch 513005 by sqlbob@users - corrections
// fredt@users - patch 1.8.0 - reworked the interface and comparison methods
// fredt@users - patch 1.8.0 - improved reliability for cached indexes
// fredt@users - patch 1.9.0 - iterators and concurrency
// fredt@users - patch 2.0.0 - enhanced selection and iterators

/**
 * Implementation of an BPlus tree with parent pointers in nodes. Subclasses
 * of Node implement the tree node objects for memory or disk storage. An
 * Index has a root Node that is linked with other nodes using Java Object
 * references or file pointers, depending on Node implementation.<p>
 * An Index object also holds information on table columns (in the form of int
 * indexes) that are covered by it.<p>
 *
 * New class derived from Hypersonic SQL code and enhanced in HSQLDB. <p>
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since Hypersonic SQL
 */
public class IndexBPlus implements Index {

    private static final IndexRowIterator emptyIterator =
        new IndexRowIterator(null, (PersistentStore) null, null, null, 0,
                             false, false);

    // fields
    private final long       persistenceId;
    protected final HsqlName name;
    private final boolean[]  colCheck;
    final int[]              colIndex;
    private final int[]      defaultColMap;
    final Type[]             colTypes;
    private final boolean[]  colDesc;
    private final boolean[]  nullsLast;
    final boolean            isSimpleOrder;
    final boolean            isSimple;
    protected final boolean  isPK;        // PK with or without columns
    protected final boolean  isUnique;    // DDL uniqueness
    protected final boolean  isConstraint;
    private final boolean    isForward;
    private boolean          isClustered;
    protected TableBase      table;
    int                      position;
    private IndexUse[]       asArray;

//    protected NodeBPlus      root;       // B plus tree root node


    //
    Object[] nullData;

    //
    ReadWriteLock lock;
    Lock          readLock;
    Lock          writeLock;

    /**
     * Constructor declaration
     *
     * @param name HsqlName of the index
     * @param id persistnece id
     * @param table table of the index
     * @param columns array of column indexes
     * @param descending boolean[]
     * @param nullsLast boolean[]
     * @param colTypes array of column types
     * @param pk if index is for a primary key
     * @param unique is this a unique index
     * @param constraint does this index belonging to a constraint
     * @param forward is this an auto-index for an FK that refers to a table
     *   defined after this table
     */
    public IndexBPlus(HsqlName name, long id, TableBase table, int[] columns,
                    boolean[] descending, boolean[] nullsLast,
                    Type[] colTypes, boolean pk, boolean unique,
                    boolean constraint, boolean forward) {

        this.persistenceId = id;
        this.name          = name;
        this.colIndex      = columns;
        this.colTypes      = colTypes;
        this.colDesc       = descending == null ? new boolean[columns.length]
                                                : descending;
        this.nullsLast     = nullsLast == null ? new boolean[columns.length]
                                               : nullsLast;
        this.isPK          = pk;
        this.isUnique      = unique;
        this.isConstraint  = constraint;
        this.isForward     = forward;
        this.table         = table;
        this.colCheck      = table.getNewColumnCheckList();
        this.asArray = new IndexUse[]{ new IndexUse(this, colIndex.length) };

//        this.root          = null;

        ArrayUtil.intIndexesToBooleanArray(colIndex, colCheck);

        this.defaultColMap = new int[columns.length];

        ArrayUtil.fillSequence(defaultColMap);

        boolean simpleOrder = colIndex.length > 0;

        for (int i = 0; i < colDesc.length; i++) {
            if (this.colDesc[i] || this.nullsLast[i]) {
                simpleOrder = false;
            }
        }

        isSimpleOrder = simpleOrder;
        isSimple      = isSimpleOrder && colIndex.length == 1;
        nullData      = new Object[colIndex.length];

        //
        switch (table.getTableType()) {

            case TableBase.MEMORY_TABLE :
            case TableBase.CACHED_TABLE :
            case TableBase.TEXT_TABLE :
                lock = new ReentrantReadWriteLock();
                break;

            default :
                lock = new ReadWriteLockDummy();
                break;
        }

        readLock  = lock.readLock();
        writeLock = lock.writeLock();
    }

    // SchemaObject implementation
    public int getType() {
        return SchemaObject.INDEX;
    }

    public HsqlName getName() {
        return name;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject parentObject) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb = new StringBuffer(64);

        sb.append(Tokens.T_CREATE).append(' ');

        if (isUnique()) {
            sb.append(Tokens.T_UNIQUE).append(' ');
        }

        sb.append(Tokens.T_INDEX).append(' ');
        sb.append(getName().statementName);
        sb.append(' ').append(Tokens.T_ON).append(' ');
        sb.append(((Table) table).getName().getSchemaQualifiedStatementName());
        sb.append(((Table) table).getColumnListSQL(colIndex, colIndex.length));

        return sb.toString();
    }

    public long getChangeTimestamp() {
        return 0;
    }

    // IndexInterface
    public IndexUse[] asArray() {
        return asArray;
    }

    public RowIterator emptyIterator() {
        return emptyIterator;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public long getPersistenceId() {
        return persistenceId;
    }

    /**
     * Returns the count of visible columns used
     */
    public int getColumnCount() {
        return colIndex.length;
    }

    /**
     * Is this a UNIQUE index?
     */
    public boolean isUnique() {
        return isUnique;
    }

    /**
     * Does this index belong to a constraint?
     */
    public boolean isConstraint() {
        return isConstraint;
    }

    /**
     * Returns the array containing column indexes for index
     */
    public int[] getColumns() {
        return colIndex;
    }

    /**
     * Returns the array containing column indexes for index
     */
    public Type[] getColumnTypes() {
        return colTypes;
    }

    public boolean[] getColumnDesc() {
        return colDesc;
    }

    public int[] getDefaultColumnMap() {
        return this.defaultColMap;
    }

    /**
     * Returns a value indicating the order of different types of index in
     * the list of indexes for a table. The position of the groups of Indexes
     * in the list in ascending order is as follows:
     *
     * primary key index
     * unique constraint indexes
     * autogenerated foreign key indexes for FK's that reference this table or
     *  tables created before this table
     * user created indexes (CREATE INDEX)
     * autogenerated foreign key indexes for FK's that reference tables created
     *  after this table
     *
     * Among a group of indexes, the order is based on the order of creation
     * of the index.
     *
     * @return ordinal value
     */
    public int getIndexOrderValue() {

        if (isPK) {
            return 0;
        }

        if (isConstraint) {
            return isForward ? 4
                             : isUnique ? 0
                                        : 1;
        } else {
            return 2;
        }
    }

    public boolean isForward() {
        return isForward;
    }

    public void setTable(TableBase table) {
        this.table = table;
    }

    public void setClustered(boolean clustered) {
        isClustered = clustered;
    }

    public boolean isClustered() {
        return isClustered;
    }

    /**
     * Returns the node count.
     */
    public long size(Session session, PersistentStore store) {

        readLock.lock();

        try {
            return store.elementCount(session);
        } finally {
            readLock.unlock();
        }
    }

    public long sizeUnique(PersistentStore store) {

        readLock.lock();

        try {
            return store.elementCountUnique(this);
        } finally {
            readLock.unlock();
        }
    }

    public double[] searchCost(Session session, PersistentStore store) {

        boolean  probeDeeper = false;
        int      counter     = 1;
        double[] changes     = new double[colIndex.length];
        int      depth       = 0;
        int[]    depths      = new int[1];

        readLock.lock();

        try {
            NodeBPlus node = getAccessor(store);
            NodeBPlus temp = node;

            if (node == null) {
                return changes;
            }

            while (true) {
                node = temp;
                temp = node.getKeys()[0];

                if (temp == null) {
                    break;
                }

                if (depth == Index.probeDepth) {
                    probeDeeper = true;

                    break;
                }

                depth++;
            }

            while (true) {
//                temp  = next(store, node, depth, probeDepth, depths);
//                depth = depths[0];
//                ??? how to define the search cost in B Plus Tree???
                temp = next(store, node);

                if (temp == null) {
                    break;
                }

                compareRowForChange(session, node.getData(store),
                                    temp.getData(store), changes);

                node = temp;

                counter++;
            }

//            if (probeDeeper) {
//                double[] factors = new double[colIndex.length];
//                int extras = probeFactor(session, store, factors, true)
//                             + probeFactor(session, store, factors, false);
//
//                for (int i = 0; i < colIndex.length; i++) {
//                    factors[i] /= 2;
//
//                    for (int j = 0; j < factors[i]; j++) {
//                        changes[i] *= 2;
//                    }
//                }
//            }

            long rowCount = store.elementCount();

            for (int i = 0; i < colIndex.length; i++) {
                if (changes[i] == 0) {
                    changes[i] = 1;
                }

                changes[i] = rowCount / changes[i];

                if (changes[i] < 2) {
                    changes[i] = 2;
                }
            }

/*
            StringBuffer s = new StringBuffer();

            s.append("count " + rowCount + " columns " + colIndex.length
                     + " selectivity " + changes[0]);
            System.out.println(s);
*/
            return changes;
        } finally {
            readLock.unlock();
        }
    }

//    int probeFactor(Session session, PersistentStore store, double[] changes,
//                    boolean left) {
//
//        int     depth = 0;
//        NodeBPlus x     = getAccessor(store);
//        NodeBPlus n     = x;
//
//        if (x == null) {
//            return 0;
//        }
//
//        while (n != null) {
//            x = n;
//            n = left ? x.getLeft(store)
//                     : x.getRight(store);
//
//            depth++;
//
//            if (depth > probeDepth && n != null) {
//                compareRowForChange(session, x.getData(store),
//                                    n.getData(store), changes);
//            }
//        }
//
//        return depth - probeDepth;
//    }

    public long getNodeCount(Session session, PersistentStore store) {

        long count = 0;

        readLock.lock();

        try {
            RowIterator it = firstRow(session, store, 0);

            while (it.hasNext()) {
                it.getNextRow();

                count++;
            }

            return count;
        } finally {
            readLock.unlock();
        }
    }

    public boolean isEmpty(PersistentStore store) {

        readLock.lock();

        try {
            return getAccessor(store) == null;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Removes all links between memory nodes
     */
    public void unlinkNodes(NodeBPlus primaryRoot) {
        System.out.println("unlinkNodes");
//        writeLock.lock();
//
//        try {
//            NodeBPlus x = primaryRoot;
//            NodeBPlus l = x;
//
//            while (l != null) {
//                x = l;
//                l = x.getLeft(null);
//            }
//
//            while (x != null) {
//                NodeBPlus n = nextUnlink(x);
//
//                x = n;
//            }
//        } finally {
//            writeLock.unlock();
//        }
    }

    private NodeBPlus nextUnlink(NodeBPlus x) {

//        NodeBPlus temp = x.getRight(null);
//
//        if (temp != null) {
//            x    = temp;
//            temp = x.getLeft(null);
//
//            while (temp != null) {
//                x    = temp;
//                temp = x.getLeft(null);
//            }
//
//            return x;
//        }
//
//        temp = x;
//        x    = x.getParent(null);
//
//        while (x != null && x.isRight(temp)) {
//            x.nRight = null;
//
//            temp.getRow(null).destroy();
//            temp.delete();
//
//            //
//            temp = x;
//            x    = x.getParent(null);
//        }
//
//        if (x != null) {
//            x.nLeft = null;
//        }
//
//        temp.getRow(null).destroy();
//        temp.delete();
//
        return x;
    }

    public void checkIndex(PersistentStore store) {

        readLock.lock();

        try {
            NodeBPlus p = getAccessor(store);
            NodeBPlus f = null;

            while (p != null ) {
                f = p;

                checkNodes(store, p);

                p = p.getKeys()[0];
            }

            while (f != null) {
                checkNodes(store, f);

                f = next(store, f);
            }
        } finally {
            readLock.unlock();
        }
    }



    void checkNodes(PersistentStore store, NodeBPlus p) {

        if (p.isLeaf) {
            if (p.isData
                    || p.getParent(store)!=null
                    || p.getPointers().length>0)  {
                System.out.print("broken index - node type");
            }
        }

        else if (p.isData) {
            if (p.isLeaf|| p.getParent(store) == null
                    || p.getKeys().length > 0
                    || p.getPointers().length > 0) {
                System.out.print("broken index - node type");
            }

            NodeBPlus parent = p.getParent(store);
            boolean flag = false;
            for (int i=0; i<parent.getKeys().length; i++) {
                if (p.equals(parent.getKeys()[i])) {
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                System.out.print("broken index - no parent");
            }
        }

        else {
            if (p.getKeys().length + 1 != p.getPointers().length) {
                System.out.print("broken index - no match in key & pointer");
            }
        }
    }

    /**
     * Compares two table rows based on the columns of this index. The rowColMap
     * parameter specifies which columns of the other table are to be compared
     * with the colIndex columns of this index. The rowColMap can cover all or
     * only some columns of this index.
     *
     * @param session Session
     * @param a row from another table
     * @param rowColMap column indexes in the other table
     * @param b a full row in this table
     * @return comparison result, -1,0,+1
     */
    public int compareRowNonUnique(Session session, Object[] a, Object[] b,
                                   int[] rowColMap) {

        int fieldcount = rowColMap.length;

        for (int j = 0; j < fieldcount; j++) {
            int i = colTypes[j].compare(session, a[colIndex[j]],
                                        b[rowColMap[j]]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    public int compareRowNonUnique(Session session, Object[] a, Object[] b,
                                   int[] rowColMap, int fieldCount) {

        for (int j = 0; j < fieldCount; j++) {
            int i = colTypes[j].compare(session, a[colIndex[j]],
                                        b[rowColMap[j]]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    /**
     * As above but use the index column data
     */
    public int compareRowNonUnique(Session session, Object[] a, Object[] b,
                                   int fieldCount) {

        for (int j = 0; j < fieldCount; j++) {
            int i = colTypes[j].compare(session, a[colIndex[j]],
                                        b[colIndex[j]]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    public void compareRowForChange(Session session, Object[] a, Object[] b,
                                    double[] changes) {

        for (int j = 0; j < colIndex.length; j++) {
            int i = colTypes[j].compare(session, a[colIndex[j]],
                                        b[colIndex[j]]);

            if (i != 0) {
                for (; j < colIndex.length; j++) {
                    changes[j]++;
                }
            }
        }
    }

    public int compareRow(Session session, Object[] a, Object[] b) {

        for (int j = 0; j < colIndex.length; j++) {
            int i = colTypes[j].compare(session, a[colIndex[j]],
                                        b[colIndex[j]]);

            if (i != 0) {
                if (isSimpleOrder) {
                    return i;
                }

                boolean nulls = a[colIndex[j]] == null
                                || b[colIndex[j]] == null;

                if (colDesc[j] && !nulls) {
                    i = -i;
                }

                if (nullsLast[j] && nulls) {
                    i = -i;
                }

                return i;
            }
        }

        return 0;
    }

    /**
     * Compare two rows of the table for inserting rows into unique indexes
     * Supports descending columns.
     *
     * @param session Session
     * @param newRow data
     * @param existingRow data
     * @param useRowId boolean
     * @param start int
     * @return comparison result, -1,0,+1
     */
    int compareRowForInsertOrDelete(Session session, Row newRow,
                                    Row existingRow, boolean useRowId,
                                    int start) {

        Object[] a = newRow.getData();
        Object[] b = existingRow.getData();

        for (int j = start; j < colIndex.length; j++) {
            int i = colTypes[j].compare(session, a[colIndex[j]],
                                        b[colIndex[j]]);

            if (i != 0) {
                if (isSimpleOrder) {
                    return i;
                }

                boolean nulls = a[colIndex[j]] == null
                                || b[colIndex[j]] == null;

                if (colDesc[j] && !nulls) {
                    i = -i;
                }

                if (nullsLast[j] && nulls) {
                    i = -i;
                }

                return i;
            }
        }

        if (useRowId) {
            long diff = newRow.getPos() - existingRow.getPos();

            return diff == 0L ? 0
                              : diff > 0L ? 1
                                          : -1;
        }

        return 0;
    }

    int compareObject(Session session, Object[] a, Object[] b,
                      int[] rowColMap, int position, int opType) {
        return colTypes[position].compare(session, a[colIndex[position]],
                                          b[rowColMap[position]], opType);
    }

    boolean hasNulls(Session session, Object[] rowData) {

        if (colIndex.length == 1) {
            return rowData[colIndex[0]] == null;
        }

        boolean normal = session == null ? true
                                         : session.database.sqlUniqueNulls;

        for (int j = 0; j < colIndex.length; j++) {
            if (rowData[colIndex[j]] == null) {
                if (normal) {
                    return true;
                }
            } else {
                if (!normal) {
                    return false;
                }
            }
        }

        return !normal;
    }

    /**
     * Insert a node into the index
     */
    public void insert(Session session, PersistentStore store, Row row) {

        NodeBPlus        n;
        NodeBPlus        x;
        int              compare = -1;
        Stack<NodeBPlus> stack = new Stack<NodeBPlus>();

        writeLock.lock();

        try {
            n = getAccessor(store);
            x = ((RowBPlus) row).getNode(position);

            if (n == null) {         // empty tree

                n = newLeafNode(store, x);
                store.setAccessor(this, n);   // root

                return;
            }

            while (!n.isLeaf) {
                // searching...
                stack.push(n);

                Row currentRow = n.getKeys()[0].row;
                compare = searchCompare(currentRow, session, row);
                if (compare < 0) {
                    n = n.getPointers()[0];
                    continue;
                }

                currentRow = n.getKeys()[n.getKeys().length-1].row;
                compare = searchCompare(currentRow, session, row);
                if (compare >= 0) {
                    n = n.getPointers()[n.getPointers().length-1];
                    continue;
                }

                Row nextRow = n.getKeys()[0].row;
                for (int i=0; i < n.getKeys().length-1; i++) {
                    currentRow = nextRow;
                    nextRow = n.getKeys()[i+1].row;
                    if (searchCompare(currentRow, session, row) >= 0 &&
                            searchCompare(nextRow, session, row) < 0) {
                        n = n.getPointers()[i+1];
                    }
                }
            }


            if (n.getKeys().length < n.nodeSize) {  // if node has not been full
                n = insertNode(session, store, n, x, null);
            } else {                                // else: split the node
                // copying all current node contents in temp node then insert the new element on it
                NodeBPlus temp = new NodeBPlus();
                temp.setKeys(n.getKeys());
                temp.setPointers(n.getPointers());
                temp = insertNode(session, store, temp, x, null);

                NodeBPlus newNode = new NodeBPlus();
                int j = (n.getKeys().length + 1) / 2;

                //take the first half of the temp nde in current node
                n.setKeys(temp.getKeys(), 0, j);

                // copying the rest of temp node in new node
                newNode.setKeys(temp.getKeys(), j, temp.getKeys().length);
                for (int i=0; i<newNode.getKeys().length; i++){
                    newNode.getKeys()[i].setParent(store, newNode);
                }

                // set next/last leaf node
                if (n.getNextPage() != null) {
                    n.getNextPage().setLastPage(newNode);
                }
                newNode.setNextPage(n.getNextPage());
                newNode.setLastPage(n);
                n.setNextPage(newNode);

                // key that will be inserting into parent node
                NodeBPlus key = temp.getKeys()[j];

                while (true) {
                    if (stack.isEmpty()) {
                        // root case
                        NodeBPlus root = new NodeBPlus(false, false);
                        root.addKeys(key);
                        root.addPointers(n);
                        root.addPointers(newNode);

                        store.setAccessor(this, root);
                        break;
                    }
                    else {
                        // parent case
                        n = stack.pop();

                        if (n.getKeys().length < n.nodeSize) {
                            n = insertNode(session, store, n, key, newNode);
                            break;
                        }
                        else {
                            // splitting one internal nodes
                            // copying them into new node and insert new elements in temp node
                            // then dividing it into current node and new node
                            temp.setLeaf(false);
                            temp.setKeys(n.getKeys());
                            temp.setPointers(n.getPointers());
                            temp = insertNode(session, store, temp, key, newNode);

                            j = temp.getPointers().length / 2;

                            n.setKeys(temp.getKeys(), 0, j-1);
                            n.setPointers(temp.getPointers(), 0, j);

                            newNode.setKeys(temp.getKeys(), j, temp.getKeys().length);
                            newNode.setPointers(temp.getPointers(), j, temp.getPointers().length);

                            if (n.getNextPage() != null) {
                                n.getNextPage().setLastPage(newNode);
                            }
                            newNode.setNextPage(n.getNextPage());
                            newNode.setLastPage(n);
                            n.setNextPage(newNode);

                            key = temp.getKeys()[j-1];
                        }

                    }
                }



            }
        } finally {
            writeLock.unlock();
        }
    }

    private NodeBPlus insertNode(Session session, PersistentStore store,
                                 NodeBPlus node, NodeBPlus key, NodeBPlus pointer) {
//        RowComparator comparator = new RowComparator(session);
//        ArrayUtil.toAdjustedArray(leaf.keys, data, 0, 1);
//        ArraySort.sort(leaf.keys, 0, leaf.keys.length, comparator);

        int pos = 0;

        Row keyRow = key.row;
        Row currentRow = node.getKeys()[0].row;

        int compare = searchCompare(currentRow, session, keyRow);
        if (compare < 0) {
            pos = 0;
            return node.set(store, key, pointer, pos);
        }

        currentRow = node.getKeys()[node.getKeys().length-1].row;
        compare = searchCompare(currentRow, session, keyRow);
        if (compare >= 0) {
            pos = node.getKeys().length;
            return node.set(store, key, pointer, pos);
        }

        Row nextRow = node.getKeys()[0].row;
        for (int i=0; i < node.getKeys().length-1; i++) {
            currentRow = nextRow;
            nextRow = node.getKeys()[i+1].row;
            if (searchCompare(currentRow, session, keyRow) >= 0 &&
                    searchCompare(nextRow, session, keyRow) < 0) {
                pos = i+1;
                return node.set(store, key, pointer, pos);
            }
        }
        return node;
    }

    private NodeBPlus newLeafNode(PersistentStore store, NodeBPlus x) {

        NodeBPlus n = new NodeBPlus();
        n.addKeys(x);
        x.setParent(store, n);

        return n;
    }


    public void delete(Session session, PersistentStore store, Row row) {

        if (!row.isInMemory()) {
            row = (Row) store.get(row, false);
        }

        NodeBPlus node = ((RowBPlus) row).getNode(position);

        if (node != null) {
            delete(session, store, node);
        }
    }

    void delete(Session session, PersistentStore store, NodeBPlus x) {

        Stack<NodeBPlus> stack = new Stack<NodeBPlus>();
        NodeBPlus n;
        NodeBPlus root;
        Row       row = x.getRow(store);

        int compare = 0;

        if (x == null) {
            return;
        }

        writeLock.lock();
        store.writeLock();

        try {

            root = getAccessor(store);
            n = root;

            if (n == null) {         // empty tree
                return;
            }

            while (!n.isLeaf) {
                stack.push(n);

                Row currentRow = n.getKeys()[0].row;
                compare = searchCompare(currentRow, session, row);
                if (compare < 0) {
                    n = n.getPointers()[0];
                    continue;
                }

                currentRow = n.getKeys()[n.getKeys().length-1].row;
                compare = searchCompare(currentRow, session, row);
                if (compare >= 0) {
                    n = n.getPointers()[n.getPointers().length-1];
                    continue;
                }

                Row nextRow = n.getKeys()[0].row;
                for (int i=0; i < n.getKeys().length-1; i++) {
                    currentRow = nextRow;
                    nextRow = n.getKeys()[i+1].row;
                    if (searchCompare(currentRow, session, row) >= 0 &&
                            searchCompare(nextRow, session, row) < 0) {
                        n = n.getPointers()[i+1];
                    }
                }
            }

            // n is a leaf node

            boolean flag = false;

            for (int i = 0; i < n.getKeys().length; i++) {

                if (n == root && x == n.getKeys()[i]) {

                    n.removeKeys(i);
                    return;

                } else if (x == n.getKeys()[i]) {

                    flag = true;
                    break;

                }
            }

            if (flag) {

                if (n.getKeys().length-1 >= n.nodeSize / 2) {
                    n.removeKeys(x);

                    NodeBPlus parent = stack.peek();
                    for (int i=0; i<parent.getKeys().length; i++) {

                        if (searchCompare(parent.getKeys()[i].getRow(store),
                                session, x.getRow(store)) == 0) {

                            parent.replaceKeys(n.getKeys()[0], i);
                            break;

                        }
                    }
                }

                else {  // combining nodes

                    NodeBPlus parent = stack.peek();

                    // Determine whether the next node comes from
                    // the same parent. If not, borrow elements from it.
                    int deter = sameParent(n, parent);


                    if (deter == 1) {
                        // borrow from the next leaf node

                        n.removeKeys(x);

                        NodeBPlus key = n.getNextPage().removeKeys(0);
                        NodeBPlus pointer = n.getNextPage().removePointers(0);
                        n.addKeys(key);
                        n.addPointers(pointer);

                        for (int i = 0; i < parent.getKeys().length; i++) {

                            if (searchCompare(parent.getKeys()[i].getRow(store),
                                    session, key.getRow(store)) == 0) {

                                parent.replaceKeys(n.getNextPage().getKeys()[0], i);
                                break;
                            }
                        }

                        for (int i = 0; i < parent.getKeys().length; i++) {

                            if (searchCompare(parent.getKeys()[i].getRow(store),
                                    session, x.getRow(store)) == 0) {

                                parent.replaceKeys(n.getKeys()[0], i);
                                break;
                            }
                        }

                        return;

                    }

                    else if (deter == 2) {
                        // borrow from the previous node

                        n.removeKeys(x);

                        NodeBPlus key =
                                n.getLastPage().removeKeys(n.getLastPage().getKeys().length-1);
                        NodeBPlus pointer =
                                n.getLastPage().removePointers(n.getLastPage().getPointers().length-1);
                        n.addKeys(key, 0);
                        n.addPointers(pointer, 0);

                        for (int i = 0; i < parent.getKeys().length; i++) {

                            if (searchCompare(parent.getKeys()[i].getRow(store),
                                    session, key.getRow(store)) == 0) {

                                parent.replaceKeys(
                                        n.getLastPage().getKeys()[n.getLastPage().getKeys().length-1],
                                        i);
                                break;
                            }
                        }

                        for (int i = 0; i < parent.getKeys().length; i++) {

                            if (searchCompare(parent.getKeys()[i].getRow(store),
                                    session, x.getRow(store)) == 0) {

                                parent.replaceKeys(n.getKeys()[0], i);
                                break;
                            }
                        }

                        return;

                    }

                    else {
                        // merging to an internal node
                        boolean prevB = true;

                        if (x == n.getKeys()[0]) {
                            prevB = false;
                        }

                        n.removeKeys(x);

                        int tempKey = 0;
                        int tempPointer = 0;

                        // if the merging with the next node
                        // then copy all elements of current node to the next node
                        // move the first element from the next node to parent
                        if (canMeargeNext(n, parent)) {
                            NodeBPlus next = n.getNextPage();

                            if (n.getLastPage() != null) {
                                n.getLastPage().setNextPage(next);
                            }

                            if (next != null) {
                                next.setLastPage(n.getLastPage());
                            }
                            else {
                                return;
                            }

                            n.setLastPage(null);
                            n.setNextPage(null);

                            for (int i=n.getKeys().length; i>=0; i--){
                                next.addKeys(n.getKeys()[i], 0);
                            }

                            for (int i=n.getPointers().length; i>=0; i--){
                                next.addPointers(n.getPointers()[i], 0);
                            }

                            for (int i = 0; i < parent.getKeys().length; i++) {

                                if (searchCompare(parent.getKeys()[i].getRow(store), session,
                                        next.getKeys()[n.getKeys().length].getRow(store)) == 0) {

                                    tempKey = i;
                                    tempPointer = i;

                                    break;
                                }
                            }
                            if (tempKey > 0 && parent.getKeys()[tempKey - 1] == x) {
                                parent.replaceKeys(next.getKeys()[0], tempKey-1);
                            }

                        }

                        // merging with the last node
                        else {

                            NodeBPlus prev = n.getLastPage();
                            if (prev != null) {
                                prev.setNextPage(n.getNextPage());
                            }
                            else {
                                return;
                            }

                            if (n.getNextPage() != null) {
                                n.getNextPage().setLastPage(prev);
                            }

                            n.setNextPage(null);
                            n.setLastPage(null);

                            for (int i=0; i<n.getKeys().length; i++){
                                prev.addKeys(n.getKeys()[i]);
                            }

                            for (int i=0; i<n.getPointers().length; i++) {
                                prev.addPointers(n.getPointers()[i]);
                            }

                            if (prevB) {
                                for (int i = 0; i < parent.getKeys().length; i++) {

                                    if (searchCompare(parent.getKeys()[i].getRow(store), session,
                                            n.getKeys()[0].getRow(store)) == 0) {

                                        tempKey = i;
                                        tempPointer = i + 1;
                                        break;
                                    }
                                }
                            } else {
                                for (int i = 0; i < parent.getKeys().length; i++) {
                                    if (searchCompare(parent.getKeys()[i].getRow(store), session,
                                            x.getRow(store)) == 0) {

                                        tempKey = i;
                                        tempPointer = i + 1;
                                        break;
                                    }
                                }
                            }

                            boolean finished = false;
                            do {
                                // if we get root
                                if (stack.isEmpty()) {

                                    root.removeKeys(tempKey);
                                    root.removePointers(tempPointer);
                                    finished = true;

                                }
                                else {

                                    n = stack.pop();

                                    //try borrowing from the sibling
                                    if (n.getKeys().length >= 2) {

                                        n.removeKeys(tempKey);
                                        n.removePointers(tempPointer);

                                        finished = true;

                                    } else {
                                        // if the root has single sibling
                                        // the tree height will decrease

                                        if (n == root) {

                                            n.removeKeys(tempKey);
                                            n.removePointers(tempPointer);

                                            if (n.getPointers().length == 1) {
                                                root = n.getPointers()[0];
                                                store.setAccessor(this, root);
                                            }

                                            finished = true;

                                        } else {

                                            n.removeKeys(tempKey);
                                            n.removePointers(tempPointer);

                                            parent = stack.peek();
                                            deter = sameParent2(n, parent);

                                            // borrowing from next internal node

                                            if (deter == 1) {

                                                int index = -1;

                                                for (int i = 0; i < parent.getPointers().length; i++) {
                                                    if (parent.getPointers()[i] == n.getNextPage()) {
                                                        index = i;
                                                        break;
                                                    }
                                                }

                                                n.addKeys(parent.removeKeys(index - 1));
                                                n.addPointers(n.getNextPage().removePointers(0));

                                                parent.addKeys(n.getNextPage().removeKeys(0), index-1);

                                                finished = true;
                                            }

                                            // boorwing form prev internal node
                                            else if (deter == 2) {

                                                int index = -1;

                                                for (int i = 0; i < parent.getPointers().length; i++) {
                                                    if (parent.getPointers()[i] == n) {

                                                        index = i;
                                                        break;
                                                    }
                                                }
                                                n.addKeys(parent.removeKeys(index - 1), 0);
                                                n.addPointers(n.getLastPage().removePointers(
                                                        n.getLastPage().getPointers().length - 1),
                                                        0);

                                                parent.addKeys(n.getLastPage().removeKeys(
                                                        n.getLastPage().getKeys().length - 1),
                                                        index-1);

                                                finished = true;
                                            } else {

                                                // merging two internal nodes
                                                if (canMeargeNext(parent, n)) {

                                                    for (int i = 0; i < parent.getPointers().length; i++) {

                                                        if (n == parent.getPointers()[i]) {
                                                            tempKey = i;
                                                            tempPointer = i;
                                                            break;
                                                        }
                                                    }

                                                    NodeBPlus next = n.getNextPage();

                                                    if (n.getLastPage() != null) {
                                                        n.getLastPage().setNextPage(next);
                                                    }
                                                    if (next != null) {
                                                        next.setLastPage(n.getLastPage());
                                                    }
                                                    else {
                                                        return;
                                                    }

                                                    next.addKeys(parent.getKeys()[tempKey], 0);

                                                    for (int i=n.getKeys().length; i>=0; i--){
                                                        next.addKeys(n.getKeys()[i], 0);
                                                    }

                                                    for (int i=n.getPointers().length; i>=0; i--){
                                                        next.addPointers(n.getPointers()[i], 0);
                                                    }



                                                } else {
                                                    for (int i = 0; i < parent.getPointers().length; i++) {

                                                        if (n == parent.getPointers()[i]) {

                                                            tempKey = i - 1;
                                                            tempPointer = i;
                                                            break;
                                                        }
                                                    }

                                                    prev = n.getLastPage();
                                                    if (prev != null) {
                                                        prev.setNextPage(n.getNextPage());
                                                    }

                                                    if (n.getNextPage() != null) {
                                                        n.getNextPage().setLastPage(prev);
                                                    }

                                                    prev.addKeys(parent.getKeys()[tempKey]);

                                                    for (int i=0; i<n.getKeys().length; i++){
                                                        prev.addKeys(n.getKeys()[i]);
                                                    }

                                                    for (int i=0; i<n.getPointers().length; i++) {
                                                        prev.addPointers(n.getPointers()[i]);
                                                    }

                                                }
                                            }
                                        }
                                    }
                                }
                            } while (!finished);
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } finally {
            store.writeUnlock();
            writeLock.unlock();
        }
    }

    private int sameParent(NodeBPlus n, NodeBPlus parent) {

        boolean _next = canMeargeNext(n, parent);
        boolean _prev = canMergePrev(n, parent);

        NodeBPlus next = n.getNextPage();
        NodeBPlus prev = n.getLastPage();

        if (_next && next.getKeys().length - 1 >= n.nodeSize / 2.0) {
            return 1;
        } else if (_prev && prev.getKeys().length - 1 >= n.nodeSize / 2.0) {
            return 2;
        } else {
            return 0;
        }
    }

    private int sameParent2(NodeBPlus n, NodeBPlus parent) {

        boolean _next = canMeargeNext(n, parent);
        boolean _prev = canMergePrev(n, parent);

        NodeBPlus next = n.getNextPage();
        NodeBPlus prev = n.getLastPage();

        if (next != null && _next && next.getKeys().length - 1 >= 1) {
            return 1;
        } else if (prev != null && _prev && prev.getKeys().length- 1 >= 1) {
            return 2;
        } else {
            return 0;
        }
    }

    private boolean canMeargeNext(NodeBPlus n, NodeBPlus parent) {

        boolean _next  = false;
        NodeBPlus next = n.getNextPage();
        int i;

        if (next != null) {
            for (i = 0; i < parent.getPointers().length; i++) {
                if (parent.getPointers()[i] == next) {
                    _next = true;
                    break;
                }
            }
        }

        return _next;
    }

    private boolean canMergePrev(NodeBPlus n, NodeBPlus parent) {

        boolean _prev  = false;
        NodeBPlus prev = n.getLastPage();
        int i;

        if (prev != null) {
            for (i = 0; i < parent.getPointers().length; i++) {
                if (parent.getPointers()[i] == prev) {
                    _prev = true;
                    break;
                }
            }
        }

        return _prev;
    }

    public boolean existsParent(Session session, PersistentStore store,
                                Object[] rowdata, int[] rowColMap) {

        NodeBPlus node = findNode(session, store, rowdata, rowColMap,
                                rowColMap.length, OpTypes.EQUAL,
                                TransactionManager.ACTION_REF, false);

        return node != null;
    }

    /**
     * Return the first node equal to the indexdata object. The rowdata has the
     * same column mapping as this index.
     *
     * @param session session object
     * @param store store object
     * @param rowdata array containing index column data
     * @param matchCount count of columns to match
     * @param compareType int
     * @param reversed boolean
     * @param map boolean[]
     * @return iterator
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata, int matchCount,
                                    int distinctCount, int compareType,
                                    boolean reversed, boolean[] map) {

        if (compareType == OpTypes.MAX) {
            return lastRow(session, store, 0);
        }

        NodeBPlus node = findNode(session, store, rowdata, defaultColMap,
                                matchCount, compareType,
                                TransactionManager.ACTION_READ, reversed);

        if (node == null) {
            return emptyIterator;
        }

        return new IndexRowIterator(session, store, this, node, distinctCount,
                                    false, reversed);
    }

    /**
     * Return the first node equal to the rowdata object.
     * The rowdata has the same column mapping as this table.
     *
     * @param session session object
     * @param store store object
     * @param rowdata array containing table row data
     * @return iterator
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata) {

        NodeBPlus node = findNode(session, store, rowdata, colIndex,
                                colIndex.length, OpTypes.EQUAL,
                                TransactionManager.ACTION_READ, false);

        if (node == null) {
            return emptyIterator;
        }

        return new IndexRowIterator(session, store, this, node, 0, false,
                                    false);
    }

    /**
     * Return the first node equal to the rowdata object. The rowdata has the
     * column mapping provided in rowColMap.
     *
     * @param session session object
     * @param store store object
     * @param rowdata array containing table row data
     * @param rowColMap int[]
     * @return iterator
     */
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata, int[] rowColMap) {

        NodeBPlus node = findNode(session, store, rowdata, rowColMap,
                                rowColMap.length, OpTypes.EQUAL,
                                TransactionManager.ACTION_READ, false);

        if (node == null) {
            return emptyIterator;
        }

        return new IndexRowIterator(session, store, this, node, 0, false,
                                    false);
    }

    /**
     * Finds the first node where the data is not null.
     *
     * @return iterator
     */
    public RowIterator findFirstRowNotNull(Session session,
                                           PersistentStore store) {

        NodeBPlus node = findNode(session, store, nullData, this.defaultColMap,
                                1, OpTypes.NOT,
                                TransactionManager.ACTION_READ, false);

        if (node == null) {
            return emptyIterator;
        }

        return new IndexRowIterator(session, store, this, node, 0, false,
                                    false);
    }

    /**
     * Returns the row for the first node of the index
     *
     * @return Iterator for first row
     */
    public RowIterator firstRow(Session session, PersistentStore store,
                                int distinctCount) {

        readLock.lock();

        try {
            NodeBPlus x = getAccessor(store);
            NodeBPlus l = x;

            while (l != null && !l.isLeaf) {
                x = l;
                l = x.getPointers()[0];
            }

            if (l == null) {
                return emptyIterator;
            }

            x = l.getKeys()[0];

            while (session != null && x != null) {
                Row row = x.getRow(store);

                if (session.database.txManager.canRead(
                        session, store, row, TransactionManager.ACTION_READ,
                        null)) {
                    break;
                }

                x = next(store, x);
            }

            if (x == null) {
                return emptyIterator;
            }

            return new IndexRowIterator(session, store, this, x,
                                        distinctCount, false, false);
        } finally {
            readLock.unlock();
        }
    }

    public RowIterator firstRow(PersistentStore store) {

        readLock.lock();

        try {
            NodeBPlus x = getAccessor(store);
            NodeBPlus l = x;

            while (l != null && !l.isLeaf) {
                x = l;
                l = x.getPointers()[0];
            }

            if (l == null) {
                return emptyIterator;
            }

            x = l.getKeys()[0];

            if (x == null) {
                return emptyIterator;
            }

            return new IndexRowIterator(null, store, this, x, 0, false, false);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the row for the last node of the index
     *
     * @return last row
     */
    public RowIterator lastRow(Session session, PersistentStore store,
                               int distinctCount) {

        readLock.lock();

        try {
            NodeBPlus x = getAccessor(store);
            NodeBPlus l = x;

            while (!l.isLeaf) {
                x = l;
                l = x.getPointers()[x.getPointers().length-1];
            }

            if (l == null) {
                return emptyIterator;
            }

            x = l.getKeys()[l.getKeys().length-1];

            while (session != null && x != null) {
                Row row = x.getRow(store);

                if (session.database.txManager.canRead(
                        session, store, row, TransactionManager.ACTION_READ,
                        null)) {
                    break;
                }

                x = last(store, x);
            }

            if (x == null) {
                return emptyIterator;
            }

            return new IndexRowIterator(session, store, this, x,
                                        distinctCount, false, true);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the node after the given one
     */
    NodeBPlus next(Session session, PersistentStore store, NodeBPlus x,
                 int distinctCount) {

        if (x == null) {
            return null;
        }

        if (distinctCount != 0) {
            return findDistinctNode(session, store, x, distinctCount, false);
        }

        while (true) {
            x = next(store, x);

            if (x == null) {
                return x;
            }

            if (session == null) {
                return x;
            }

            Row row = x.getRow(store);

            if (session.database.txManager.canRead(
                    session, store, row, TransactionManager.ACTION_READ,
                    null)) {
                return x;
            }
        }
    }

    NodeBPlus last(Session session, PersistentStore store, NodeBPlus x,
                 int distinctCount) {

        if (x == null) {
            return null;
        }

        if (distinctCount != 0) {
            return findDistinctNode(session, store, x, distinctCount, true);
        }

        while (true) {
            x = last(store, x);

            if (x == null) {
                return x;
            }

            if (session == null) {
                return x;
            }

            Row row = x.getRow(store);

            if (session.database.txManager.canRead(
                    session, store, row, TransactionManager.ACTION_READ,
                    null)) {
                return x;
            }
        }
    }

    NodeBPlus next(PersistentStore store, NodeBPlus x) {

        if (x == null) {
            return null;
        }

        RowBPlus row = x.getRow(store);

        x = row.getNode(position);

        NodeBPlus temp = x.getParent(store);

        if (temp != null) {
            int i = ArrayUtil.find(temp.getKeys(), x);
            if (i < 0) {
                System.out.println("wrong parent!");
                return x;
                // error
            }

            if (i < temp.getKeys().length - 1) {
                return temp.getKeys()[i + 1];
            }

            temp = temp.getNextPage();
            if (temp != null) {
                return temp.getKeys()[0];
            } else {
                return null;
            }
        }

        return x;
    }

    NodeBPlus last(PersistentStore store, NodeBPlus x) {

        if (x == null) {
            return null;
        }

        RowBPlus row = x.getRow(store);

        x = row.getNode(position);

        NodeBPlus temp = x.getParent(store);

        if (temp != null) {
            int i = ArrayUtil.find(temp.getKeys(), x);
            if (i < 0) {
                System.out.println("wrong parent!");
                return x;
                // error
            }

            if (i > 0) {
                return temp.getKeys()[i - 1];
            }

            temp = temp.getLastPage();
            if (temp != null) {
                return temp.getKeys()[temp.getKeys().length-1];
            } else {
                return null;
            }
        }
        return x;
    }

    boolean isEqualReadable(Session session, PersistentStore store,
                            NodeBPlus node) {

        NodeBPlus  c = node;
        Object[] data;
        Object[] nodeData;
        Row      row;

        row = node.getRow(store);

        session.database.txManager.setTransactionInfo(store, row);

        if (session.database.txManager.canRead(session, store, row,
                                               TransactionManager.ACTION_DUP,
                                               null)) {
            return true;
        }

        data = node.getData(store);

        while (true) {
            c = last(store, c);

            if (c == null) {
                break;
            }

            nodeData = c.getData(store);

            if (compareRow(session, data, nodeData) == 0) {
                row = c.getRow(store);

                session.database.txManager.setTransactionInfo(store, row);

                if (session.database.txManager.canRead(
                        session, store, row, TransactionManager.ACTION_DUP,
                        null)) {
                    return true;
                }

                continue;
            }

            break;
        }

        while (true) {
            c = next(session, store, node, 0);

            if (c == null) {
                break;
            }

            nodeData = c.getData(store);

            if (compareRow(session, data, nodeData) == 0) {
                row = c.getRow(store);

                session.database.txManager.setTransactionInfo(store, row);

                if (session.database.txManager.canRead(
                        session, store, row, TransactionManager.ACTION_DUP,
                        null)) {
                    return true;
                }

                continue;
            }

            break;
        }

        return false;
    }

    /**
     * Finds a match with a row from a different table
     *
     * @param session Session
     * @param store PersistentStore
     * @param rowdata array containing data for the index columns
     * @param rowColMap map of the data to columns
     * @param fieldCount int
     * @param compareType int
     * @param readMode int
     * @param reversed
     * @return matching node or null
     */
    NodeBPlus findNode(Session session, PersistentStore store, Object[] rowdata,
                     int[] rowColMap, int fieldCount, int compareType,
                     int readMode, boolean reversed) {

        readLock.lock();

        try {
            NodeBPlus x          = getAccessor(store);
            NodeBPlus n          = null;
            NodeBPlus key        = null;
            NodeBPlus result     = null;
            Row     currentRow = null;
            Row     nextRow    = null;
            int     currentCompare = 0;
            int     nextCompare    = 0;

            if (compareType != OpTypes.EQUAL
                    && compareType != OpTypes.IS_NULL) {
                fieldCount--;

                if (compareType == OpTypes.SMALLER
                        || compareType == OpTypes.SMALLER_EQUAL) {
                    reversed = true;
                }
            }

            while (x != null && !x.isLeaf) {
                currentCompare = 0;
                nextCompare    = 0;

                nextRow = x.getKeys()[0].getRow(store);

                if (fieldCount > 0) {
                    nextCompare = compareRowNonUnique(session, nextRow.getData(),
                            rowdata, rowColMap, fieldCount);
                }

                for (int j=0; j<x.getKeys().length; j++) {

                    key = x.getKeys()[j];
                    currentRow = nextRow;
                    currentCompare = nextCompare;

                    if (j == x.getKeys().length-1) {

                        nextRow = null;

                    }
                    else {

                        nextRow = x.getKeys()[j + 1].getRow(store);
                        if (fieldCount > 0) {
                            nextCompare = compareRowNonUnique(session, nextRow.getData(),
                                    rowdata, rowColMap, fieldCount);
                        }
                    }

                    if (currentCompare == 0 ) {
                        switch (compareType) {

                            case OpTypes.IS_NULL :
                            case OpTypes.EQUAL : {
                                result = key;

                                if (reversed) {
                                    n = x.getPointers()[j+1];
                                } else {
                                    n = x.getPointers()[j];
                                }

                                break;
                            }
                            case OpTypes.NOT :
                            case OpTypes.GREATER : {
                                currentCompare = compareObject(session, currentRow.getData(),
                                        rowdata, rowColMap, fieldCount,
                                        compareType);

                                if (currentCompare <= 0) {
                                    n = x.getPointers()[j+1];
                                } else {
                                    result = key;
                                    n = x.getPointers()[j];
                                }

                                break;
                            }
                            case OpTypes.GREATER_EQUAL_PRE :
                            case OpTypes.GREATER_EQUAL : {
                                currentCompare = compareObject(session, currentRow.getData(),
                                        rowdata, rowColMap, fieldCount,
                                        compareType);

                                if (currentCompare < 0) {
                                    n = x.getPointers()[j+1];
                                } else {
                                    result = key;
                                    n = x.getPointers()[j];
                                }

                                break;
                            }
                            case OpTypes.SMALLER : {
                                currentCompare = compareObject(session, currentRow.getData(),
                                        rowdata, rowColMap, fieldCount,
                                        compareType);

                                if (currentCompare < 0) {
                                    result = key;
                                    n = x.getPointers()[j+1];
                                } else {
                                    n = x.getPointers()[j];
                                }

                                break;
                            }
                            case OpTypes.SMALLER_EQUAL : {
                                currentCompare = compareObject(session, currentRow.getData(),
                                        rowdata, rowColMap, fieldCount,
                                        compareType);

                                if (compareType <= 0) {
                                    result = key;
                                    n = x.getPointers()[j+1];
                                } else {
                                    n = x.getPointers()[j];
                                }

                                break;
                            }
                            default :
                                Error.runtimeError(ErrorCode.U_S0500, "Index");
                        }

                        break;

                    } else if (currentCompare < 0 ) {
                        if (nextRow!=null && nextCompare > 0) {
                            n = x.getPointers()[j+1];
                            break;

                        } else if (nextRow == null) {
                            n = x.getPointers()[j+1];
                            break;

                        }
                    }
                    else if (currentCompare > 0) {
                        n = x.getPointers()[j];
                        break;
                    }


                }

                if (n == null) {
                    break;
                }

                x = n;
            }


            // in leaf node
            for (int j=0; j<x.getKeys().length-1; j++) {

                key = x.getKeys()[j];
                currentRow = nextRow;
                currentCompare = nextCompare;

                nextRow = x.getKeys()[j + 1].getRow(store);
                if (fieldCount > 0) {
                    nextCompare = compareRowNonUnique(session, nextRow.getData(),
                            rowdata, rowColMap, fieldCount);
                }

                if (currentCompare == 0) {
                    switch (compareType) {

                        case OpTypes.IS_NULL:
                        case OpTypes.EQUAL: {
                            result = key;

                            break;
                        }
                        case OpTypes.NOT:
                        case OpTypes.GREATER: {
                            currentCompare = compareObject(session, currentRow.getData(),
                                    rowdata, rowColMap, fieldCount,
                                    compareType);

                            if (currentCompare > 0) {
                                result = key;
                            }

                            break;
                        }
                        case OpTypes.GREATER_EQUAL_PRE:
                        case OpTypes.GREATER_EQUAL: {
                            currentCompare = compareObject(session, currentRow.getData(),
                                    rowdata, rowColMap, fieldCount,
                                    compareType);

                            if (currentCompare >= 0) {
                                result = key;
                            }

                            break;
                        }
                        case OpTypes.SMALLER: {
                            currentCompare = compareObject(session, currentRow.getData(),
                                    rowdata, rowColMap, fieldCount,
                                    compareType);

                            if (currentCompare < 0) {
                                result = key;
                            }

                            break;
                        }
                        case OpTypes.SMALLER_EQUAL: {
                            currentCompare = compareObject(session, currentRow.getData(),
                                    rowdata, rowColMap, fieldCount,
                                    compareType);

                            if (compareType <= 0) {
                                result = key;
                            }

                            break;
                        }
                        default:
                            Error.runtimeError(ErrorCode.U_S0500, "Index");
                    }
                }
            }


                    // MVCC 190
            if (session == null) {
                return result;
            }

            while (result != null) {
                currentRow = result.getRow(store);

                if (session.database.txManager.canRead(session, store,
                                                       currentRow, readMode,
                                                       colIndex)) {
                    break;
                }

                result = reversed ? last(store, result)
                                  : next(store, result);

                if (result == null) {
                    break;
                }

                currentRow = result.getRow(store);

                if (fieldCount > 0
                        && compareRowNonUnique(
                            session, currentRow.getData(), rowdata, rowColMap,
                            fieldCount) != 0) {
                    result = null;

                    break;
                }
            }

            return result;
        } catch (RuntimeException e) {
            throw e;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Find the first different node after/before the given one.
     * @param session
     * @param store
     * @param node
     * @param fieldCount
     * @param reversed  false for searching afterwards
     * @return
     */
    NodeBPlus findDistinctNode(Session session, PersistentStore store,
                             NodeBPlus node, int fieldCount, boolean reversed) {

        readLock.lock();


        try {
            NodeBPlus x          = getAccessor(store);
            NodeBPlus n          = null;
            NodeBPlus key        = null;
            NodeBPlus result     = null;
            Object[] rowData    = node.getData(store);
            Row     currentRow = null;
            Row     nextRow    = null;
            int     currentCompare = 0;
            int     nextCompare    = 0;

            while (x != null && !x.isLeaf) {
                currentCompare = 0;
                nextCompare    = 0;

                nextRow = x.getKeys()[0].getRow(store);

                if (fieldCount > 0) {
                    nextCompare = compareRowNonUnique(session, currentRow.getData(),
                            rowData, colIndex, fieldCount);
                }

                for (int j=0; j<x.getKeys().length-1; j++) {

                    key = x.getKeys()[j];
                    currentRow = nextRow;
                    currentCompare = nextCompare;

                    nextRow = x.getKeys()[j+1].getRow(store);
                    if (fieldCount > 0) {
                        nextCompare = compareRowNonUnique(session, nextRow.getData(),
                                rowData, colIndex, fieldCount);
                    }

                    if (currentCompare == 0 ) {

                        if (reversed) {
                            n = x.getPointers()[j+1];
                        } else {
                            n = x.getPointers()[j];
                        }

                        break;

                    }
                    else if (currentCompare < 0 ) {

                        if (reversed){
                            result = key;
                        }

                        if (nextCompare > 0) {
                            n = x.getPointers()[j+1];
                            break;
                        }
                    }
                    else if (currentCompare > 0) {

                        if (!reversed) {
                            result = key;
                        }

                        n = x.getPointers()[j];

                        break;
                    }


                }

                if (n == null) {
                    break;
                }

                x = n;
            }


            // in leaf node
            if (x != null) {

                currentCompare = 0;

                if (reversed) {

                    for (int j = x.getKeys().length - 1; j >= 0; j--) {

                        key = x.getKeys()[j];
                        currentRow = key.getRow(store);

                        if (fieldCount > 0) {
                            currentCompare = compareRowNonUnique(session, currentRow.getData(),
                                    rowData, colIndex, fieldCount);
                        }

                        if (currentCompare < 0) {
                            result = key;
                            break;
                        }

                    }
                } else {

                    for (int j = 0; j < x.getKeys().length; j++) {

                        key = x.getKeys()[j];
                        currentRow = key.getRow(store);

                        if (fieldCount > 0) {
                            currentCompare = compareRowNonUnique(session, currentRow.getData(),
                                    rowData, colIndex, fieldCount);
                        }

                        if (currentCompare > 0) {
                            result = key;
                            break;
                        }
                    }
                }
            }

            // MVCC 190
            if (session == null) {
                return result;
            }

            while (result != null) {
                currentRow = result.getRow(store);

                if (session.database.txManager.canRead(
                        session, store, currentRow,
                        TransactionManager.ACTION_READ, colIndex)) {
                    break;
                }

                result = reversed ? last(store, result)
                                  : next(store, result);
            }

            return result;
        } finally {
            readLock.unlock();
        }
    }


    public int searchCompare(Row currentRow, Session session, Row row){
        final Object[] rowData       = row.getData();
        boolean        compareRowId  = !isUnique || hasNulls(session, rowData);
        int            compare       = 0;

        compare = compareRowForInsertOrDelete(session, row,
                currentRow,
                compareRowId, 0);

        if (compare == 0) {
            if (isConstraint) {
                Constraint c =
                        ((Table) table).getUniqueConstraintForIndex(this);

                throw c.getException(row.getData());
            } else {
                throw Error.error(ErrorCode.X_23505,
                        name.statementName);
            }
        }

        return compare;
    }


    NodeBPlus getAccessor(PersistentStore store) {

        NodeBPlus node = (NodeBPlus) store.getAccessor(this);

        return node;
    }

    IndexRowIterator getIterator(Session session, PersistentStore store,
                                 NodeBPlus x, boolean single, boolean reversed) {

        if (x == null) {
            return emptyIterator;
        } else {
            IndexRowIterator it = new IndexRowIterator(session, store, this,
                x, 0, single, reversed);

            return it;
        }
    }

    public static final class IndexRowIterator implements RowIterator {

        final Session         session;
        final PersistentStore store;
        final IndexBPlus        index;
        NodeBPlus               nextnode;
        Row                   lastrow;
        int                   distinctCount;
        boolean               single;
        boolean               reversed;

        /**
         * When session == null, rows from all sessions are returned
         */
        public IndexRowIterator(Session session, PersistentStore store,
                                IndexBPlus index, NodeBPlus node,
                                int distinctCount, boolean single,
                                boolean reversed) {

            this.session       = session;
            this.store         = store;
            this.index         = index;
            this.distinctCount = distinctCount;
            this.single        = single;
            this.reversed      = reversed;

            if (index == null) {
                return;
            }

            nextnode = node;
        }

        public boolean hasNext() {
            return nextnode != null;
        }

        public Row getNextRow() {

            if (nextnode == null) {
                release();

                return null;
            }

            NodeBPlus lastnode = nextnode;

            if (single) {
                nextnode = null;
            } else {
                index.readLock.lock();
                store.writeLock();

                try {
                    if (reversed) {
                        nextnode = index.last(session, store, nextnode,
                                              distinctCount);
                    } else {
                        nextnode = index.next(session, store, nextnode,
                                              distinctCount);
                    }
                } finally {
                    store.writeUnlock();
                    index.readLock.unlock();
                }
            }

            lastrow = lastnode.getRow(store);

            return lastrow;
        }

        public Object[] getNext() {

            Row row = getNextRow();

            return row == null ? null
                               : row.getData();
        }

        public void removeCurrent() {
            store.delete(session, lastrow);
            store.remove(lastrow);
        }

        public void release() {}

        public boolean setRowColumns(boolean[] columns) {
            return false;
        }

        public long getRowId() {
            return nextnode.getPos();
        }


    }

}
