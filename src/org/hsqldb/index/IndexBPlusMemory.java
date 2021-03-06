/*
 * For work developed by the HSQL Development Group:
 *
 * Copyright (c) 2001-2010, The HSQL Development Group
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

import org.hsqldb.Constraint;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Row;
import org.hsqldb.RowBPlus;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.types.Type;

import javax.xml.soap.Node;
import java.util.Stack;

/**
 * Implementation of an BPlus for memory tables.<p>
 *
 *  New class derived from Hypersonic SQL code and enhanced in HSQLDB. <p>
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since Hypersonic SQL
 */
public class IndexBPlusMemory extends IndexBPlus {

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



    public IndexBPlusMemory(HsqlName name, long id, TableBase table,
                          int[] columns, boolean[] descending,
                          boolean[] nullsLast, Type[] colTypes, boolean pk,
                          boolean unique, boolean constraint,
                          boolean forward) {
        super(name, id, table, columns, descending, nullsLast, colTypes, pk,
              unique, constraint, forward);
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
                temp.setKeys(store, n.getKeys());
                temp.setPointers(n.getPointers());
                temp = insertNode(session, store, temp, x, null);

                NodeBPlus newNode = new NodeBPlus();
                int j = (n.getKeys().length + 1) / 2;

                //take the first half of the temp nde in current node
                n.setKeys(store, temp.getKeys(), 0, j);

                // copying the rest of temp node in new node
                newNode.setKeys(store, temp.getKeys(), j, temp.getKeys().length);
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

                        // n is root
                        NodeBPlus root = new NodeBPlus(false, false);  // interior node

                        root.addKeys(store, key);

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
                            temp.setKeys(store, n.getKeys());
                            temp.setPointers(n.getPointers());
                            temp = insertNode(session, store, temp, key, newNode);

                            j = temp.getPointers().length / 2;

                            n.setKeys(store, temp.getKeys(), 0, j-1);
                            n.setPointers(temp.getPointers(), 0, j);

                            newNode.setKeys(store, temp.getKeys(), j, temp.getKeys().length);
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

    public int searchCompare(Row currentRow, Session session, Row row){
        boolean        compareSimple = isSimple;
        final Object[] rowData       = row.getData();
        boolean        compareRowId  = !isUnique || hasNulls(session, rowData);
        int            compare       = 0;
        if (compareSimple) {
            compare =
                    colTypes[0].compare(session, rowData[colIndex[0]],
                            currentRow.getData()[colIndex[0]]);
            if (compare == 0 && compareRowId) {
                compare = compareRowForInsertOrDelete(session, row,
                        currentRow,
                        compareRowId, 1);
            }
        } else {
            compare = compareRowForInsertOrDelete(session, row,
                    currentRow,
                    compareRowId, 0);
        }

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

    public int searchCompareForDelete(Row currentRow, Session session, Row row) {
        final Object[] rowData       = row.getData();
        boolean        compareRowId  = !isUnique || hasNulls(session, rowData);
        return compareRowForInsertOrDelete(session, row,
                currentRow,
                compareRowId, 0);
    }

    private NodeBPlus insertNode(Session session, PersistentStore store,
                                 NodeBPlus node, NodeBPlus key, NodeBPlus pointer) {
//        RowComparator comparator = new RowComparator(session);
//        ArrayUtil.toAdjustedArray(leaf.keys, data, 0, 1);
//        ArraySort.sort(leaf.keys, 0, leaf.keys.length, comparator);

        int pos = 0;

        if (node == null) {
            node.addKeys(store, key);
            key.setParent(store, node);
            return node;
        }

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


    /**
     * Initialize a leaf node.
     */
    private NodeBPlus newLeafNode(PersistentStore store, NodeBPlus x) {

        NodeBPlus n = new NodeBPlus();
        n.addKeys(store, x);
        x.setParent(store, n);

        return n;
    }

    void delete(Session session, PersistentStore store, NodeBPlus x) {

        Stack<NodeBPlus> stack = new Stack<NodeBPlus>();
        NodeBPlus n;
        NodeBPlus root;
        Row       row = x.getRow(store);


        int compare = 0;

        writeLock.lock();

        try {
            root = getAccessor(store);
            n = root;

            if (n == null) {         // empty tree
                return;
            }

            while (!n.isLeaf) {
                stack.push(n);

                Row currentRow = n.getKeys()[0].row;
                compare = searchCompareForDelete(currentRow, session, row);
                if (compare < 0) {
                    n = n.getPointers()[0];
                    continue;
                }

                currentRow = n.getKeys()[n.getKeys().length-1].row;
                compare = searchCompareForDelete(currentRow, session, row);
                if (compare >= 0) {
                    n = n.getPointers()[n.getPointers().length-1];
                    continue;
                }

                Row nextRow = n.getKeys()[0].row;
                for (int i=0; i < n.getKeys().length-1; i++) {
                    currentRow = nextRow;
                    nextRow = n.getKeys()[i+1].row;
                    if (searchCompareForDelete(currentRow, session, row) >= 0 &&
                            searchCompareForDelete(nextRow, session, row) < 0) {
                        n = n.getPointers()[i+1];
                    }
                }
            }

            // n is a leaf node

            boolean flag = false;

            for (int i = 0; i < n.getKeys().length; i++) {

                if (n == root && x == n.getKeys()[i]) {

                    n.removeKeys(store, i);
                    return;

                } else if (x == n.getKeys()[i]) {

                    flag = true;
                    break;

                }
            }

            if (flag) {

                if (n.getKeys().length-1 >= n.nodeSize / 2) {
                    n.removeKeys(store, x);

                    NodeBPlus parent = stack.peek();
                    for (int i=0; i<parent.getKeys().length; i++) {

                        if (searchCompareForDelete(parent.getKeys()[i].getRow(store),
                                session, x.getRow(store)) == 0) {

                            parent.replaceKeys(store, n.getKeys()[0], i);
                            break;

                        }
                    }
                }

                else {  // combining nodes

                    NodeBPlus parent = stack.peek();

                    // Determine whether the next node comes from
                    // the same parent. If so, borrow elements from it.
                    int deter = sameParent(n, parent);


                    if (deter == 1) {
                        // borrow from the next leaf node

                        n.removeKeys(store, x);

                        NodeBPlus key = n.getNextPage().removeKeys(store, 0);

                        n.addKeys(store, key);

                        for (int i = 0; i < parent.getKeys().length; i++) {

                            if (searchCompareForDelete(parent.getKeys()[i].getRow(store),
                                    session, key.getRow(store)) == 0) {

                                parent.replaceKeys(store, n.getNextPage().getKeys()[0], i);
                                break;
                            }
                        }

                        for (int i = 0; i < parent.getKeys().length; i++) {

                            if (searchCompareForDelete(parent.getKeys()[i].getRow(store),
                                    session, x.getRow(store)) == 0) {

                                parent.replaceKeys(store, n.getKeys()[0], i);
                                break;
                            }
                        }

                        return;

                    }

                    else if (deter == 2) {
                        // borrow from the previous leaf node

                        n.removeKeys(store, x);

                        NodeBPlus key =
                                n.getLastPage().removeKeys(store,
                                        n.getLastPage().getKeys().length-1);
                        n.addKeys(store, key, 0);

                        for (int i = 0; i < parent.getKeys().length; i++) {

                            if (searchCompareForDelete(parent.getKeys()[i].getRow(store),
                                    session, key.getRow(store)) == 0) {

                                parent.replaceKeys(store,
                                        n.getLastPage().getKeys()[n.getLastPage().getKeys().length-1],
                                        i);
                                break;
                            }
                        }

                        for (int i = 0; i < parent.getKeys().length; i++) {

                            if (searchCompareForDelete(parent.getKeys()[i].getRow(store),
                                    session, x.getRow(store)) == 0) {

                                parent.replaceKeys(store, n.getKeys()[0], i);
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

                        n.removeKeys(store, x);

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

                            for (int i=n.getKeys().length-1; i>=0; i--){
                                next.addKeys(store, n.getKeys()[i], 0);
                            }

                            n.delete();

                            for (int i = 0; i < parent.getKeys().length; i++) {

                                if (searchCompareForDelete(parent.getKeys()[i].getRow(store), session,
                                        next.getKeys()[next.getKeys().length-1].getRow(store)) == 0) {

                                    tempKey = i;
                                    tempPointer = i;

                                    break;
                                }
                            }
                            if (tempKey > 0 && parent.getKeys()[tempKey - 1] == x) {
                                parent.replaceKeys(store, next.getKeys()[0], tempKey-1);
                            }

                        }

                        // merging with the last node
                        else {

                            NodeBPlus prev = n.getLastPage();
                            if (prev != null) {
                                prev.setNextPage(n.getNextPage());
                            } else {
                                return;
                            }

                            if (n.getNextPage() != null) {
                                n.getNextPage().setLastPage(prev);
                            }

                            n.setNextPage(null);
                            n.setLastPage(null);

                            for (int i = 0; i < n.getKeys().length; i++) {
                                prev.addKeys(store, n.getKeys()[i]);
                            }

                            if (prevB) {
                                for (int i = 0; i < parent.getKeys().length; i++) {

                                    if (searchCompareForDelete(parent.getKeys()[i].getRow(store), session,
                                            n.getKeys()[0].getRow(store)) == 0) {

                                        tempKey = i;
                                        tempPointer = i + 1;
                                        break;
                                    }
                                }
                            } else {
                                for (int i = 0; i < parent.getKeys().length; i++) {
                                    if (searchCompareForDelete(parent.getKeys()[i].getRow(store), session,
                                            x.getRow(store)) == 0) {

                                        tempKey = i;
                                        tempPointer = i + 1;
                                        break;
                                    }
                                }
                            }
                        }

                        boolean finished = false;
                        do {
                            // if we get root
                            if (stack.isEmpty()) {

                                root.removeKeys(store, tempKey);
                                root.removePointers(tempPointer);
                                finished = true;

                            }
                            else {

                                n = stack.pop();

                                //try borrowing from the sibling
                                if (n.getKeys().length >= 2) {

                                    n.removeKeys(store, tempKey);
                                    n.removePointers(tempPointer);

                                    finished = true;

                                } else {
                                    // if the root has single sibling
                                    // the tree height will decrease

                                    if (n == root) {

                                        n.removeKeys(store, tempKey);
                                        n.removePointers(tempPointer);

                                        if (n.getPointers().length == 1) {
                                            root = n.getPointers()[0];
                                            store.setAccessor(this, root);
                                        }

                                        finished = true;

                                    } else {

                                        n.removeKeys(store, tempKey);
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

                                            n.addKeys(store, parent.removeKeys(store, index - 1));
                                            n.addPointers(n.getNextPage().removePointers(0));

                                            NodeBPlus temp = n.getNextPage().removeKeys(store, 0);

                                            parent.addKeys(store, temp, index-1);

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
                                            n.addKeys(store, parent.removeKeys(store, index - 1), 0);
                                            n.addPointers(n.getLastPage().removePointers(
                                                    n.getLastPage().getPointers().length - 1),
                                                    0);

                                            parent.addKeys(store, n.getLastPage().removeKeys(
                                                    store, n.getLastPage().getKeys().length - 1),
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

                                                next.addKeys(store, parent.getKeys()[tempKey], 0);

                                                for (int i=n.getKeys().length-1; i>=0; i--){
                                                    next.addKeys(store, n.getKeys()[i], 0);
                                                }

                                                for (int i=n.getPointers().length-1; i>=0; i--){
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

                                                NodeBPlus prev = n.getLastPage();
                                                if (prev != null) {
                                                    prev.setNextPage(n.getNextPage());
                                                }

                                                if (n.getNextPage() != null) {
                                                    n.getNextPage().setLastPage(prev);
                                                }

                                                prev.addKeys(store, parent.getKeys()[tempKey]);

                                                for (int i=0; i<n.getKeys().length; i++){
                                                    prev.addKeys(store, n.getKeys()[i]);
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

            else {
                System.out.print("not found!");

            }




        } catch (RuntimeException e) {
            throw e;
        } finally {
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

    //NodeBPlus next(PersistentStore store, NodeBPlus x) {

//        NodeBPlus r = x.nRight;
//
//        if (r != null) {
//            x = r;
//
//            NodeBPlus l = x.nLeft;
//
//            while (l != null) {
//                x = l;
//                l = x.nLeft;
//            }
//
//            return x;
//        }
//
//        NodeBPlus ch = x;
//
//        x = x.nParent;
//
//        while (x != null && ch == x.nRight) {
//            ch = x;
//            x  = x.nParent;
//        }
//
//        return x;
//    }

    //NodeBPlus last(PersistentStore store, NodeBPlus x) {

//        if (x == null) {
//            return null;
//        }
//
//        NodeBPlus left = x.nLeft;
//
//        if (left != null) {
//            x = left;
//
//            NodeBPlus right = x.nRight;
//
//            while (right != null) {
//                x     = right;
//                right = x.nRight;
//            }
//
//            return x;
//        }
//
//        NodeBPlus ch = x;
//
//        x = x.nParent;
//
//        while (x != null && ch.equals(x.nLeft)) {
//            ch = x;
//            x  = x.nParent;
//        }
//
//        return x;
//    }

//    /**
//     * Balances part of the tree after an alteration to the index.
//     */
//    void balance(PersistentStore store, NodeBPlus x, boolean isleft) {
//
//        while (true) {
//            int sign = isleft ? 1
//                              : -1;
//
//            switch (x.iBalance * sign) {
//
//                case 1 :
//                    x.iBalance = 0;
//
//                    return;
//
//                case 0 :
//                    x.iBalance = -sign;
//                    break;
//
//                case -1 :
//                    NodeBPlus l = isleft ? x.nLeft
//                                       : x.nRight;
//
//                    if (l.iBalance == -sign) {
//                        x.replace(store, this, l);
//                        x.set(store, isleft, l.child(store, !isleft));
//                        l.set(store, !isleft, x);
//
//                        x.iBalance = 0;
//                        l.iBalance = 0;
//                    } else {
//                        NodeBPlus r = !isleft ? l.nLeft
//                                            : l.nRight;
//
//                        x.replace(store, this, r);
//                        l.set(store, !isleft, r.child(store, isleft));
//                        r.set(store, isleft, l);
//                        x.set(store, isleft, r.child(store, !isleft));
//                        r.set(store, !isleft, x);
//
//                        int rb = r.iBalance;
//
//                        x.iBalance = (rb == -sign) ? sign
//                                                   : 0;
//                        l.iBalance = (rb == sign) ? -sign
//                                                  : 0;
//                        r.iBalance = 0;
//                    }
//
//                    return;
//            }
//
//            if (x.nParent == null) {
//                return;
//            }
//
//            isleft = x.nParent == null || x == x.nParent.nLeft;
//            x      = x.nParent;
//        }
//    }
}
