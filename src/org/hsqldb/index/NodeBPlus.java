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

import org.hsqldb.Row;
import org.hsqldb.RowBPlus;
import org.hsqldb.RowBPlusDisk;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.LongLookup;
import org.hsqldb.lib.ObjectComparator;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.rowio.RowInputInterface;

import javax.xml.soap.Node;

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// fredt@users 20020920 - path 1.7.1 - refactoring to cut mamory footprint
// fredt@users 20021205 - path 1.7.2
// - enhancements
// fredt@users 20021215 - doc 1.7.2 - javadoc comments

/**
 *  The parent for all BPlus node implementations. Subclasses of Node vary
 *  in the way they hold
 *  references to other Nodes in the BPlus tree, or to their Row data.<br>
 *
 *  nNext links the Node objects belonging to different indexes for each
 *  table row. It is used solely by Row to locate the node belonging to a
 *  particular index.<br>
 *
 *  New class derived from Hypersonic SQL code and enhanced in HSQLDB. <p>
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.9.0
 * @since Hypersonic SQL
 */
public class NodeBPlus implements CachedObject {

    static final int NO_POS = RowBPlusDisk.NO_POS;
    public int       iBalance;
    public boolean   isLeaf;
    public boolean   isData;        // Note: data node is a corresponding node to a single row
                                    // in oder to organize the data.
    public int       nodeSize = 3;
    public NodeBPlus   nNext;    // node of next index (nNext==null || nNext.iId=iId+1)

    //
    protected NodeBPlus   nLeft;
    protected NodeBPlus   nRight;
    protected NodeBPlus   nParent;

    protected NodeBPlus[] keys     = new NodeBPlus[]{};
    protected NodeBPlus[] pointers = new NodeBPlus[]{};
    protected NodeBPlus nextPage;    // next leaf node

    protected final Row row;

    NodeBPlus() {
        // default new created leaf node
        row = null;
        isLeaf = true;
        isData = false;
    }

    public NodeBPlus(Row r) {
        // default new created data node
        row = r;
        isLeaf = false;
        isData = true;
    }

    public NodeBPlus(boolean isData, boolean isLeaf){
        // new created interior node
        this.row    = null;
        this.isLeaf = isLeaf;
        this.isData = isData;
    }

    public NodeBPlus newInteriorNode(){
        return new NodeBPlus(false, false);
    }

    public void delete() {
        ArrayUtil.clearArray(ArrayUtil.CLASS_CODE_OBJECT, keys, 0, keys.length);
        ArrayUtil.clearArray(ArrayUtil.CLASS_CODE_OBJECT, pointers, 0, pointers.length);
        nextPage = null;
        iBalance = 0;
        nLeft    = nRight = nParent = null;
    }

    NodeBPlus getLeft(PersistentStore store) {
        return nLeft;
    }

    NodeBPlus setLeft(PersistentStore persistentStore, NodeBPlus n) {

        nLeft = n;

        return this;
    }

    public int getBalance(PersistentStore store) {
        return iBalance;
    }

    boolean isLeft(NodeBPlus node) {
        return nLeft == node;
    }

    boolean isRight(NodeBPlus node) {
        return nRight == node;
    }

    NodeBPlus getRight(PersistentStore persistentStore) {
        return nRight;
    }

    NodeBPlus setRight(PersistentStore persistentStore, NodeBPlus n) {

        nRight = n;

        return this;
    }

    NodeBPlus getParent(PersistentStore store) {
        return nParent;
    }

    boolean isRoot(PersistentStore store) {
        return nParent == null;
    }

    NodeBPlus setParent(PersistentStore persistentStore, NodeBPlus n) {

        nParent = n;

        return this;
    }

    public NodeBPlus setBalance(PersistentStore store, int b) {

        iBalance = b;

        return this;
    }

    boolean isFromLeft(PersistentStore store) {

        if (nParent == null) {
            return true;
        }

        return this == nParent.nLeft;
    }

    public NodeBPlus child(PersistentStore store, boolean isleft) {
        return isleft ? getLeft(store)
                      : getRight(store);
    }

    public NodeBPlus set(PersistentStore store, NodeBPlus key, NodeBPlus pointer, int pos) {
        if (isLeaf) {
            // leaf node
            keys = (NodeBPlus[])ArrayUtil.toAdjustedArray(keys, key, pos, 1);
        }
        else {
            // interior node
            keys = (NodeBPlus[])ArrayUtil.toAdjustedArray(keys, key, pos, 1);
            pointers = (NodeBPlus[])ArrayUtil.toAdjustedArray(pointer, pointer, pos+1, 1);
        }
        return this;
    }

    public NodeBPlus set(PersistentStore store, boolean isLeft, NodeBPlus n) {

        if (isLeft) {
            nLeft = n;
        } else {
            nRight = n;
        }

        if (n != null) {
            n.nParent = this;
        }

        return this;
    }

    public void replace(PersistentStore store, Index index, NodeBPlus n) {

        // TODO
//        if (nParent == null) {
//            if (n != null) {
//                n = n.setParent(store, null);
//            }
//
//            store.setAccessor(index, n);
//        } else {
//            nParent.set(store, isFromLeft(store), n);
//        }
    }

    boolean equals(NodeBPlus n) {
        return n == this;
    }

    public void setInMemory(boolean in) {}

    public int getDefaultCapacity() {
        return 0;
    }

    public void read(RowInputInterface in) {}

    public void write(RowOutputInterface out) {}

    public void write(RowOutputInterface out, LongLookup lookup) {}

    public long getPos() {
        return 0;
    }

    public RowBPlus getRow(PersistentStore store) {
        if (isData) {
            return (RowBPlus) row;
        }
        else {
            return null;
        }
    }

    protected Object[] getData(PersistentStore store) {
        if (isData) {
            return row.getData();
        }
        else {
            return null;
        }
    }

    public NodeBPlus[] getKeys() {
        return keys;
    }

    public NodeBPlus[] getPointers() {
        return pointers;
    }

    public NodeBPlus getNextPage() {
        return nextPage;
    }

    public void setLeaf(boolean isLeaf) {
        this.isLeaf = isLeaf;
    }

    public void setKeys(NodeBPlus[] keys) {
        ArrayUtil.copyArray(keys, this.keys, keys.length);
    }

    public void setKeys(NodeBPlus[] keys, int start, int end) {
        ArrayUtil.copyArray(keys, this.keys, start, end);
    }

    public void addKeys(NodeBPlus key) {
        keys = (NodeBPlus[]) ArrayUtil.toAdjustedArray(this.keys, key, this.keys.length, 1);
    }

    public void setPointers(NodeBPlus[] pointers) {
        ArrayUtil.copyArray(pointers, this.pointers, pointers.length);
//        this.pointers = (NodeBPlus[]) ArrayUtil.duplicateArray(pointers);
    }

    public void setPointers(NodeBPlus[] pointers, int start, int end) {
        ArrayUtil.copyArray(pointers, this.pointers, start, end);
    }

    public void addPointers(NodeBPlus pointer){
        pointers = (NodeBPlus[]) ArrayUtil.toAdjustedArray(this.pointers, pointer, this.pointers.length, 1);
    }

    public void setNextPage(NodeBPlus n) {
        this.nextPage = n;
    }

    public void updateAccessCount(int count) {}

    public int getAccessCount() {
        return 0;
    }

    public void setStorageSize(int size) {}

    public int getStorageSize() {
        return 0;
    }

    final public boolean isBlock() {
        return false;
    }

    public void setPos(long pos) {}

    public boolean isNew() {
        return false;
    }

    public boolean hasChanged() {
        return false;
    }

    public boolean isKeepInMemory() {
        return false;
    }
    ;

    public boolean keepInMemory(boolean keep) {
        return true;
    }

    public boolean isInMemory() {
        return false;
    }

    public void restore() {}

    public void destroy() {}

    public int getRealSize(RowOutputInterface out) {
        return 0;
    }

    public boolean isMemory() {
        return true;
    }


}
