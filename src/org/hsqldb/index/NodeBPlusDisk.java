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

import java.io.IOException;

import org.hsqldb.RowBPlus;
import org.hsqldb.RowBPlusDisk;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.LongLookup;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.types.Type;

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// fredt@users 20020920 - path 1.7.1 - refactoring to cut mamory footprint
// fredt@users 20021205 - path 1.7.2 - enhancements

/**
 *  Cached table Node implementation.<p>
 *  Only integral references to left, right and parent nodes in the BPlus tree
 *  are held and used as pointers data.<p>
 *
 *  iId is a reference to the Index object that contains this node.<br>
 *  This fields can be eliminated in the future, by changing the
 *  method signatures to take a Index parameter from Index.java (fredt@users)
 *
 *  New class derived from Hypersonic SQL code and enhanced in HSQLDB. <p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge dot net)
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 2.2.9
 * @since Hypersonic SQL
 */
public class NodeBPlusDisk extends NodeBPlus {

    final RowBPlusDisk row;
    Type  type;

    //
    private int             iParent = NO_POS;
    private int             iLast   = NO_POS;
    private int             iNext   = NO_POS;
    private int             iId;    // id of Index object for this Node
    public static final int SIZE_IN_BYTE = 4 * 4;

    public NodeBPlusDisk(RowBPlusDisk r, RowInputInterface in,
                       int id) throws IOException {


        row      = r;
        iId      = id;
        iParent  = in.readInt();

        if (iParent <= 0) {
            iParent = NO_POS;
        }
    }

    public NodeBPlusDisk(RowBPlusDisk r, int id) {
        row = r;
        iId = id;
    }

    public void delete() {

        iParent  = NO_POS;
        nParent  = null;

        row.setNodesChanged();
    }

    public boolean isInMemory() {
        return row.isInMemory();
    }

    public boolean isMemory() {
        return false;
    }

    public long getPos() {
        return row.getPos();
    }

    public int getID() {
        return iId;
    }

    public RowBPlus getRow(PersistentStore store) {

        if (!row.isInMemory()) {
            return (RowBPlusDisk) store.get(this.row, false);
        } else {
            row.updateAccessCount(store.getAccessCount());
        }

        return row;
    }

    public Object[] getData(PersistentStore store) {
        return row.getData();
    }

    private NodeBPlusDisk findNode(PersistentStore store, int pos) {

        NodeBPlusDisk ret = null;
        RowBPlusDisk  r   = (RowBPlusDisk) store.get(pos, false);

        if (r != null) {
            ret = (NodeBPlusDisk) r.getNode(iId);
        }

        return ret;
    }


    NodeBPlus getParent(PersistentStore store) {

        NodeBPlusDisk node = this;
        RowBPlusDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowBPlusDisk) store.get(this.row, false);
            node = (NodeBPlusDisk) row.getNode(iId);
        }

        if (node.iParent == NO_POS) {
            return null;
        }

        if (node.nParent == null || !node.nParent.isInMemory()) {
            node.nParent = findNode(store, iParent);
        }

        return node.nParent;
    }


    NodeBPlus setParent(PersistentStore store, NodeBPlus n) {

        NodeBPlusDisk node = this;
        RowBPlusDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowBPlusDisk) store.get(this.row, true);
            node = (NodeBPlusDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            row.keepInMemory(false);

            throw Error.runtimeError(ErrorCode.U_S0500, "NodeBPlusDisk");
        }

        row.setNodesChanged();

        node.iParent = n == null ? NO_POS
                                 : (int) n.getPos();
        node.nParent = (NodeBPlusDisk) n;

        row.keepInMemory(false);

        return node;
    }

    boolean equals(NodeBPlus n) {

        if (n instanceof NodeBPlusDisk) {
            return this == n || (getPos() == ((NodeBPlusDisk) n).getPos());
        }

        return false;
    }

    public int getRealSize(RowOutputInterface out) {
        return NodeBPlusDisk.SIZE_IN_BYTE;
    }

    public void setInMemory(boolean in) {

        if (!in) {
            if (nParent != null) {
                for (int i=0; i < nParent.getKeys().length; i++) {
                    if (row.getPos() == nParent.getKeys()[i].getPos()) {
                        nParent.getKeys()[i] = null;
                    }
                }
            }

            nParent = null;
        }
    }

    public void write(RowOutputInterface out) {

        out.writeInt((iParent == NO_POS) ? 0
                                         : iParent);
    }

    public void write(RowOutputInterface out, LongLookup lookup) {

        out.writeInt(getTranslatePointer(iParent, lookup));
    }

    private static int getTranslatePointer(int pointer, LongLookup lookup) {

        int newPointer = 0;

        if (pointer != NodeBPlus.NO_POS) {
            if (lookup == null) {
                newPointer = pointer;
            } else {
                newPointer = (int) lookup.lookup(pointer);
            }
        }

        return newPointer;
    }

    public void restore() {}

    public void destroy() {}

    public void updateAccessCount(int count) {}

    public int getAccessCount() {
        return 0;
    }

    public void setStorageSize(int size) {}

    public int getStorageSize() {
        return 0;
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

    public boolean keepInMemory(boolean keep) {
        return false;
    }
}
