/* Copyright (c) 2001-2011, The HSQL Development Group
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
 */


package org.hsqldb.index;

import java.io.IOException;

import org.hsqldb.RowBPlus;
import org.hsqldb.RowBPlusDisk;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.LongLookup;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

/**
 *  Subclass of NodeBPlus for huge databases.
 * @author Fred Toussi (fredt@users dot sourceforge dot net)
 * @version 2.2.9
 * @since 2.2.9
 */
public class NodeBPlusDiskLarge extends NodeBPlus {

    final RowBPlusDisk row;

    //
    private long            iParent = NO_POS;
    private int             iId;    // id of Index object for this Node
    public static final int SIZE_IN_BYTE = 4 * 4;

    public NodeBPlusDiskLarge(RowBPlusDisk r, RowInputInterface in,
                       int id) throws IOException {

        int ext;

        row      = r;
        iId      = id;
        ext      = in.readInt();
        iParent  = in.readInt() & 0xffffffffL;

        if (ext > 0xff) {
            iParent |= (((long) ext << 8) & 0xff00000000L);
        }

        if (iParent == 0) {
            iParent = NO_POS;
        }
    }

    public NodeBPlusDiskLarge(RowBPlusDisk r, int id) {
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

    private NodeBPlusDiskLarge findNode(PersistentStore store, long pos) {

        NodeBPlusDiskLarge ret = null;
        RowBPlusDisk  r   = (RowBPlusDisk) store.get(pos, false);

        if (r != null) {
            ret = (NodeBPlusDiskLarge) r.getNode(iId);
        }

        return ret;
    }

    NodeBPlus getParent(PersistentStore store) {

        NodeBPlusDiskLarge node = this;
        RowBPlusDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowBPlusDisk) store.get(this.row, false);
            node = (NodeBPlusDiskLarge) row.getNode(iId);
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

        NodeBPlusDiskLarge node = this;
        RowBPlusDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowBPlusDisk) store.get(this.row, true);
            node = (NodeBPlusDiskLarge) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            row.keepInMemory(false);

            throw Error.runtimeError(ErrorCode.U_S0500, "NodeBPlusDisk");
        }

        row.setNodesChanged();

        node.iParent = n == null ? NO_POS
                                 : ((NodeBPlusDisk) n).getID();

        if (n != null && !n.isInMemory()) {
            n = findNode(store, n.getPos());
        }

        node.nParent = (NodeBPlusDiskLarge) n;

        row.keepInMemory(false);

        return node;
    }



    public void replace(PersistentStore store, Index index, NodeBPlus n) {

//        NodeBPlusDiskLarge node = this;
//        RowBPlusDisk  row  = this.row;
//
//        if (!row.keepInMemory(true)) {
//            row  = (RowBPlusDisk) store.get(this.row, true);
//            node = (NodeBPlusDiskLarge) row.getNode(iId);
//        }
//
//        if (node.iParent == NO_POS) {
//            if (n != null) {
//                n = n.setParent(store, null);
//            }
//
//            store.setAccessor(index, n);
//        } else {
//            boolean isFromLeft = node.isFromLeft(store);
//
//            node.getParent(store).set(store, isFromLeft, n);
//        }
//
//        row.keepInMemory(false);
    }

    boolean equals(NodeBPlus n) {

        if (n instanceof NodeBPlusDiskLarge) {
            return this == n || row.getPos() == ((NodeBPlusDiskLarge) n).getPos();
        }

        return false;
    }

    public int getRealSize(RowOutputInterface out) {
        return NodeBPlusDiskLarge.SIZE_IN_BYTE;
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
        write(out, null);
    }

    public void write(RowOutputInterface out, LongLookup lookup) {

        long parentTemp = getTranslatePointer(iParent, lookup);
        int  ext        = 0;

        ext |= (int) ((parentTemp & 0xff00000000L) >> 8);

        out.writeInt(ext);
        out.writeInt((int) parentTemp);
    }

    private static long getTranslatePointer(long pointer, LongLookup lookup) {

        long newPointer = 0;

        if (pointer != NodeBPlus.NO_POS) {
            if (lookup == null) {
                newPointer = pointer;
            } else {
                newPointer = lookup.lookup(pointer);
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
