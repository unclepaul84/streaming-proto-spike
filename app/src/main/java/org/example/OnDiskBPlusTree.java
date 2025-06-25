// OnDiskBPlusTree.java

package org.example;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

public class OnDiskBPlusTree {
    static final int PAGE_SIZE = 4096;
    static final int MAX_KEY_SIZE = 128;
    static final int MAX_VALUE_SIZE = 1024;

    static final byte NODE_LEAF = 1;
    static final byte NODE_INTERNAL = 2;
    static final byte NODE_OVERFLOW = 3;

    RandomAccessFile file;
    FileChannel channel;
    MappedByteBuffer map;
    int nextPageId = 1;

    public OnDiskBPlusTree(String path) throws IOException {
        boolean newFile = !Files.exists(Paths.get(path));
        file = new RandomAccessFile(path, "rw");
        channel = file.getChannel();
        map = channel.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
        if (newFile) {
            int root = allocateLeafPage();
            map.putInt(0, root); // root page pointer at position 0
        } else {
            nextPageId = (int) (channel.size() / PAGE_SIZE);
        }
    }

    public void insert(byte[] key, byte[] value) throws IOException {
        InsertResult result = insertRecursive(getRootPageId(), key, value);
        if (result != null && result.newRightPage != -1) {
            int newRoot = allocateInternalPage();
            ByteBuffer rootBuf = getPage(newRoot);
            rootBuf.put(0, NODE_INTERNAL);
            rootBuf.putInt(1, 1); // entry count
            writeInternalEntry(rootBuf, 5, result.middleKey, getRootPageId(), result.newRightPage);
            map.putInt(0, newRoot); // update root pointer
        }
    }

    private InsertResult insertRecursive(int pageId, byte[] key, byte[] value) throws IOException {
        ByteBuffer buf = getPage(pageId);
        byte type = buf.get(0);
        int count = buf.getInt(1);

        if (type == NODE_LEAF) {
            List<LeafEntry> entries = readLeafEntries(buf);
            for (LeafEntry e : entries) {
                if (Arrays.equals(e.key, key)) {
                    appendToOverflow(e.overflowPage, value);
                    return null;
                }
            }

            int overflow = allocateOverflow(value);
            entries.add(new LeafEntry(key, overflow));
            entries.sort(Comparator.comparing(e -> new ByteArrayWrapper(e.key)));

            if (estimateLeafSize(entries) <= PAGE_SIZE - 5) {
                writeLeafEntries(buf, entries);
                return null;
            } else {
                int splitIndex = entries.size() / 2;
                List<LeafEntry> left = entries.subList(0, splitIndex);
                List<LeafEntry> right = entries.subList(splitIndex, entries.size());
                writeLeafEntries(buf, left);

                int newRightPage = allocateLeafPage();
                ByteBuffer rightBuf = getPage(newRightPage);
                writeLeafEntries(rightBuf, right);

                return new InsertResult(right.get(0).key, newRightPage);
            }
        }

        if (type == NODE_INTERNAL) {
            List<InternalEntry> entries = readInternalEntries(buf);
            int i = 0;
            for (; i < entries.size(); i++) {
                if (Arrays.compare(key, entries.get(i).key) < 0) break;
            }

            int childPage = (i == 0) ? entries.get(0).leftChild : entries.get(i - 1).rightChild;
            InsertResult res = insertRecursive(childPage, key, value);
            if (res == null || res.newRightPage == -1) return null;

            byte[] newKey = res.middleKey;
            int newPage = res.newRightPage;

            InternalEntry insert = new InternalEntry(newKey, childPage, newPage);
            entries.add(insert);
            entries.sort(Comparator.comparing(e -> new ByteArrayWrapper(e.key)));

            if (estimateInternalSize(entries) <= PAGE_SIZE - 5) {
                writeInternalEntries(buf, entries);
                return null;
            } else {
                int splitIndex = entries.size() / 2;
                InternalEntry mid = entries.get(splitIndex);

                List<InternalEntry> left = entries.subList(0, splitIndex);
                List<InternalEntry> right = entries.subList(splitIndex + 1, entries.size());

                writeInternalEntries(buf, left);
                int newRight = allocateInternalPage();
                writeInternalEntries(getPage(newRight), right);

                return new InsertResult(mid.key, newRight);
            }
        }

        throw new IllegalStateException("Unknown page type");
    }

    public Iterable<byte[]> search(byte[] key) {
        return () -> new Iterator<byte[]>() {
            int overflowPage = findOverflow(getRootPageId(), key);
            ByteBuffer buf = (overflowPage != -1) ? getPage(overflowPage) : null;
            boolean valid = overflowPage != -1;

            @Override
            public boolean hasNext() {
                return valid;
            }

  @Override
    public byte[] next() {
        if (!valid) throw new NoSuchElementException();

        buf.position(5); // ✅ Fix: ensure correct buffer position for each page
        int len = buf.getInt();
        byte[] val = new byte[len];
        buf.get(val);
        int next = buf.getInt(PAGE_SIZE - 4);
        if (next == -1) {
            valid = false;
        } else {
            buf = getPage(next);
            // don't need buf.position(5) here again because next() restarts at top
        }
        return val;
    }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove not supported");
            }
        };
    }

    private int findOverflow(int pageId, byte[] key) {
        ByteBuffer buf = getPage(pageId);
        byte type = buf.get(0);

        if (type == NODE_LEAF) {
            List<LeafEntry> entries = readLeafEntries(buf);
            for (LeafEntry e : entries) {
                if (Arrays.equals(e.key, key)) return e.overflowPage;
            }
            return -1;
        }

        if (type == NODE_INTERNAL) {
            List<InternalEntry> entries = readInternalEntries(buf);
            for (InternalEntry e : entries) {
                if (Arrays.compare(key, e.key) < 0) return findOverflow(e.leftChild, key);
            }
            return findOverflow(entries.get(entries.size() - 1).rightChild, key);
        }

        throw new IllegalStateException("Unknown page type");
    }

    // --- Data Structures

    static class LeafEntry {
        byte[] key;
        int overflowPage;
        LeafEntry(byte[] key, int overflowPage) {
            this.key = key;
            this.overflowPage = overflowPage;
        }
    }

    static class InternalEntry {
        byte[] key;
        int leftChild, rightChild;
        InternalEntry(byte[] key, int leftChild, int rightChild) {
            this.key = key;
            this.leftChild = leftChild;
            this.rightChild = rightChild;
        }
    }

    static class InsertResult {
        byte[] middleKey;
        int newRightPage;
        InsertResult(byte[] key, int page) {
            this.middleKey = key;
            this.newRightPage = page;
        }
    }

    static class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
        byte[] data;
        ByteArrayWrapper(byte[] d) { data = d; }
        public int compareTo(ByteArrayWrapper o) { return Arrays.compare(data, o.data); }
    }

    // --- Page Access + Serialization
private ByteBuffer getPage(int pageId) {
    int offset = pageId * PAGE_SIZE;
    if (offset + PAGE_SIZE > map.capacity()) {
        throw new IllegalStateException("Exceeded mapped file size. Increase initial map size or switch to dynamic remap.");
    }
    map.position(offset);
    ByteBuffer slice = map.slice();
    slice.limit(PAGE_SIZE); // only valid because we checked above
    return slice;
}

    private int getRootPageId() { return map.getInt(0); }

    private int allocateLeafPage() {
        int id = nextPageId++;
        ByteBuffer buf = getPage(id);
        buf.put(0, NODE_LEAF);
        buf.putInt(1, 0);
        return id;
    }

    private int allocateInternalPage() {
        int id = nextPageId++;
        ByteBuffer buf = getPage(id);
        buf.put(0, NODE_INTERNAL);
        buf.putInt(1, 0);
        return id;
    }

    private int allocateOverflow(byte[] value) {
        int pageId = nextPageId++;
        ByteBuffer buf = getPage(pageId);
        buf.put(0, NODE_OVERFLOW);
        buf.putInt(1, 0);
        buf.position(5);
        buf.putInt(value.length);
        buf.put(value);
        buf.putInt(PAGE_SIZE - 4, -1);
        return pageId;
    }

    private void appendToOverflow(int pageId, byte[] value) {
        while (true) {
            ByteBuffer buf = getPage(pageId);
            int next = buf.getInt(PAGE_SIZE - 4);
            if (next == -1) {
                int newPage = allocateOverflow(value);
                buf.putInt(PAGE_SIZE - 4, newPage);
                break;
            }
            pageId = next;
        }
    }

    private List<LeafEntry> readLeafEntries(ByteBuffer buf) {
        int count = buf.getInt(1);
        List<LeafEntry> entries = new ArrayList<>();
        int pos = 5;
        for (int i = 0; i < count; i++) {
            buf.position(pos);
            int klen = buf.getInt();
            byte[] key = new byte[klen];
            buf.get(key);
            int overflow = buf.getInt();
            entries.add(new LeafEntry(key, overflow));
            pos = buf.position();
        }
        return entries;
    }

    private void writeLeafEntries(ByteBuffer buf, List<LeafEntry> entries) {
        buf.clear();
        buf.put(0, NODE_LEAF);
        buf.putInt(1, entries.size());
        buf.position(5);
        for (LeafEntry e : entries) {
            buf.putInt(e.key.length);
            buf.put(e.key);
            buf.putInt(e.overflowPage);
        }
    }

    private List<InternalEntry> readInternalEntries(ByteBuffer buf) {
        int count = buf.getInt(1);
        List<InternalEntry> entries = new ArrayList<>();
        int pos = 5;
        for (int i = 0; i < count; i++) {
            buf.position(pos);
            int klen = buf.getInt();
            byte[] key = new byte[klen];
            buf.get(key);
            int left = buf.getInt();
            int right = buf.getInt();
            entries.add(new InternalEntry(key, left, right));
            pos = buf.position();
        }
        return entries;
    }

    private void writeInternalEntries(ByteBuffer buf, List<InternalEntry> entries) {
        buf.clear();
        buf.put(0, NODE_INTERNAL);
        buf.putInt(1, entries.size());
        buf.position(5);
        for (InternalEntry e : entries) {
            buf.putInt(e.key.length);
            buf.put(e.key);
            buf.putInt(e.leftChild);
            buf.putInt(e.rightChild);
        }
    }

    private void writeInternalEntry(ByteBuffer buf, int pos, byte[] key, int left, int right) {
        buf.position(pos);
        buf.putInt(key.length);
        buf.put(key);
        buf.putInt(left);
        buf.putInt(right);
    }

    private int estimateLeafSize(List<LeafEntry> entries) {
        return 5 + entries.stream().mapToInt(e -> 4 + e.key.length + 4).sum();
    }

    private int estimateInternalSize(List<InternalEntry> entries) {
        return 5 + entries.stream().mapToInt(e -> 4 + e.key.length + 4 + 4).sum();
    }
}
