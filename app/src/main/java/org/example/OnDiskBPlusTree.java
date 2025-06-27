// OnDiskBPlusTree.java

package org.example;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

public class OnDiskBPlusTree implements AutoCloseable {
    static final int PAGE_SIZE = 1024 * 4;
    static final int MAX_KEY_SIZE = 128;
    static final int MAX_VALUE_SIZE = 8;

    static final byte NODE_LEAF = 1;
    static final byte NODE_INTERNAL = 2;
    static final byte NODE_OVERFLOW = 3;

    RandomAccessFile file;
    FileChannel channel;
    long nextPageId = 1;

    public OnDiskBPlusTree(String path) throws IOException {
        boolean newFile = !Files.exists(Paths.get(path));
        file = new RandomAccessFile(path, "rw");
        channel = file.getChannel();
        if (newFile) {
            long root = allocateLeafPage();
            writeRootPageId(root);
        } else {
            nextPageId = channel.size() / PAGE_SIZE;
        }
    }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.force(true);
            channel.close();
        }
        if (file != null) {
            file.close();
        }
    }

    public void insert(byte[] key, byte[] value) throws IOException {
        if (key.length > MAX_KEY_SIZE || value.length > MAX_VALUE_SIZE) {
            throw new IllegalArgumentException("Key or value exceeds maximum size");
        }
        InsertResult result = insertRecursive(getRootPageId(), key, value);
        if (result != null && result.newRightPage != -1) {
            long newRoot = allocateInternalPage();
            ByteBuffer rootBuf = getPage(newRoot);
            rootBuf.put(0, NODE_INTERNAL);
            rootBuf.putInt(1, 1);
            rootBuf.position(5);
            rootBuf.putLong(result.leftChildOfNewRoot); // leftmost child
            writeInternalEntry(rootBuf, rootBuf.position(), result.middleKey, result.newRightPage);
            writePage(newRoot, rootBuf);
            writeRootPageId(newRoot);
        }
    }

    private InsertResult insertRecursive(long pageId, byte[] key, byte[] value) throws IOException {
        ByteBuffer buf = getPage(pageId);
        byte type = buf.get(0);
        int count = buf.getInt(1);

        if (type == NODE_LEAF) {
            List<LeafEntry> entries = readLeafEntries(buf);
            int idx = findKeyIndex(entries, key);
            if (idx != -1) {
                // Use tail pointer for fast append
                LeafEntry entry = entries.get(idx);
                long newTail = appendToOverflow(entry.overflowTailPage, value);
                if (newTail != entry.overflowTailPage) {
                    // Update tail pointer if a new page was added
                    entry.overflowTailPage = newTail;
                    writeLeafEntries(buf, entries);
                    writePage(pageId, buf);
                }
                return null;
            }

            long overflow = allocateOverflow(value);
            // Find correct insert position using binary search (no sort needed)
            int insertPos = 0;
            int left = 0, right = entries.size() - 1;
            while (left <= right) {
                int mid = (left + right) >>> 1;
                int cmp = compareByteArrays(entries.get(mid).key, key);
                if (cmp < 0) {
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }
            insertPos = left;
            entries.add(insertPos, new LeafEntry(key, overflow, overflow));

            if (estimateLeafSize(entries) <= PAGE_SIZE - 5) {
                writeLeafEntries(buf, entries);
                writePage(pageId, buf);
                return null;
            } else {
                int splitIndex = entries.size() / 2;
                List<LeafEntry> leftList = new ArrayList<>(entries.subList(0, splitIndex));
                List<LeafEntry> rightList = new ArrayList<>(entries.subList(splitIndex, entries.size()));
                writeLeafEntries(buf, leftList);
                writePage(pageId, buf);

                long newRightPage = allocateLeafPage();
                ByteBuffer rightBuf = getPage(newRightPage);
                writeLeafEntries(rightBuf, rightList);
                writePage(newRightPage, rightBuf);

                return new InsertResult(rightList.get(0).key, newRightPage, pageId);
            }
        }

        if (type == NODE_INTERNAL) {
            long[] leftmostChild = new long[1];
            List<InternalEntry> entries = readInternalEntries(buf, leftmostChild);
            if (entries.isEmpty()) {
                throw new IllegalStateException("Internal node has no children");
            }
            int i = 0;
            for (; i < entries.size(); i++) {
                if (compareByteArrays(key, entries.get(i).key) < 0)
                    break;
            }

            long childPage = (i == 0) ? leftmostChild[0] : entries.get(i - 1).rightChild;
            InsertResult res = insertRecursive(childPage, key, value);
            if (res == null || res.newRightPage == -1)
                return null;

            // Insert new entry after split
            InternalEntry insert = new InternalEntry(res.middleKey, res.newRightPage);
            if (i == 0) {
                entries.add(0, insert);
                leftmostChild[0] = res.leftChildOfNewRoot;
            } else {
                entries.add(i, insert);
            }

            if (estimateInternalSize(entries) <= PAGE_SIZE - 5 - 8) {
                writeInternalEntries(buf, leftmostChild[0], entries);
                writePage(pageId, buf);
                return null;
            } else {
                int splitIndex = entries.size() / 2;
                byte[] promotedKey = entries.get(splitIndex).key;
                long promotedRightChild = entries.get(splitIndex).rightChild;

                List<InternalEntry> leftEntries = new ArrayList<>(entries.subList(0, splitIndex));
                List<InternalEntry> rightEntries = new ArrayList<>(entries.subList(splitIndex + 1, entries.size()));

                long leftLeftmost = leftmostChild[0];
                long rightLeftmost = promotedRightChild;

                writeInternalEntries(buf, leftLeftmost, leftEntries);
                writePage(pageId, buf);

                long newRight = allocateInternalPage();
                ByteBuffer rightBuf = getPage(newRight);
                writeInternalEntries(rightBuf, rightLeftmost, rightEntries);
                writePage(newRight, rightBuf);

                return new InsertResult(promotedKey, newRight, pageId);
            }
        }

        throw new IllegalStateException("Unknown page type");
    }

    private int findKeyIndex(List<LeafEntry> entries, byte[] key) {
        int left = 0, right = entries.size() - 1;
        while (left <= right) {
            int mid = (left + right) >>> 1;
            int cmp = compareByteArrays(entries.get(mid).key, key);
            if (cmp == 0)
                return mid;
            if (cmp < 0)
                left = mid + 1;
            else
                right = mid - 1;
        }
        return -1;
    }

    public Iterable<byte[]> search(byte[] key) {
        if (key.length > MAX_KEY_SIZE) {
            throw new IllegalArgumentException("Key exceeds maximum size");
        }
        return () -> new Iterator<byte[]>() {
            long[] overflowPages = findOverflow(getRootPageId(), key);
            long overflowPage = (overflowPages != null) ? overflowPages[0] : -1;
            long tailPage = (overflowPages != null) ? overflowPages[1] : -1;
            ByteBuffer buf = (overflowPage != -1) ? getPage(overflowPage) : null;
            int valueIndex = 0;
            int valueCount = 0;
            int[] valueOffsets = null;
            boolean valid = overflowPage != -1;

            private void loadOffsets() {
                if (buf == null)
                    return;
                valueCount = buf.getInt(1);
                valueOffsets = new int[valueCount];
                int pos = 5;
                for (int i = 0; i < valueCount; i++) {
                    valueOffsets[i] = pos;
                    buf.position(pos);
                    int len = buf.getInt();
                    pos = buf.position() + len;
                }
            }

            {
                if (valid)
                    loadOffsets();
            }

            @Override
            public boolean hasNext() {
                while (valid) {
                    if (valueIndex < valueCount) {
                        return true;
                    }
                    // Move to next overflow page if available
                    long next = buf.getLong(PAGE_SIZE - 8);
                    if (next == -1) {
                        valid = false;
                        return false;
                    }
                    buf = getPage(next);
                    valueIndex = 0;
                    loadOffsets();
                }
                return false;
            }

            @Override
            public byte[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                buf.position(valueOffsets[valueIndex]);
                int len = buf.getInt();
                byte[] val = new byte[len];
                buf.get(val);
                valueIndex++;
                return val;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove not supported");
            }
        };
    }

    // Returns [head, tail] or null if not found
    private long[] findOverflow(long pageId, byte[] key) {
        ByteBuffer buf = getPage(pageId);
        byte type = buf.get(0);

        if (type == NODE_LEAF) {
            List<LeafEntry> entries = readLeafEntries(buf);
            int idx = findKeyIndex(entries, key);
            if (idx != -1) {
                LeafEntry e = entries.get(idx);
                return new long[] { e.overflowHeadPage, e.overflowTailPage };
            }
            return null;
        }

        if (type == NODE_INTERNAL) {
            long[] leftmostChild = new long[1];
            List<InternalEntry> entries = readInternalEntries(buf, leftmostChild);
            if (entries.isEmpty()) {
                throw new IllegalStateException("Internal node has no children");
            }
            for (InternalEntry e : entries) {
                if (compareByteArrays(key, e.key) < 0)
                    return findOverflow(leftmostChild[0], key);
                leftmostChild[0] = e.rightChild;
            }
            return findOverflow(leftmostChild[0], key);
        }

        throw new IllegalStateException("Unknown page type");
    }

    // --- Data Structures

    static class LeafEntry {
        byte[] key;
        long overflowHeadPage;
        long overflowTailPage;

        LeafEntry(byte[] key, long head, long tail) {
            this.key = key;
            this.overflowHeadPage = head;
            this.overflowTailPage = tail;
        }
    }

    static class InternalEntry {
        byte[] key;
        long rightChild;

        InternalEntry(byte[] key, long rightChild) {
            this.key = key;
            this.rightChild = rightChild;
        }
    }

    static class InsertResult {
        byte[] middleKey;
        long newRightPage;
        long leftChildOfNewRoot;

        InsertResult(byte[] key, long page, long leftChild) {
            this.middleKey = key;
            this.newRightPage = page;
            this.leftChildOfNewRoot = leftChild;
        }
    }

    static class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
        byte[] data;

        ByteArrayWrapper(byte[] d) {
            data = d;
        }

        public int compareTo(ByteArrayWrapper o) {
            return compareByteArrays(data, o.data);
        }
    }

    // --- Page Access + Serialization

    private ByteBuffer getPage(long pageId) {
        long offset = pageId * PAGE_SIZE;
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        try {
            channel.read(buf, offset);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buf.flip();
        return buf;
    }

    private void writePage(long pageId, ByteBuffer buf) {
        long offset = pageId * PAGE_SIZE;
        buf.position(0);
        buf.limit(PAGE_SIZE);
        try {
            channel.write(buf, offset);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long getRootPageId() {
        ByteBuffer buf = ByteBuffer.allocate(8);
        try {
            channel.read(buf, 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buf.flip();
        return buf.getLong();
    }

    private void writeRootPageId(long pageId) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(pageId);
        buf.flip();
        try {
            channel.write(buf, 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long allocateLeafPage() {
        long id = nextPageId++;
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        buf.put(0, NODE_LEAF);
        buf.putInt(1, 0);
        writePage(id, buf);
        return id;
    }

    private long allocateInternalPage() {
        long id = nextPageId++;
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        buf.put(0, NODE_INTERNAL);
        buf.putInt(1, 0);
        writePage(id, buf);
        return id;
    }

    private long allocateOverflow(byte[] value) {
        long pageId = nextPageId++;
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        buf.put(0, NODE_OVERFLOW);
        buf.putInt(1, 1); // value count
        buf.position(5);
        buf.putInt(value.length);
        buf.put(value);
        buf.position(PAGE_SIZE - 8);
        buf.putLong(-1L);
        writePage(pageId, buf);
        return pageId;
    }

    // Returns new tail page id (may be same as input if no new page allocated)
    private long appendToOverflow(long tailPageId, byte[] value) {
        ByteBuffer buf = getPage(tailPageId);
        int count = buf.getInt(1);
        int pos = 5;
        for (int i = 0; i < count; i++) {
            buf.position(pos);
            int len = buf.getInt();
            pos = buf.position() + len;
        }
        int spaceNeeded = 4 + value.length;
        if (pos + spaceNeeded + 8 <= PAGE_SIZE) { // 8 bytes for next pointer
            buf.position(pos);
            buf.putInt(value.length);
            buf.put(value);
            buf.putInt(1, count + 1);
            writePage(tailPageId, buf);
            return tailPageId;
        } else {
            long newPage = nextPageId++;
            ByteBuffer newBuf = ByteBuffer.allocate(PAGE_SIZE);
            newBuf.put(0, NODE_OVERFLOW);
            newBuf.putInt(1, 1); // value count
            newBuf.position(5);
            newBuf.putInt(value.length);
            newBuf.put(value);
            newBuf.position(PAGE_SIZE - 8);
            newBuf.putLong(-1L);
            writePage(newPage, newBuf);
            buf.position(PAGE_SIZE - 8);
            buf.putLong(newPage);
            writePage(tailPageId, buf);
            return newPage;
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
            long head = buf.getLong();
            long tail = buf.getLong();
            entries.add(new LeafEntry(key, head, tail));
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
            buf.putLong(e.overflowHeadPage);
            buf.putLong(e.overflowTailPage);
        }
    }

    private List<InternalEntry> readInternalEntries(ByteBuffer buf, long[] leftmostChild) {
        int count = buf.getInt(1);
        int pos = 5;
        leftmostChild[0] = buf.getLong(pos);
        pos += 8;
        List<InternalEntry> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            buf.position(pos);
            int klen = buf.getInt();
            byte[] key = new byte[klen];
            buf.get(key);
            long right = buf.getLong();
            entries.add(new InternalEntry(key, right));
            pos = buf.position();
        }
        return entries;
    }

    private void writeInternalEntries(ByteBuffer buf, long leftmostChild, List<InternalEntry> entries) {
        buf.clear();
        buf.put(0, NODE_INTERNAL);
        buf.putInt(1, entries.size());
        buf.position(5);
        buf.putLong(leftmostChild);
        for (InternalEntry e : entries) {
            buf.putInt(e.key.length);
            buf.put(e.key);
            buf.putLong(e.rightChild);
        }
    }

    private void writeInternalEntry(ByteBuffer buf, int pos, byte[] key, long right) {
        buf.position(pos);
        buf.putInt(key.length);
        buf.put(key);
        buf.putLong(right);
    }

    private int estimateLeafSize(List<LeafEntry> entries) {
        // 4 bytes key len + key + 8 bytes head + 8 bytes tail per entry
        return 5 + entries.stream().mapToInt(e -> 4 + e.key.length + 8 + 8).sum();
    }

    private int estimateInternalSize(List<InternalEntry> entries) {
        // 5 bytes header + 8 bytes leftmostChild + per-entry
        return 5 + 8 + entries.stream().mapToInt(e -> 4 + e.key.length + 8).sum();
    }

    public static int compareByteArrays(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int byteA = a[i] & 0xFF;
            int byteB = b[i] & 0xFF;
            if (byteA != byteB) {
                return byteA - byteB;
            }
        }
        return a.length - b.length;
    }
}
