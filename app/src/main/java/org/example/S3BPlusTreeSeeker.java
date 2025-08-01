package org.example;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class S3BPlusTreeSeeker implements AutoCloseable {
    static final int PAGE_SIZE = OnDiskBPlusTree.PAGE_SIZE;
    static final int MAX_KEY_SIZE = OnDiskBPlusTree.MAX_KEY_SIZE;
    static final byte NODE_LEAF = OnDiskBPlusTree.NODE_LEAF;
    static final byte NODE_INTERNAL = OnDiskBPlusTree.NODE_INTERNAL;
    static final byte NODE_OVERFLOW = OnDiskBPlusTree.NODE_OVERFLOW;

    private final S3Client s3;
    private final String bucket;
    private final String key;
    private final long fileSize;

    public S3BPlusTreeSeeker(S3Client s3, String bucket, String key) {
        this.s3 = s3;
        this.bucket = bucket;
        this.key = key;
        this.fileSize = fetchFileSize();
    }

    private long fetchFileSize() {
        HeadObjectRequest req = HeadObjectRequest.builder().bucket(bucket).key(key).build();
        return s3.headObject(req).contentLength();
    }

    @Override
    public void close() {
        s3.close();
    }

    private ByteBuffer getPage(long pageId) {
        long offset = pageId * PAGE_SIZE;
        if (offset >= fileSize) throw new IllegalArgumentException("Page out of bounds");
        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range("bytes=" + offset + "-" + (offset + PAGE_SIZE - 1))
                .build();
        try (ResponseInputStream<GetObjectResponse> in = s3.getObject(req)) {
            byte[] buf = new byte[PAGE_SIZE];
            int read = in.readNBytes(buf, 0, PAGE_SIZE);
            if (read < PAGE_SIZE) throw new IOException("Short read");
            return ByteBuffer.wrap(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long getRootPageId() {
        // Root page id is stored at offset 0 (first 8 bytes)
        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range("bytes=0-7")
                .build();
        try (ResponseInputStream<GetObjectResponse> in = s3.getObject(req)) {
            byte[] buf = new byte[8];
            int read = in.readNBytes(buf, 0, 8);
            if (read < 8) throw new IOException("Short read");
            ByteBuffer bb = ByteBuffer.wrap(buf);
            return bb.getLong();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterable<byte[]> search(byte[] key) {
        if (key.length > MAX_KEY_SIZE) throw new IllegalArgumentException("Key too large");
        return () -> new Iterator<byte[]>() {
            long[] overflowPages = findOverflow(getRootPageId(), key);
            long overflowPage = (overflowPages != null) ? overflowPages[0] : -1;
            ByteBuffer buf = (overflowPage != -1) ? getPage(overflowPage) : null;
            int valueIndex = 0;
            int valueCount = 0;
            int[] valueOffsets = null;
            boolean valid = overflowPage != -1;

            private void loadOffsets() {
                if (buf == null) return;
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
                if (valid) loadOffsets();
            }

            @Override
            public boolean hasNext() {
                while (valid) {
                    if (valueIndex < valueCount) return true;
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
                if (!hasNext()) throw new NoSuchElementException();
                buf.position(valueOffsets[valueIndex]);
                int len = buf.getInt();
                byte[] val = new byte[len];
                buf.get(val);
                valueIndex++;
                return val;
            }
        };
    }

    // Returns [head, tail] or null if not found
    private long[] findOverflow(long pageId, byte[] key) {
        ByteBuffer buf = getPage(pageId);
        byte type = buf.get(0);

        if (type == NODE_LEAF) {
            List<OnDiskBPlusTree.LeafEntry> entries = readLeafEntries(buf);
            int idx = findKeyIndex(entries, key);
            if (idx != -1) {
                OnDiskBPlusTree.LeafEntry e = entries.get(idx);
                return new long[]{e.overflowHeadPage, e.overflowTailPage};
            }
            return null;
        }

        if (type == NODE_INTERNAL) {
            long[] leftmostChild = new long[1];
            List<OnDiskBPlusTree.InternalEntry> entries = readInternalEntries(buf, leftmostChild);
            if (entries.isEmpty()) throw new IllegalStateException("Internal node has no children");
            for (OnDiskBPlusTree.InternalEntry e : entries) {
                if (OnDiskBPlusTree.compareByteArrays(key, e.key) < 0)
                    return findOverflow(leftmostChild[0], key);
                leftmostChild[0] = e.rightChild;
            }
            return findOverflow(leftmostChild[0], key);
        }

        throw new IllegalStateException("Unknown page type");
    }

    private int findKeyIndex(List<OnDiskBPlusTree.LeafEntry> entries, byte[] key) {
        int left = 0, right = entries.size() - 1;
        while (left <= right) {
            int mid = (left + right) >>> 1;
            int cmp = OnDiskBPlusTree.compareByteArrays(entries.get(mid).key, key);
            if (cmp == 0) return mid;
            if (cmp < 0) left = mid + 1;
            else right = mid - 1;
        }
        return -1;
    }

    private List<OnDiskBPlusTree.LeafEntry> readLeafEntries(ByteBuffer buf) {
        int count = buf.getInt(1);
        List<OnDiskBPlusTree.LeafEntry> entries = new ArrayList<>();
        int pos = 5;
        for (int i = 0; i < count; i++) {
            buf.position(pos);
            int klen = buf.getInt();
            byte[] key = new byte[klen];
            buf.get(key);
            long head = buf.getLong();
            long tail = buf.getLong();
            entries.add(new OnDiskBPlusTree.LeafEntry(key, head, tail));
            pos = buf.position();
        }
        return entries;
    }

    private List<OnDiskBPlusTree.InternalEntry> readInternalEntries(ByteBuffer buf, long[] leftmostChild) {
        int count = buf.getInt(1);
        int pos = 5;
        leftmostChild[0] = buf.getLong(pos);
        pos += 8;
        List<OnDiskBPlusTree.InternalEntry> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            buf.position(pos);
            int klen = buf.getInt();
            byte[] key = new byte[klen];
            buf.get(key);
            long right = buf.getLong();
            entries.add(new OnDiskBPlusTree.InternalEntry(key, right));
            pos = buf.position();
        }
        return entries;
    }
}