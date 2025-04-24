package org.example;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.*;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.function.Function;

public class MMappedProtoHashMap<P extends com.google.protobuf.GeneratedMessageV3>
        implements AutoCloseable {
    private final int SEGMENT_COUNT;
    private final int SEGMENT_ENTRY_SIZE = 24;
    private final int SEGMENT_MAX_ENTRIES;
    private final int CACHE_SIZE;
    private final int bufferCacheSize;

    public MMappedProtoHashMap(Path mapDir, Function<byte[], P> protoFactory,
                               int segmentCount,  int segmentMaxEntries, int cacheSize, int bufferCacheSizeBytes) throws IOException, com.google.protobuf.InvalidProtocolBufferException {
        this.SEGMENT_COUNT = segmentCount;   
        this.SEGMENT_MAX_ENTRIES = segmentMaxEntries;
        this.CACHE_SIZE = cacheSize;
        this.protoFactory = protoFactory;
        this.indexSegments = new MappedByteBuffer[SEGMENT_COUNT];
        this.bufferCacheSize = bufferCacheSizeBytes;
        
        this.threadReadBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(bufferCacheSize));
        this.threadWriteBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(bufferCacheSize));


        this.cache = Collections.synchronizedMap(
            new LinkedHashMap<String, P>(CACHE_SIZE, 0.75f, true) {
                protected boolean removeEldestEntry(Map.Entry<String, P> eldest) {
                    return size() > CACHE_SIZE;
                }
            }
        );

        initIndexSegments(mapDir);
        Path valuePath = mapDir.resolve("values.dat");
        valueChannel = FileChannel.open(valuePath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    public MMappedProtoHashMap(Path mapDir, Function<byte[], P> protoFactory) throws IOException, com.google.protobuf.InvalidProtocolBufferException {
        this(mapDir, protoFactory, 1, 3000000, 1, 64 * 1024);
    }

    private final MappedByteBuffer[] indexSegments;
    private final FileChannel valueChannel;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final ThreadLocal<ByteBuffer> threadReadBuffer;  
    private final ThreadLocal<ByteBuffer> threadWriteBuffer;
    private final Function<byte[],P> protoFactory;

    // LRU cache for deserialized values
    private final Map<String, P> cache; 



    private void initIndexSegments(Path shardDir) throws IOException {
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String segmentFile = String.format("index-segment-%02d.idx", i);
            Path segmentPath = shardDir.resolve(segmentFile);
            RandomAccessFile raf = new RandomAccessFile(segmentPath.toFile(), "rw");
            raf.setLength((long) SEGMENT_ENTRY_SIZE * SEGMENT_MAX_ENTRIES);
            indexSegments[i] = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, raf.length());
        }
    }

    private int hash(byte[] key) {
        return Arrays.hashCode(key);
    }

    private int segmentForHash(int hash) {
        return (hash >>> 24) % SEGMENT_COUNT;
    }

    public void put(byte[] key, P value) throws IOException {
        lock.writeLock().lock();
        try {
            int hash = hash(key);
            int segmentIndex = segmentForHash(hash);
            MappedByteBuffer segment = indexSegments[segmentIndex];

            long slotOffset = findSlotOffset(segment, hash, key, true);

            byte[] valueBytes = value.toByteArray();
            long valOffset = valueChannel.size();

            ByteBuffer buf = threadWriteBuffer.get();
            int totalLen = 4 + key.length + 4 + valueBytes.length;
            if (buf.capacity() < totalLen) {
                buf = ByteBuffer.allocate(totalLen);
                threadWriteBuffer.set(buf);
            } else {
                buf.clear();
            }

            buf.putInt(key.length).put(key);
            buf.putInt(valueBytes.length).put(valueBytes);
            buf.flip();
            valueChannel.write(buf);

            segment.position((int) slotOffset);
            segment.putLong(hash);
            segment.putLong(valOffset);
            segment.putInt(totalLen);
            segment.put((byte) 1); // valid

            // cache update
            cache.put(Base64.getEncoder().encodeToString(key), value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public P get(byte[] key) throws IOException {
        String cacheKey = Base64.getEncoder().encodeToString(key);
        P cached = cache.get(cacheKey);
        if (cached != null) return cached;

        lock.readLock().lock();
        try {
            int hash = hash(key);
            int segmentIndex = segmentForHash(hash);
            MappedByteBuffer segment = indexSegments[segmentIndex];

            long slotOffset = findSlotOffset(segment, hash, key, false);

            segment.position((int) slotOffset);
            long storedHash = segment.getLong();
            long valOffset = segment.getLong();
            int len = segment.getInt();
            byte flags = segment.get();

            if (flags == 0 || storedHash != hash) return null;

            ByteBuffer buf = threadReadBuffer.get();
            if (buf.capacity() < len) {
                buf = ByteBuffer.allocate(len);
                threadReadBuffer.set(buf);
            } else {
                buf.clear();
            }

            buf.limit(len);
            valueChannel.read(buf, valOffset);
            buf.flip();

            int keyLen = buf.getInt();
            byte[] storedKey = new byte[keyLen];
            buf.get(storedKey);

            if (!Arrays.equals(storedKey, key)) return null;

            int valueLen = buf.getInt();
            byte[] valBytes = new byte[valueLen];
            buf.get(valBytes);
            P value = this.protoFactory.apply(valBytes);

            cache.put(cacheKey, value);
            return value;
        } finally {
            lock.readLock().unlock();
        }
    }

    private long findSlotOffset(MappedByteBuffer segment, int hash, byte[] key, boolean forWrite) {
        long start = (hash & 0x7fffffffL) % SEGMENT_MAX_ENTRIES;
        for (long i = 0; i < SEGMENT_MAX_ENTRIES; i++) {
            long idx = (start + i) % SEGMENT_MAX_ENTRIES;
            long offset = idx * SEGMENT_ENTRY_SIZE;

            segment.position((int) offset);
            long storedHash = segment.getLong();
            long valOffset = segment.getLong();
            int len = segment.getInt();
            byte flags = segment.get();

            if (flags == 0) {
                return offset; // unused slot
            }

            if (storedHash == hash) {
                try {
                    ByteBuffer buf = threadReadBuffer.get();
                    if (buf.capacity() < len) {
                        buf = ByteBuffer.allocate(len);
                        threadReadBuffer.set(buf);
                    } else {
                        buf.clear();
                    }
                    buf.limit(len);
                    valueChannel.read(buf, valOffset);
                    buf.flip();

                    int keyLen = buf.getInt();
                    byte[] storedKey = new byte[keyLen];
                    buf.get(storedKey);
                    if (Arrays.equals(storedKey, key)) {
                        return offset; // key match
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException("Segment full");
    }

    public void close() throws IOException {
        valueChannel.close();
    }
} // end of Shard class
