package org.example;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.*;
import java.util.function.Function;
import java.nio.ByteBuffer;

public class S3StreamableProtoFileParser<H, P> {

    private final Function<byte[], H> headerFactory;
    private final Function<byte[], P> protoFactory;
    private final String bucket;
    private final String key;
    private final S3Client s3;

    public S3StreamableProtoFileParser(
            S3Client s3,
            String bucket,
            String key,
            Function<byte[], H> headerFactory,
            Function<byte[], P> protoFactory) {
        if (bucket == null || key == null)
            throw new IllegalArgumentException("bucket/key cannot be null");
        if (headerFactory == null)
            throw new IllegalArgumentException("headerFactory cannot be null");
        if (protoFactory == null)
            throw new IllegalArgumentException("protoFactory cannot be null");
        this.s3 = s3;
        this.bucket = bucket;
        this.key = key;
        this.headerFactory = headerFactory;
        this.protoFactory = protoFactory;
    }

    public StreamablePayloadRandomAccesor GetPayloadRandomAccesor() throws IOException, InvalidProtocolBufferException {
        return new StreamablePayloadRandomAccesor(s3, bucket, key, headerFactory, protoFactory);
    }

    public class StreamablePayloadRandomAccesor implements AutoCloseable {
        private final S3Client s3;
        private final String bucket;
        private final String key;
        private final Function<byte[], H> headerFactory;
        private final Function<byte[], P> protoFactory;
        private H header;
        private long headerEndOffset;

        public StreamablePayloadRandomAccesor(
                S3Client s3,
                String bucket,
                String key,
                Function<byte[], H> headerFactory,
                Function<byte[], P> protoFactory) throws IOException, InvalidProtocolBufferException {
            this.s3 = s3;
            this.bucket = bucket;
            this.key = key;
            this.headerFactory = headerFactory;
            this.protoFactory = protoFactory;
            // Read first 8 bytes (magic + header length)
            byte[] meta = s3RangeRead(0, 7);
            ByteBuffer metaBuf = ByteBuffer.wrap(meta);
            int magic = metaBuf.getInt();
            if (magic != StreamableProtoFileParser.MAGIC_BYTE)
                throw new IOException("Invalid magic byte");
            int headerLen = metaBuf.getInt();
            // Read header bytes
            byte[] headerBytes = s3RangeRead(8, 8 + headerLen - 1);
            this.header = headerFactory.apply(headerBytes);
            this.headerEndOffset = 8 + headerLen;
        }

        public P GetPayloadAtOffset(long offset) throws IOException, InvalidProtocolBufferException {
            if (offset <= 0)
                throw new IllegalArgumentException("Offset cannot be negative");
            // Read 4 bytes for length
            byte[] lenBytes = s3RangeRead(offset, offset + 3);
            int length = ByteBuffer.wrap(lenBytes).getInt();
            if (length == StreamableProtoFileParser.FILE_SEAL_MARKER)
                return null;
            // Read payload
            byte[] data = s3RangeRead(offset + 4, offset + 3 + length);
            return protoFactory.apply(data);
        }

        public H GetHeader() {
            return header;
        }

        private byte[] s3RangeRead(long start, long end) throws IOException {
            GetObjectRequest req = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .range("bytes=" + start + "-" + end)
                    .build();
            try (ResponseInputStream<GetObjectResponse> in = s3.getObject(req)) {
                byte[] buf = new byte[(int) (end - start + 1)];
                int totalRead = 0;
                while (totalRead < buf.length) {
                    int read = in.read(buf, totalRead, buf.length - totalRead);
                    if (read == -1) {
                        throw new IOException("Could not read full range");
                    }
                    totalRead += read;
                }
                return buf;
            }
        }

        public void close() {
        }
    }

}