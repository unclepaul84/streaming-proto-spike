package org.example;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.util.function.Function;

import com.google.protobuf.InvalidProtocolBufferException;

public class StreamableProtoFileParser<H, P> {

    public static final int MAGIC_BYTE = 0x1973;
    public static final int FILE_SEAL_MARKER = -1;

    private final Function<byte[], H> headerFactory;
    private final Function<byte[], P> protoFactory;
    private final String file;

    public StreamableProtoFileParser(String file, Function<byte[], H> headerFactory, Function<byte[], P> protoFactory)
            throws com.google.protobuf.InvalidProtocolBufferException, IllegalArgumentException {
        if (file == null) {
            throw new IllegalArgumentException("file cannot be null");
        }
        if (headerFactory == null) {
            throw new IllegalArgumentException("headerFactory cannot be null");
        }
        if (protoFactory == null) {
            throw new IllegalArgumentException("protoFactory cannot be null");
        }
        this.headerFactory = headerFactory;
        this.protoFactory = protoFactory;
        this.file = file;

    }

    public StreamablePayloadEnumerator GetPayloadEnumerator() throws IOException {
        var fi = new DataInputStream(new BufferedInputStream(new FileInputStream(this.file), 64 * 1024));
        return new StreamablePayloadEnumerator(fi, this.headerFactory, this.protoFactory);
    }

    public StreamablePayloadRandomAccesor GetPayloadRandomAccesor() throws IOException, InvalidProtocolBufferException {
        var raf = new RandomAccessFile(this.file, "r");
        return new StreamablePayloadRandomAccesor(raf, this.headerFactory, this.protoFactory);
    }
    public class StreamablePayloadRandomAccesor implements AutoCloseable {

        private final RandomAccessFile raf;
        private H header;
        private final Function<byte[], P> protoFactory;

        private StreamablePayloadRandomAccesor(RandomAccessFile raf, Function<byte[], H> headerFactory,
                Function<byte[], P> protoFactory) throws IOException, InvalidProtocolBufferException {
            this.protoFactory = protoFactory;
            this.raf = raf;
            int magicByte = raf.readInt();

            if (magicByte != StreamableProtoFileParser.MAGIC_BYTE) {
                throw new IOException("Invalid magic byte");
            }

            int headerLength = raf.readInt();

            byte[] headerBytes = new byte[headerLength];

            raf.read(headerBytes);

            header = headerFactory.apply(headerBytes);
        }

        public P GetPayloadAtOffset(long offset) throws IOException, InvalidProtocolBufferException {
            if (offset <= 0) {
                throw new IllegalArgumentException("Offset cannot be negative");
            }

            raf.seek(offset);
            int length = this.raf.readInt();

            if (length == StreamableProtoFileParser.FILE_SEAL_MARKER) {

                return null;
            }

            byte[] data = new byte[length];
            this.raf.read(data);
            return protoFactory.apply(data);
        }

        public H GetHeader() {
            return header;
        }

        public void close() throws Exception {
            this.raf.close();
        }
    }

    public class StreamablePayloadEnumerator implements AutoCloseable {

        private boolean sealReached = false;

        private final Function<byte[], P> protoFactory;
        private final DataInputStream fi;
        private H header;

        private StreamablePayloadEnumerator(DataInputStream fi, Function<byte[], H> headerFactory,
                Function<byte[], P> protoFactory) throws IOException, InvalidProtocolBufferException {

            this.protoFactory = protoFactory;
            this.fi = fi;

            int magicByte = fi.readInt();

            if (magicByte != StreamableProtoFileParser.MAGIC_BYTE) {
                throw new IOException("Invalid magic byte");
            }

            int headerLength = fi.readInt();

            byte[] headerBytes = new byte[headerLength];

            fi.read(headerBytes);

            header = headerFactory.apply(headerBytes);
        }

        public H GetHeader() {
            return header;
        }

        public P GetNextPayload() throws IOException, InvalidProtocolBufferException {

            if (sealReached) {
                return null;
            }
            if (this.fi.available() > 0) {

                int length = this.fi.readInt();

                if (length == StreamableProtoFileParser.FILE_SEAL_MARKER) {
                    sealReached = true;
                    return null;
                }

                byte[] data = new byte[length];
                this.fi.read(data);
                return protoFactory.apply(data);
            } else if (!sealReached) {
                throw new IOException(
                        "This file was not properly sealed! This suggests that the file was not fully written.");
            }
            return null;
        }

        public void close() throws Exception {
            fi.close();
        }

    }

}
