package org.example;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.function.Function;

import com.google.protobuf.InvalidProtocolBufferException;

public class StreamableProtoFileParser<H,P> {

    public static final int MAGIC_BYTE = 0x1973;

    private final Function<byte[],H> headerFactory;
    private final Function<byte[],P> protoFactory;
    private final String file;

    public StreamableProtoFileParser(String file,  Function<byte[],H> headerFactory, Function<byte[],P> protoFactory) throws com.google.protobuf.InvalidProtocolBufferException,IllegalArgumentException  {
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
        var fi = new DataInputStream( new BufferedInputStream( new FileInputStream(this.file), 64 * 1024));
        return new StreamablePayloadEnumerator(fi, this.headerFactory, this.protoFactory);
    }

    public class StreamablePayloadEnumerator implements AutoCloseable {

        

       
        private final Function<byte[],P> protoFactory;
        private final DataInputStream fi;
        private H header;
        private StreamablePayloadEnumerator(DataInputStream fi, Function<byte[],H> headerFactory, Function<byte[],P> protoFactory) throws IOException, InvalidProtocolBufferException {
            
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

        public P GetNextPayload() throws IOException,InvalidProtocolBufferException {
            if (this.fi.available() > 0) {
                int length = this.fi.readInt();
                byte[] data = new byte[length];
                this.fi.read(data);
                return protoFactory.apply(data);
            }
            return null;
        }

        public void close() throws Exception {
            fi.close();
        }
       
    }




    
}
