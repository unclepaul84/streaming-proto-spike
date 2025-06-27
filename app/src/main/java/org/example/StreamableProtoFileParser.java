package org.example;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.function.Function;

import com.google.protobuf.InvalidProtocolBufferException;

public class StreamableProtoFileParser<H,P> {

    public static final int MAGIC_BYTE = 0x1973;
    public static final int FILE_SEAL_MARKER = -1;

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

        

        private boolean sealReached = false;    
        private boolean randomAccessMode = false;

        private final Function<byte[],P> protoFactory;
        private final DataInputStream fi;
        private H header;
        private StreamablePayloadEnumerator(DataInputStream fi, Function<byte[],H> headerFactory, Function<byte[],P> protoFactory) throws IOException, InvalidProtocolBufferException {
            
            this.protoFactory = protoFactory;
            this.fi = fi;
            fi.mark(Integer.MAX_VALUE);
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

        public P GetPayloadAtOffset(long offset) throws IOException, InvalidProtocolBufferException {
            if (offset < 0) {
                throw new IllegalArgumentException("Offset cannot be negative");
            }
            if (offset > fi.available()) {
                throw new IllegalArgumentException("Offset is beyond the end of the file");
            }
            this.sealReached = false;
            this.randomAccessMode = true;  
            fi.reset();
          
            fi.skipNBytes(offset);

              if (this.fi.available() > 0) {
                
                int length = this.fi.readInt();

                if (length == StreamableProtoFileParser.FILE_SEAL_MARKER) {
                    
                    return null;
                }

                byte[] data = new byte[length];
                this.fi.read(data);
                return protoFactory.apply(data);
            }

            return null;
        }




        public P GetNextPayload() throws IOException,InvalidProtocolBufferException {
            if(randomAccessMode) {
                throw new IllegalStateException("Cannot call GetNextPayload in random access mode. After first call to GetPayloadAtOffset() reader enters random access mode. Must start with a fresh instance of the parser to get back to sequential mode.");
            }
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
            }
            else if (!sealReached) {
                throw new IOException("This file was not properly sealed! This suggests that the file was not fully written.");
            }
            return null;
        }

        public void close() throws Exception {
            fi.close();
        }
       
    }




    
}
