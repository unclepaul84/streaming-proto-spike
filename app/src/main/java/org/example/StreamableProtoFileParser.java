package org.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.function.Function;

import com.google.protobuf.InvalidProtocolBufferException;

public class StreamableProtoFileParser<H,P> {

    public static final int MAGIC_BYTE = 0x1973;

    private final Function<byte[],H> headerFactory;
    private final Function<byte[],P> protoFactory;
    private final String file;

    public StreamableProtoFileParser(String file,  Function<byte[],H> headerFactory, Function<byte[],P> protoFactory) throws com.google.protobuf.InvalidProtocolBufferException  {
        
        this.headerFactory = headerFactory;
        this.protoFactory = protoFactory;
        this.file = file;

    }

    public StreamablePayloadEnumerator<H,P> GetPayloadEnumerator() throws IOException {
        FileInputStream fi = new FileInputStream(this.file);
        return new StreamablePayloadEnumerator<H,P>(fi, this.headerFactory, this.protoFactory);
    }

    public class StreamablePayloadEnumerator<H,P> implements AutoCloseable {

        

        private final Function<byte[],H> headerFactory;
        private final Function<byte[],P> protoFactory;
        private final FileInputStream fi;
        private H header;
        private StreamablePayloadEnumerator(FileInputStream fi, Function<byte[],H> headerFactory, Function<byte[],P> protoFactory) throws IOException, InvalidProtocolBufferException {
            this.headerFactory = headerFactory;
            this.protoFactory = protoFactory;
            this.fi = fi;

            int magicByte = fi.read();

            if (magicByte != 0x1973) {
                throw new IOException("Invalid magic byte");
            }

            int headerLength = fi.read();

            byte[] headerBytes = new byte[headerLength];
            
            fi.read(headerBytes);
            
            header = headerFactory.apply(headerBytes);
        }

        public H GetHeader() {
            return header;
        }

        public P GetNextPayload() throws IOException,InvalidProtocolBufferException {
            if (this.fi.available() > 0) {
                int length = this.fi.read();
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
