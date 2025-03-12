package org.example;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;

public class DescriptorBasedStreamableProtoFileParser {

    public static final int MAGIC_BYTE = 0x1973;
    public static final int FILE_SEAL_MARKER = -1;


    private final String file;
    private final Descriptor headerDescriptor;
    private final Descriptor payloadDescriptor;

    public DescriptorBasedStreamableProtoFileParser(String file, Descriptor headerDescriptor, Descriptor payloadDescriptor) throws com.google.protobuf.InvalidProtocolBufferException,IllegalArgumentException  {
        if (file == null) {
            throw new IllegalArgumentException("file cannot be null");
        }
        if (headerDescriptor == null) {
            throw new IllegalArgumentException("headerDescriptor cannot be null");
        }
        if (payloadDescriptor == null) {
            throw new IllegalArgumentException("payloadDescriptor cannot be null");
        }
        this.headerDescriptor = headerDescriptor;
        this.payloadDescriptor = payloadDescriptor;
        this.file = file;

    }

    public StreamablePayloadEnumerator GetPayloadEnumerator() throws IOException {
        var fi = new DataInputStream( new BufferedInputStream( new FileInputStream(this.file), 64 * 1024));
        return new StreamablePayloadEnumerator(fi, this.headerDescriptor, this.payloadDescriptor);
    }

    public class StreamablePayloadEnumerator implements AutoCloseable {

        private boolean sealReached = false;    

        private final DataInputStream fi;
        private DynamicMessage header;
        private Descriptor payloadDescriptor;
        private StreamablePayloadEnumerator(DataInputStream fi, Descriptor headerDescriptor, Descriptor payloadDescriptor) throws IOException, InvalidProtocolBufferException {
            
            this.payloadDescriptor = payloadDescriptor;
            this.fi = fi;

            int magicByte = fi.readInt();

            if (magicByte != StreamableProtoFileParser.MAGIC_BYTE) {
                throw new IOException("Invalid magic byte");
            }

            int headerLength = fi.readInt();

            byte[] headerBytes = new byte[headerLength];
            
            fi.read(headerBytes);
            
            this.header  = DynamicMessage.parseFrom(headerDescriptor, headerBytes);

        }

        public DynamicMessage GetHeader() {
            return header;
        }

        public DynamicMessage GetNextPayload() throws IOException,InvalidProtocolBufferException {
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
                return  DynamicMessage.parseFrom(this.payloadDescriptor, data);
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
