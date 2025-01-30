package org.example;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class StreamableProtoFileWriter<H extends com.google.protobuf.GeneratedMessageV3, P extends com.google.protobuf.GeneratedMessageV3>
        implements AutoCloseable {

    private final FileOutputStream fi;

    public StreamableProtoFileWriter(String file, H header) throws FileNotFoundException, IOException {

        this.fi = new FileOutputStream(file);
        
        this.fi.write(((int)0x1973));
        byte[] headerBytes = header.toByteArray();

        this.fi.write(headerBytes.length);
        this.fi.write(headerBytes);

    }

    public void Write(P payload) throws IOException {
        byte[] payloadBytes = payload.toByteArray();
        this.fi.write(payloadBytes.length);
        this.fi.write(payloadBytes);
    }

    public void close() throws Exception {
        fi.close();
    }
}
