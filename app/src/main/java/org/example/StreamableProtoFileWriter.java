package org.example;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class StreamableProtoFileWriter<H extends com.google.protobuf.GeneratedMessageV3, P extends com.google.protobuf.GeneratedMessageV3>
        implements AutoCloseable {

    private final DataOutputStream fi;
    private boolean sealed = false;

    public StreamableProtoFileWriter(String file, H header)
            throws FileNotFoundException, IOException, IllegalArgumentException {

        if (file == null) {
            throw new IllegalArgumentException("File path cannot be null");
        }

        if (header == null) {
            throw new IllegalArgumentException("Header cannot be null");
        }
        if (!file.endsWith(".binpb")) {
            throw new IllegalArgumentException("File must end with .binpb");
        }
        this.fi = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file), 64 * 1024));

        this.fi.writeInt(StreamableProtoFileParser.MAGIC_BYTE);

        byte[] headerBytes = header.toByteArray();

        this.fi.writeInt(headerBytes.length);
        this.fi.write(headerBytes);

    }

    public int Write(P payload) throws IOException {

        if (sealed) {
            throw new IOException("File is sealed");
        }

        byte[] payloadBytes = payload.toByteArray();
        this.fi.writeInt(payloadBytes.length);
        this.fi.write(payloadBytes);

        return payloadBytes.length;
    }

    // Seal the file
    public void Seal() throws IOException {
        if (!sealed) {
            this.fi.writeInt(StreamableProtoFileParser.FILE_SEAL_MARKER);
            sealed = true;
        }
    }

    public void close() throws Exception {

        Seal();

        fi.flush();
        fi.close();
    }
}
