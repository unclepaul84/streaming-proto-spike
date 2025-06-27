package org.example;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.function.BiConsumer;

import proto.*;
public class PricesStreamableFileWriter extends StreamableProtoFileWriter<PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader, PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload> {
    public PricesStreamableFileWriter (String file,PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader header) throws FileNotFoundException, IOException,IllegalArgumentException  {
        super(file, header);
    }

    public PricesStreamableFileWriter (String file,PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader header,BiConsumer<Long, PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload> onWriteCallback ) throws FileNotFoundException, IOException,IllegalArgumentException  {
        super(file, header, onWriteCallback);
    }
}
