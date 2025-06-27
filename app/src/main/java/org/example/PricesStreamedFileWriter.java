package org.example;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.function.BiConsumer;

import proto.*;
public class PricesStreamedFileWriter extends StreamableProtoFileWriter<PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader, PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload> {
    public PricesStreamedFileWriter (String file,PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader header) throws FileNotFoundException, IOException,IllegalArgumentException  {
        super(file, header);
    }

    public PricesStreamedFileWriter (String file,PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader header,BiConsumer<Long, PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload> onWriteCallback ) throws FileNotFoundException, IOException,IllegalArgumentException  {
        super(file, header, onWriteCallback);
    }
}
