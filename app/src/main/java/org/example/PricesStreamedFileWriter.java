package org.example;
import java.io.FileNotFoundException;
import java.io.IOException;


import proto.*;
public class PricesStreamedFileWriter extends StreamableProtoFileWriter<PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader, PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload> {
    public PricesStreamedFileWriter (String file,PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader header) throws FileNotFoundException, IOException,IllegalArgumentException  {
        super(file, header);
    }
}
