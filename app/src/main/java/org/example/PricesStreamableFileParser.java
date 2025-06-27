package org.example;
import java.util.function.Function;

import proto.*;
public class PricesStreamableFileParser extends StreamableProtoFileParser<PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader, PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload> {
    public PricesStreamableFileParser(String file,  Function<byte[],PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader> headerFactory, Function<byte[],PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload> protoFactory) throws com.google.protobuf.InvalidProtocolBufferException,IllegalArgumentException {
        super(file, headerFactory, protoFactory);
    }
}
