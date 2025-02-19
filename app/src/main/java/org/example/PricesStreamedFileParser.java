package org.example;
import java.util.function.Function;

import proto.*;
public class PricesStreamedFileParser extends StreamableProtoFileParser<PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader, PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload> {
    public PricesStreamedFileParser(String file,  Function<byte[],PricesStreamedFileHeaderOuterClass.PricesStreamedFileHeader> headerFactory, Function<byte[],PricesStreamedFilePayloadOuterClass.PricesStreamedFilePayload> protoFactory) throws com.google.protobuf.InvalidProtocolBufferException,IllegalArgumentException {
        super(file, headerFactory, protoFactory);
    }
}
