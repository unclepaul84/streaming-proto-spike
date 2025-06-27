from proto.PricesStreamedFileHeader_pb2 import PricesStreamedFileHeader
from proto.PricesStreamedFilePayload_pb2 import PricesStreamedFilePayload
from proto_streamed_file.StreamableProtoFileParser import StreamableProtoFileParser

with StreamableProtoFileParser('price_entities_python.binpb', PricesStreamedFileHeader, PricesStreamedFilePayload) as parser:
    header = parser.getHeader()

    print(f"Header: {header}")

    print(f"Payload: {parser.getNextPayload()}")
    print(f"Payload: {parser.getNextPayload()}")
    print(f"Payload: {parser.getNextPayload()}")

