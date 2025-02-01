from proto.PricesStreamedFileHeader_pb2 import PricesStreamedFileHeader
from proto.PricesStreamedFilePayload_pb2 import PricesStreamedFilePayload
from proto_streamed_file.StreamableProtoFileParser import StreamableProtoFileParser

parser = StreamableProtoFileParser('../app/price_entities.bin', PricesStreamedFileHeader, PricesStreamedFilePayload)

header = parser.getHeader()

print(f"Header: {header}")

print(f"Payload: {parser.getNextPayload()}")

