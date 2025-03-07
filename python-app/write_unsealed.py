from proto.PricesStreamedFileHeader_pb2 import PricesStreamedFileHeader
from proto.PricesStreamedFilePayload_pb2 import PricesStreamedFilePayload
from proto.PriceEntity_pb2 import PriceEntity

from proto_streamed_file.StreamableProtoFileWriter import StreamableProtoFileWriter

header = PricesStreamedFileHeader()
header.source = "python-app"

writer = StreamableProtoFileWriter('price_entities_python.bin', header)


payload = PricesStreamedFilePayload()
payload.price.name = "AAPL"
payload.price.prices.append(100.0)

writer.writePayload(payload)

