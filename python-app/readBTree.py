from proto_streamed_file.OnDiskBPlusTree import OnDiskBPlusTree
from proto.PricesStreamedFileHeader_pb2 import PricesStreamedFileHeader
from proto.PricesStreamedFilePayload_pb2 import PricesStreamedFilePayload
from proto_streamed_file.StreamableProtoFileParser import StreamableProtoFileParser


tree = OnDiskBPlusTree("../app/name.index")

for i in range(500000, 500010):
    key = f"AAPL{i}".encode("utf-8")
 
    for value in tree.search(key):
        result = int.from_bytes(value, byteorder="big")
        print (result)
       

with StreamableProtoFileParser('../app/price_entities_java_indexed.binpb', PricesStreamedFileHeader, PricesStreamedFilePayload) as parser:
    header = parser.getHeader()

    print(f"Header: {header}")

    print(f"Payload: {parser.getNextPayload()}")
    #print(f"Payload: {parser.getNextPayload()}")
   # print(f"Payload: {parser.getNextPayload()}")
 
    for value in tree.search( f"AAPL{1499999}".encode("utf-8")):
        offset = int.from_bytes(value, byteorder="big")
        print(f"Payload: {parser.getPayloadAtOffset(offset)}")