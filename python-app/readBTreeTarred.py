from proto_streamed_file.OnDiskBPlusTree import OnDiskBPlusTree
from proto.PricesStreamedFileHeader_pb2 import PricesStreamedFileHeader
from proto.PricesStreamedFilePayload_pb2 import PricesStreamedFilePayload
from proto_streamed_file.StreamableProtoFileParser import StreamableProtoFileParser

import tarfile

with tarfile.open("../app/prices.binpb.tar.gz", "r:gz") as tar:
    tar.extractall(path="./")

tree = OnDiskBPlusTree("name.index")

with StreamableProtoFileParser('data.binpb', PricesStreamedFileHeader, PricesStreamedFilePayload) as parser:
    header = parser.getHeader()

    print(f"Header: {header}")

 
    for value in tree.search( f"AAPL1".encode("utf-8")):
        offset = int.from_bytes(value, byteorder="big")
        print(f"Payload: {parser.getPayloadAtOffset(offset)}")