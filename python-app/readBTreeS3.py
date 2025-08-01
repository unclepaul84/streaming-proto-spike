from proto_streamed_file.S3BPlusTreeSeeker import S3BPlusTreeSeeker
from proto.PricesStreamedFileHeader_pb2 import PricesStreamedFileHeader
from proto.PricesStreamedFilePayload_pb2 import PricesStreamedFilePayload
from proto_streamed_file.StreamableProtoFileS3Parser import StreamableProtoFileS3Parser
import boto3
from botocore.client import Config
from botocore import UNSIGNED

boto3.setup_default_session()
s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

tree = S3BPlusTreeSeeker(
    bucket="test-streaming-proto-files",
    key="prices.binpb/name.index",
    s3_client=s3_client
)

for i in range(1, 2):
    key = f"AAPL{i}".encode("utf-8")
    for value in tree.search(key):
        result = int.from_bytes(value, byteorder="big")
        print (result)
       

with StreamableProtoFileS3Parser('test-streaming-proto-files', 'prices.binpb/data.binpb', PricesStreamedFileHeader, PricesStreamedFilePayload, s3_client) as parser:
    header = parser.getHeader()

    print(f"Header: {header}")


    for value in tree.search( f"AAPL{1}".encode("utf-8")):
        offset = int.from_bytes(value, byteorder="big")
        print(f"Payload: {parser.getPayloadAtOffset(offset)}")