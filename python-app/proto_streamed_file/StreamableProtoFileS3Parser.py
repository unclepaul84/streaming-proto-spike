import struct
import boto3
from google.protobuf.message import Message
from io import BytesIO

class StreamableProtoFileS3Parser:
    MAGIC_BYTE = 0x1973
    FILE_SEAL_MARKER = -1

    def __init__(self, bucket, key, header_proto_message_class, payload_proto_message_class, s3_client):
        if s3_client is None:
            raise ValueError("s3_client must be provided")
        if not bucket or not key:
            raise ValueError("bucket and key must be provided")
        self.header_proto_message_class = header_proto_message_class
        self.payload_proto_message_class = payload_proto_message_class
        self.bucket = bucket
        self.key = key
        self.s3 = s3_client
        self.header = self.header_proto_message_class()
        self.seal_read = False
        self.offset = 0

        if not isinstance(self.header, Message):
            raise ValueError("header_proto_instance must be a protobuf message")
        payload_test = self.payload_proto_message_class()
        if not isinstance(payload_test, Message):
            raise ValueError("payload_proto_instance must be a protobuf class")

        # Read magic byte (4 bytes)
        magic_byte = self._read_int(self.offset, 4)
        if magic_byte != self.MAGIC_BYTE:
            raise ValueError("Invalid magic byte")
        self.offset += 4

        # Read header size (4 bytes)
        header_size = self._read_int(self.offset, 4)
        self.offset += 4

        # Read header bytes
        header_bytes = self._read_bytes(self.offset, header_size)
        self.header.ParseFromString(header_bytes)
        self.offset += header_size

    def _read_bytes(self, offset, length):
        range_header = f"bytes={offset}-{offset+length-1}"
        resp = self.s3.get_object(Bucket=self.bucket, Key=self.key, Range=range_header)
        return resp["Body"].read()

    def _read_int(self, offset, length):
        data = self._read_bytes(offset, length)
        return int.from_bytes(data, byteorder="big")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # Nothing to close

    def getHeader(self):
        return self.header

    def getPayloadAtOffset(self, offset):
        payload_size = self._read_int(offset, 4)
        payload_bytes = self._read_bytes(offset + 4, payload_size)
        payload = self.payload_proto_message_class()
        payload.ParseFromString(payload_bytes)
        return payload
