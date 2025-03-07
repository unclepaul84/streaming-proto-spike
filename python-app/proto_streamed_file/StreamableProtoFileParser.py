import struct
from google.protobuf.message import Message

class StreamableProtoFileParser:
    MAGIC_BYTE = 0x1973
    FILE_SEAL_MARKER = -1
    def __init__(self,file_name, header_proto_message_class, payload_proto_message_class):
        self.header_proto_message_class = header_proto_message_class
        self.payload_proto_message_class = payload_proto_message_class
        if file_name is None:
            raise ValueError("file_name cannot be None")
        self.file_name = file_name
        self.file = open(self.file_name, "rb")
        self.header = self.header_proto_message_class()
        self.seal_read = False

        if not isinstance(self.header, Message):
            raise ValueError("header_proto_instance must be a protobuf message")
        
        payload_test = self.payload_proto_message_class()
        
        if not isinstance(payload_test, Message):
            raise ValueError("payload_proto_instance must be a protobuf class")

        magic_byte = struct.unpack('>i', self.file.read(4))[0]
        if magic_byte != self.MAGIC_BYTE:
            raise ValueError("Invalid magic byte")    
        header_size = struct.unpack('>i', self.file.read(4))[0]
        header_bytes = self.file.read(header_size)      
        self.header.ParseFromString(header_bytes)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()
    
    def getHeader(self):
        return self.header
    
    def getNextPayload(self):
        if self.seal_read:
            return
        size_buffer = self.file.read(4)
        if not size_buffer:
             if not self.seal_read:
                 raise ValueError("Seal not found. File is may be corrupted")
        payload_size = struct.unpack('>i', size_buffer)[0]
        if(payload_size == self.FILE_SEAL_MARKER):
            self.seal_read = True
            return None
        payload_bytes = self.file.read(payload_size)
        if payload_bytes is None:
            return None
        payload = self.payload_proto_message_class()
        payload.ParseFromString(payload_bytes)
        return payload