import struct
from . import StreamableProtoFileParser
from google.protobuf.message import Message


class StreamableProtoFileWriter:
    
    def __init__(self,file_name, header_proto_instance):
        if not isinstance(header_proto_instance, Message):
            raise ValueError("header_proto_instance must be a protobuf message")
        
        if file_name is None:
            raise ValueError("file_name cannot be None")
        self.file_name = file_name
        self.file = open(self.file_name, "wb")
      
        magic_byte = struct.pack('>i',StreamableProtoFileParser.StreamableProtoFileParser.MAGIC_BYTE)
        self.file.write(magic_byte)  
        header_bytes = header_proto_instance.SerializeToString()
        header_size = struct.pack('>i', len(header_bytes))
        self.file.write(header_size)
        self.file.write(header_bytes)
        

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()
    
    
    def writePayload(self, payload_proto_instance):
        if not isinstance(payload_proto_instance, Message):
            raise ValueError("payload_proto_instance must be a protobuf message")  
        payload_bytes = payload_proto_instance.SerializeToString()
        payload_size = struct.pack('>i', len(payload_bytes))
        self.file.write(payload_size)
        self.file.write(payload_bytes)
        return payload_size