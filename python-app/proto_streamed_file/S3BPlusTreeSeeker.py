import struct
from typing import Iterator, Optional, List, Tuple

PAGE_SIZE = 1024 * 4
MAX_KEY_SIZE = 128
MAX_VALUE_SIZE = 8

NODE_LEAF = 1
NODE_INTERNAL = 2
NODE_OVERFLOW = 3

class LeafEntry:
    def __init__(self, key: bytes, overflow_head: int, overflow_tail: int):
        self.key = key
        self.overflow_head = overflow_head
        self.overflow_tail = overflow_tail

class InternalEntry:
    def __init__(self, key: bytes, right_child: int):
        self.key = key
        self.right_child = right_child

class S3BPlusTreeSeeker:
    def __init__(self, bucket: str, key: str, s3_client):
        self.bucket = bucket
        self.key = key
        self.s3 = s3_client
        self.file_size = self._get_file_size()
        self.next_page_id = self.file_size // PAGE_SIZE

    def _get_file_size(self) -> int:
        resp = self.s3.head_object(Bucket=self.bucket, Key=self.key)
        return resp['ContentLength']

    def get_page(self, page_id: int) -> memoryview:
        offset = page_id * PAGE_SIZE
        resp = self.s3.get_object(
            Bucket=self.bucket,
            Key=self.key,
            Range=f'bytes={offset}-{offset+PAGE_SIZE-1}'
        )
        data = resp['Body'].read()
        if len(data) < PAGE_SIZE:
            data = data + b'\x00' * (PAGE_SIZE - len(data))
        return memoryview(data)

    def get_root_page_id(self) -> int:
        resp = self.s3.get_object(
            Bucket=self.bucket,
            Key=self.key,
            Range='bytes=0-7'
        )
        data = resp['Body'].read()
        return struct.unpack_from('>Q', data, 0)[0]

    def search(self, key: bytes) -> Iterator[bytes]:
        if len(key) > MAX_KEY_SIZE:
            raise ValueError("Key exceeds maximum size")
        overflow_pages = self._find_overflow(self.get_root_page_id(), key)
        if not overflow_pages:
            return iter([])
        overflow_page, tail_page = overflow_pages
        return self._overflow_iterator(overflow_page)

    def _overflow_iterator(self, overflow_page: int) -> Iterator[bytes]:
        page_id = overflow_page
        while page_id != -1:
            buf = self.get_page(page_id)
            node_type = buf[0]
            assert node_type == NODE_OVERFLOW
            value_count = struct.unpack_from('>I', buf, 1)[0]
            pos = 5
            for _ in range(value_count):
                klen = struct.unpack_from('>I', buf, pos)[0]
                pos += 4
                val = bytes(buf[pos:pos+klen])
                pos += klen
                yield val
            page_id = struct.unpack_from('>q', buf, PAGE_SIZE - 8)[0]

    def _find_overflow(self, page_id: int, key: bytes) -> Optional[Tuple[int, int]]:
        buf = self.get_page(page_id)
        node_type = buf[0]
        if node_type == NODE_LEAF:
            entries = self._read_leaf_entries(buf)
            idx = self._find_key_index(entries, key)
            if idx != -1:
                entry = entries[idx]
                return (entry.overflow_head, entry.overflow_tail)
            return None
        elif node_type == NODE_INTERNAL:
            leftmost_child, entries = self._read_internal_entries(buf)
            for entry in entries:
                if key < entry.key:
                    return self._find_overflow(leftmost_child, key)
                leftmost_child = entry.right_child
            return self._find_overflow(leftmost_child, key)
        else:
            raise Exception("Unknown page type")

    def _find_key_index(self, entries: List[LeafEntry], key: bytes) -> int:
        left, right = 0, len(entries) - 1
        while left <= right:
            mid = (left + right) // 2
            cmp = (entries[mid].key > key) - (entries[mid].key < key)
            if cmp == 0:
                return mid
            elif cmp < 0:
                left = mid + 1
            else:
                right = mid - 1
        return -1

    def _read_leaf_entries(self, buf: memoryview) -> List[LeafEntry]:
        count = struct.unpack_from('>I', buf, 1)[0]
        entries = []
        pos = 5
        for _ in range(count):
            klen = struct.unpack_from('>I', buf, pos)[0]
            pos += 4
            key = bytes(buf[pos:pos+klen])
            pos += klen
            head = struct.unpack_from('>Q', buf, pos)[0]
            pos += 8
            tail = struct.unpack_from('>Q', buf, pos)[0]
            pos += 8
            entries.append(LeafEntry(key, head, tail))
        return entries

    def _read_internal_entries(self, buf: memoryview) -> Tuple[int, List[InternalEntry]]:
        count = struct.unpack_from('>I', buf, 1)[0]
        pos = 5
        leftmost_child = struct.unpack_from('>Q', buf, pos)[0]
        pos += 8
        entries = []
        for _ in range(count):
            klen = struct.unpack_from('>I', buf, pos)[0]
            pos += 4
            key = bytes(buf[pos:pos+klen])
            pos += klen
            right = struct.unpack_from('>Q', buf, pos)[0]
            pos += 8
            entries.append(InternalEntry(key, right))
        return leftmost_child, entries
