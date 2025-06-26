from proto_streamed_file.OnDiskBPlusTree import OnDiskBPlusTree

def from_byte_array(b):
    return (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3]

tree = OnDiskBPlusTree("../app/btree.data")

for i in range(5000):
    key = f"AAPL{i}".encode("utf-8")
    cnt = 0
    for value in tree.search(key):
        result = from_byte_array(value)
        cnt += 1
    if cnt != 5000:
        print(f"Incorrect values found for key: {key.decode()} - Expected 5000, found {cnt}")