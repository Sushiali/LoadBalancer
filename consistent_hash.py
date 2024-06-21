import hashlib
import bisect

class ConsistentHash:
    def __init__(self, num_replicas=3):
        self.num_replicas = num_replicas
        self.ring = {}
        self.sorted_keys = []

    def _hash(self, key):
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_server(self, server):
        for i in range(self.num_replicas):
            server_key = f"{server}-{i}"
            hash_value = self._hash(server_key)
            self.ring[hash_value] = server
            bisect.insort(self.sorted_keys, hash_value)

    def remove_server(self, server):
        for i in range(self.num_replicas):
            server_key = f"{server}-{i}"
            hash_value = self._hash(server_key)
            self.ring.pop(hash_value)
            self.sorted_keys.remove(hash_value)

    def get_server(self, key):
        if not self.ring:
            return None
        hash_value = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, hash_value)
        if idx == len(self.sorted_keys):
            idx = 0
        return self.ring[self.sorted_keys[idx]]

# Example usage
if __name__ == "__main__":
    ch = ConsistentHash()

    # Adding servers
    ch.add_server("server1")
    ch.add_server("server2")
    ch.add_server("server3")

    # Getting server for a given key
    print(ch.get_server("my_key"))  # Example output: server2
    print(ch.get_server("another_key"))  # Example output: server1

    # Removing a server
    ch.remove_server("server2")

    # Getting server for the same key after removal
    print(ch.get_server("my_key"))  # Example output: server1 or server3
