from blob_cache import BlobCache

class BlobCacheDict(BlobCache):

    def __len__(self):
        return len(self.index)

    def __contains__(self, key):
        return key in self.index

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __iter__(self):
        for key in self.index:
            yield key

    def __delitem__(self, key):
        self.delete(key)

    def keys(self):
        return (k for k in self.index.keys())

    def values(self):
        return (self.get(k) for k in self.index.keys())

    def items(self):
        return ((k, self.get(k)) for k in self.index.keys())
