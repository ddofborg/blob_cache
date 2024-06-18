from lmdbdict import lmdbdict

class LmdbCache:
    def __init__(self, filename):
        self.filename = filename+'.lmdb'
        self.db = lmdbdict(self.filename, mode='w')

    def __enter__(self):
        self.db = lmdbdict(self.filename, mode='w')
        return self.db

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.close()

    def set(self, key, value):
        self.db[key] = value

    def get(self, key):
        return self.db[key]

    def delete(self, key):
        del self.db[key]
