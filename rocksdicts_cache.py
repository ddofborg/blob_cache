from rocksdict import Rdict

class RocksdictCache:
    def __init__(self, filename):
        self.filename = filename+'.rockdb'
        self.db = Rdict(self.filename)

    def __enter__(self):
        self.db = Rdict(self.filename)
        return self.db

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.close()

    def set(self, key, value):
        self.db[key] = value

    def get(self, key):
        return self.db[key]

    def delete(self, key):
        del self.db[key]
