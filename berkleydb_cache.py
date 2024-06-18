import dbm

class BerkleyDbCache:
    def __init__(self, filename):
        self.filename = filename+'.dbm'
        self.db = dbm.open(self.filename, 'c')

    def __enter__(self):
        self.db = dbm.open(self.filename, 'c')
        return self.db

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.close()

    def set(self, key, value):
        self.db[key] = value

    def get(self, key):
        return self.db[key]

    def delete(self, key):
        del self.db[key]
