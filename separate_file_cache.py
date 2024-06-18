import gzip
import os
import json
from typing import Union

class SeparateFileCache:
    def __init__(self, directory: str):
        self.directory = directory
        if not os.path.exists(directory):
            os.makedirs(directory)

    def set(self, key: str, value: Union[str, dict], is_json: bool = False):
        file_path = os.path.join(self.directory, key)
        with gzip.open(file_path, 'wb', compresslevel=6) as f:
            data = json.dumps(value).encode() if is_json else value.encode()
            f.write(data)

    def get(self, key: str):
        file_path = os.path.join(self.directory, key)
        if not os.path.exists(file_path):
            raise KeyError(f'Key {key} not found')
        with gzip.open(file_path, 'rb') as f:
            data = f.read()
        return json.loads(data) if data.startswith(b'{') else data.decode()

    def delete(self, key: str):
        file_path = os.path.join(self.directory, key)
        if os.path.exists(file_path):
            os.remove(file_path)

    def has(self, key: str) -> bool:
        file_path = os.path.join(self.directory, key)
        return os.path.exists(file_path)
