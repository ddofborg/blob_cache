from blob_cache_dict import BlobCacheDict
from benchmark import alphanumeric_string
import time

rnd = alphanumeric_string(1*1024**2)  # 1mb
binary = b''.join([bytes([i]) for i in range(256)])

c = BlobCacheDict('tmp_test_cache')

print(f'Current keys: {list(c.keys())}')

# Test key-values
t = {}
t['string'] = 'value1'
t['int'] = 1
t['float'] = 1.1
t['dict'] = {'a': 1, 'b': 2}
t['list'] = [1, 2, 3]
t['bytes'] = b'value1'
t['bool'] = True
t['string_1M'] = rnd
t['string_2M'] = rnd+rnd
t['binary'] = binary
t['mb_string1'] = '漢字はユニコード'
t['mb_string2'] = 'X生'

print('----')
for k,v in t.items():
    print(f'Setting key: `{k}`, ValueType: {type(v)}')
    c[k] = v

for k,v in t.items():
    print(f'Comparing key: `{k}`...', end='')
    try:
        assert c[k] == v
        print('OK')
    except AssertionError:
        print('FAILED')


print('----')
print('Setting key `delete`...')
c['delete'] = 'delete'
print(f'Has key `delete`: {c.has("delete")}')
print('Deleting key `delete`...')
del c['delete']
print(f'Has key `delete`: {c.has("delete")}')


print('----')
c.set('2sec_ttl', 'value', ttl=2)
for _ in range(5):
    print(f'Has key `2sec_ttl`: {c.has("2sec_ttl")}')
    time.sleep(0.5)
print(f'Has key `2sec_ttl`: {c.has("2sec_ttl")}')


print('----')
if c.has('2sec_ttl_callback'):
    print(f'Key `2sec_ttl_callback` expires in {c.when_expired("2sec_ttl_callback", True)}s')
for _ in range(5):
    print(f'Value key `2sec_ttl_callback`: {c.get("2sec_ttl_callback", refresh_callback=lambda x: "value_new_20", new_ttl=20)}')
    time.sleep(0.5)
print(f'Value key `2sec_ttl_callback`: {c.get("2sec_ttl_callback")}')
print(f'Key `2sec_ttl_callback` expires in {c.when_expired("2sec_ttl_callback", True)}s')


print('----')
for k,v in t.items():
    print(f'Value preview for key `{k}`: {str(c.get(k))[:30]}')


print('----')
f=c.fragmentation_ratio()
print(f'Fragmentation: {f}')


c.close()


