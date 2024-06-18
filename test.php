<?php

require_once 'blob_cache.php';

$rnd = str_repeat('a', 1 * 1024 * 1024); // 1MB
$binary = '';
for ($i = 0; $i < 256; $i++) {
    $binary .= chr($i);
}

$c = new BlobCache('tmp_test_cache');

echo 'Current keys: ' . implode(', ', $c->keys()) . "\n";

// Test key-values
$t = array();
$t['string'] = 'value1';
$t['int'] = 1;
$t['float'] = 1.1;
$t['dict'] = array('a' => 1, 'b' => 2);
$t['list'] = array(1, 2, 3);
$t['bytes'] = 'value1';
$t['bool'] = true;
$t['string_1M'] = $rnd;
$t['string_2M'] = $rnd . $rnd;
$t['binary'] = $binary;
$t['mb_string1'] = '漢字はユニコード';
$t['mb_string2'] = 'X生';

echo "----\n";
foreach ($t as $k => $v) {
    echo "Setting key: `{$k}`, ValueType: " . gettype($v) . "\n";
    $c->set($k, $v);
}

foreach ($t as $k => $v) {
    echo "Comparing key: `{$k}`...";
    $val = $c->get($k);
    if ($val == $v) {
        echo "OK\n";
    } else {
        echo "FAILED\n";
    }
}

echo "----\n";
echo "Setting key `delete`...\n";
$c->set('delete', 'delete');
echo 'Has key `delete`: ' . ($c->has('delete') ? 'true' : 'false') . "\n";
echo "Deleting key `delete`...\n";
$c->delete('delete');
echo 'Has key `delete`: ' . ($c->has('delete') ? 'true' : 'false') . "\n";

echo "----\n";
$c->set('2sec_ttl', 'value', 2);
for ($i = 0; $i < 7; $i++) {
    echo 'Has key `2sec_ttl`: ' . ($c->has('2sec_ttl') ? 'true' : 'false') . "\n";
    usleep(500000); // 0.5 seconds
}
echo 'Has key `2sec_ttl`: ' . ($c->has('2sec_ttl') ? 'true' : 'false') . "\n";

echo "----\n";
if ($c->has('2sec_ttl_callback')) {
    echo 'Key `2sec_ttl_callback` expires in ' . $c->whenExpired('2sec_ttl_callback', true) . "s\n";
} else {
    $c->set('2sec_ttl_callback', 'value_new_2', 2);
}
for ($i = 0; $i < 7; $i++) {
    echo 'Value key `2sec_ttl_callback`: ' . $c->get('2sec_ttl_callback', function ($x) {
        return 'value_new_20';
    }, 20) . "\n";
    usleep(500000); // 0.5 seconds
}
echo 'Value key `2sec_ttl_callback`: ' . $c->get('2sec_ttl_callback') . "\n";
echo 'Key `2sec_ttl_callback` expires in ' . $c->whenExpired('2sec_ttl_callback', true) . "s\n";

echo "----\n";
foreach ($t as $k => $v) {
    echo 'Value preview for key `' . $k . '`: ' . substr(json_encode($c->get($k)), 0, 30) . "\n";
}

echo "----\n";
$f = $c->fragmentationRatio();
echo 'Fragmentation: ' . $f . "\n";

$c->close();
