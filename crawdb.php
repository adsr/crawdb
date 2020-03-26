#!/usr/bin/env php
<?php
declare(strict_types=1);

define('CRAWDB_PHP_ERROR_FFI', -1000);
define('CRAWDB_PHP_ERROR_HEADER', -1001);

function crawdb_new(string $idx_path, string $dat_path, int $nkey, ?string $crawdb_h = null, ?string $libcrawdb_so = null): object {
    return _crawdb_new_open($idx_path, $dat_path, $nkey, $is_new = true, $crawdb_h, $libcrawdb_so);
}

function crawdb_open(string $idx_path, string $dat_path, ?string $crawdb_h = null, ?string $libcrawdb_so = null): object {
    return _crawdb_new_open($idx_path, $dat_path, 0, $is_new = false, $crawdb_h, $libcrawdb_so);
}

function crawdb_set(object $crawh, string $key, string $data): int {
    $nkey = _crawdb_set_key($crawh, $key);

    $ndata = strlen($data);
    if ($ndata > $crawh->nval) {
        $crawh->val = $crawh->ffi->new("uchar[{$ndata}]");
        $crawh->nval = $ndata;
    }
    FFI::memcpy($crawh->val, $data, $ndata);

    return $crawh->ffi->crawdb_set(
        $crawh->craw,
        $crawh->key,
        $crawh->nkey->cdata,
        $crawh->val,
        $ndata
    );
}

function crawdb_get(object $crawh, string $key): ?string {
    $nkey = _crawdb_set_key($crawh, $key);

    $crawh->get_nval->cdata = 0;
    $rv = $crawh->ffi->crawdb_get(
        $crawh->craw,
        $crawh->key,
        $crawh->nkey->cdata,
        FFI::addr($crawh->get_val),
        FFI::addr($crawh->get_nval)
    );
    if ($rv !== 0) {
        $GLOBALS['_craw_last_error'] = $rv;
        return null;
    }
    if (FFI::isNull($crawh->get_val)) {
        $GLOBALS['_craw_last_error'] = 0;
        return null;
    }

    $get_data = '';
    for ($i = 0; $i < $crawh->get_nval->cdata; ++$i) {
        $get_data .= chr($crawh->get_val[$i]);
    }

    return $get_data;
}

function crawdb_reload(object $crawh): int {
    return $crawh->ffi->crawdb_reload($crawh->craw);
}

function crawdb_index(object $crawh): int {
    return $crawh->ffi->crawdb_index($crawh->craw);
}

function crawdb_free(object $crawh): int {
    return $crawh->ffi->crawdb_free($crawh->craw);
}

$_craw_last_error = 0;
function crawdb_last_error() {
    return $GLOBALS['_craw_last_error'];
}

function _crawdb_new_open(string $idx_path, string $dat_path, int $nkey, bool $is_new, ?string $crawdb_h, ?string $libcrawdb_so): object {
    if ($crawdb_h === null) {
        $crawdb_h = __DIR__ . '/crawdb.h';
    }

    if ($$libcrawdb_so === null) {
        $libcrawdb_so = __DIR__ . '/libcrawdb.so';
    }

    if (!is_readable($crawdb_h) || !is_file($crawdb_h)) {
        $GLOBALS['_craw_last_error'] = CRAWDB_PHP_ERROR_HEADER;
        return null;
    }

    try {
        $header_file = file_get_contents($crawdb_h);
        $header_file = preg_replace('/^CRAWDB_API\s+/m', '', $header_file);
        $ffi = FFI::cdef($header_file, $libcrawdb_so);
    } catch (FFI\Exception $e) {
        $GLOBALS['_craw_last_error'] = CRAWDB_PHP_ERROR_FFI;
        return null;
    }

    $crawh = (object)[];

    $crawh->craw = $ffi->new('crawdb_t*');
    $crawh->key = null;
    $crawh->nkey = $ffi->new('uint32_t');
    $crawh->nkey->cdata = 0;
    $crawh->val = null;
    $crawh->nval = 0;
    $crawh->get_val = $ffi->new('uchar*');
    $crawh->get_nval = $ffi->new('uint32_t');
    $crawh->ffi = $ffi;

    if ($is_new) {
        $rv = $crawh->ffi->crawdb_new($idx_path, $dat_path, $nkey, FFI::addr($crawh->craw));
        if ($rv !== 0) {
            $GLOBALS['_craw_last_error'] = $rv;
            return null;
        }
    } else {
        $rv = $crawh->ffi->crawdb_open($idx_path, $dat_path, FFI::addr($crawh->craw));
        if ($rv !== 0) {
            $GLOBALS['_craw_last_error'] = $rv;
            return null;
        }
    }

    $rv = $crawh->ffi->crawdb_get_nkey($crawh->craw, FFI::addr($crawh->nkey));
    if ($rv !== 0) {
        $crawh->ffi->crawdb_free($crawh->craw);
        $GLOBALS['_craw_last_error'] = $rv;
        return null;
    }

    $crawh->key = $crawh->ffi->new("uchar[{$crawh->nkey->cdata}]");

    return $crawh;
}

function _crawdb_set_key(object $crawh, string $key): int {
    $nkey = min(strlen($key), $crawh->nkey->cdata);
    FFI::memset($crawh->key, 0, $crawh->nkey->cdata);
    FFI::memcpy($crawh->key, $key, $nkey);
    return $nkey;
}

function _crawdb_test() {
    $crawh = crawdb_new('/tmp/idx', '/tmp/dat', 32);
    crawdb_set($crawh, 'hello', 'world42');
    crawdb_reload($crawh);
    crawdb_index($crawh);
    $val = crawdb_get($crawh, 'hello');
    if ($val === 'world42') {
        printf("PASS\n");
    } else {
        printf("FAIL\n");
    }
    crawdb_free($crawh);
}

if (!empty(getenv('CRAWDB_PHP_TEST', true))) {
    _crawdb_test();
}
