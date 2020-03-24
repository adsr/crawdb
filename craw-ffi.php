#!/usr/bin/env php
<?php
declare(strict_types=1);

class CrawDB {

    public function initFFI(string $crawdb_h, string $libcrawdb_so) {
        $this->maybeFreeCraw();
        $header_file = file_get_contents($crawdb_h);
        $header_file = preg_replace('/^CRAWDB_API\s+/m', '', $header_file);
        $ffi = FFI::cdef($header_file, $libcrawdb_so);
        $this->craw = $ffi->new('crawdb_t*');
        $this->key = null;
        $this->nkey = $ffi->new('uint32_t');
        $this->nkey->cdata = 0;
        $this->val = null;
        $this->nval = 0;
        $this->get_val = $ffi->new('uchar*');
        $this->get_nval = $ffi->new('uint32_t');
        $this->ffi = $ffi;
    }

    public function new(string $idx_path, string $dat_path, int $nkey): int {
        $this->ensureFFI();
        $this->maybeFreeCraw();

        $rv = $this->ffi->crawdb_new($idx_path, $dat_path, $nkey, FFI::addr($this->craw));
        if ($rv !== 0) {
            return $rv;
        }

        $rv = $this->allocKey();
        if ($rv !== 0) {
            return $rv;
        }
        return 0;
    }

    public function open(string $idx_path, string $dat_path): int {
        $this->ensureFFI();
        $this->maybeFreeCraw();

        $rv = $this->ffi->crawdb_open($idx_path, $dat_path, FFI::addr($this->craw));
        if ($rv !== 0) {
            return $rv;
        }
        if (FFI::isNull($this->craw)) {
            return -1;
        }

        $rv = $this->allocKey();
        if ($rv !== 0) {
            return $rv;
        }
        return 0;
    }

    public function set(string $key, string $data): int {
        $this->ensureCraw();

        $nkey = $this->setKey($key);

        $ndata = strlen($data);
        if ($ndata > $this->nval) {
            $this->val = $this->ffi->new("uchar[{$ndata}]");
            $this->nval = $ndata;
        }
        FFI::memcpy($this->val, $data, $ndata);

        return $this->ffi->crawdb_set(
            $this->craw,
            $this->key,
            $this->nkey->cdata,
            $this->val,
            $ndata
        );
    }

    public function get(string $key, int &$out_rv): ?string {
        $this->ensureCraw();

        $nkey = $this->setKey($key);

        $this->get_nval->cdata = 0;
        $rv = $this->ffi->crawdb_get(
            $this->craw,
            $this->key,
            $this->nkey->cdata,
            FFI::addr($this->get_val),
            FFI::addr($this->get_nval)
        );
        if ($rv !== 0) {
            $out_rv = $rv;
            return null;
        }
        if (FFI::isNull($this->get_val)) {
            $out_rv = 0;
            return null;
        }

        $get_data = '';
        for ($i = 0; $i < $this->get_nval->cdata; ++$i) {
            $get_data .= chr($this->get_val[$i]);
        }

        $out_rv = 0;
        return $get_data;
    }

    public function index(): int {
        $this->ensureCraw();
        return $this->ffi->crawdb_index($this->craw);
    }

    public function reload(): int {
        $this->ensureCraw();
        return $this->ffi->crawdb_reload($this->craw);
    }

    public function free(): int {
        $this->ensureCraw();
        return $this->ffi->crawdb_free($this->craw);
    }

    private function ensureFFI(): void {
        if (!$this->ffi) {
            throw new RuntimeException("Expected ffi");
        }
    }

    private function ensureCraw(): void {
        $this->ensureFFI();
        if ($this->craw === null || FFI::isNull($this->craw)) {
            throw new RuntimeException("Expected craw pointer");
        }
    }

    private function maybeFreeCraw(): void {
        if ($this->craw !== null && !FFI::isNull($this->craw)) {
            $this->free();
        }
    }

    private function allocKey(): int {
        $rv = $this->ffi->crawdb_get_nkey($this->craw, FFI::addr($this->nkey));
        if ($rv !== 0) {
            return $rv;
        }
        if ($this->nkey->cdata < 1) {
            return -1;
        }
        $this->key = $this->ffi->new("uchar[{$this->nkey->cdata}]");
        return 0;
    }

    private function setKey(string $key): int {
        $nkey = min(strlen($key), $this->nkey->cdata);
        FFI::memset($this->key, 0, $this->nkey->cdata);
        FFI::memcpy($this->key, $key, $nkey);
        return $nkey;
    }

    public static function test() {
        $rv = 0;
        $craw = new CrawDB();
        $craw->initFFI('crawdb.h', __DIR__ . '/libcrawdb.so');
        $rv = $craw->new('/tmp/idx', '/tmp/dat', 32);
        $rv = $craw->set('hello', 'world');
        $rv = $craw->reload();
        $rv = $craw->index();
        $val = $craw->get('hello', $rv);
        if ($rv !== 0 || $val !== 'world') {
            throw new RuntimeException('Test failure');
        }
        $craw->free();
    }
}

CrawDB::test();
