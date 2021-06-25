#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdint.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

/*
  IDX FORMAT

      HEADER    "CRAW":4
                <vers:1>
                <nkey:4>
                <nsorted:8>
                <dead:1>
      SORTED    <key:nkey> <offset:8> <len:4> <cksum:2> <del:1>
                ...
    UNSORTED    ...
*/

#define CRAWDB_OK                      0
#define CRAWDB_ERR                    -1
/* i=-2; for e in $(grep -Proh 'CRAWDB_ERR_\w+' | sort | uniq); do echo "#define $e $i"; i=$((i-1)); done */
#define CRAWDB_ERR_BAD_IDX_SIZE       -2
#define CRAWDB_ERR_BAD_NSORTED        -3
#define CRAWDB_ERR_BSEARCH            -4
#define CRAWDB_ERR_GET_BAD_KEY        -5
#define CRAWDB_ERR_GET_DATA_CKSUM     -6
#define CRAWDB_ERR_GET_DATA_READ      -7
#define CRAWDB_ERR_INDEX_COPY         -8
#define CRAWDB_ERR_INDEX_LSEEK        -9
#define CRAWDB_ERR_INDEX_OPEN_COPY    -10
#define CRAWDB_ERR_LOCK_EX            -11
#define CRAWDB_ERR_LOCK_UN            -12
#define CRAWDB_ERR_LSEARCH            -13
#define CRAWDB_ERR_OPEN_BAD_HEADER    -14
#define CRAWDB_ERR_OPEN_BAD_NSORTED   -15
#define CRAWDB_ERR_OPEN_BAD_SIZE      -16
#define CRAWDB_ERR_OPEN_BAD_VERS      -17
#define CRAWDB_ERR_OPEN_DAT           -18
#define CRAWDB_ERR_OPEN_IDX           -19
#define CRAWDB_ERR_OPEN_LSEEK         -20
#define CRAWDB_ERR_OPEN_NKEY_ZERO     -21
#define CRAWDB_ERR_OPEN_READ_HEADER   -22
#define CRAWDB_ERR_OPEN_WRITE_HEADER  -23
#define CRAWDB_ERR_READ_IDX_RECORD    -24
#define CRAWDB_ERR_SET_ALREADY_EXISTS -25
#define CRAWDB_ERR_SET_BAD_KEY        -26
#define CRAWDB_ERR_SET_IDX_DEAD       -27
#define CRAWDB_ERR_SET_LSEEK          -28
#define CRAWDB_ERR_SET_PREAD_DEAD     -29
#define CRAWDB_ERR_SET_WRITE_DAT      -30
#define CRAWDB_ERR_SET_WRITE_IDX      -31
#define CRAWDB_ERR_SORT_COPY_HEADER   -32
#define CRAWDB_ERR_SORT_LSEEK         -33
#define CRAWDB_ERR_SORT_OPEN_NEW      -34
#define CRAWDB_ERR_SORT_READ          -35
#define CRAWDB_ERR_SORT_WRITE_NSORTED -36
#define CRAWDB_ERR_SORT_WRITE_REC     -37
#define CRAWDB_ERR_SWAP_COPY          -38
#define CRAWDB_ERR_SWAP_LSEEK         -39
#define CRAWDB_ERR_SWAP_RENAME        -40
#define CRAWDB_ERR_SWAP_WRITE_DEAD    -41
#define CRAWDB_ERR_DELETE_NOT_FOUND   -42
#define CRAWDB_ERR_DELETE_WRITE_FLAG  -43
#define CRAWDB_ERR_DELETE_RELOAD      -44

#define CRAWDB_HEADER_SIZE             18
#define CRAWDB_HEADER_VERS             1
#define CRAWDB_OFFSET_NSORTED          9
#define CRAWDB_OFFSET_DEAD             17
#define CRAWDB_API                     __attribute__ ((visibility ("default")))

#define try(__call)                         do { if ((rv = (__call)) != CRAWDB_OK) return rv; } while(0)
#define goto_if_err(__cond, __errv, __labl) do { if (__cond) { rv = (__errv); goto __labl; }  } while(0);
#define return_if_err(__cond, __errv)       do { if (__cond) return (__errv);                 } while(0);

typedef struct crawdb_s crawdb_t;
typedef unsigned char uchar;

struct crawdb_s {
    char *idx_path;
    char *dat_path;
    int fd_idx;
    int fd_dat;
    long idx_size;
    int locked;
    uint8_t vers;
    uint32_t nkey;
    uint64_t nsorted;
    uint64_t nunsorted;
    uint64_t ntotal;
    uint8_t dead;
    uchar *rec;
    size_t nrec;
    uchar *data;
    size_t ndata;
};

CRAWDB_API int crawdb_new(char *idx_path, char *dat_path, uint32_t nkey, crawdb_t **out_craw);
CRAWDB_API int crawdb_open(char *idx_path, char *dat_path, crawdb_t **out_craw);
CRAWDB_API int crawdb_reload(crawdb_t *craw);
CRAWDB_API int crawdb_set(crawdb_t *craw, uchar *key, uint32_t nkey, uchar *val, uint32_t nval);
CRAWDB_API int crawdb_get(crawdb_t *craw, uchar *key, uint32_t nkey, uchar **out_val, uint32_t *out_nval, uint64_t *out_idx);
CRAWDB_API int crawdb_delete(crawdb_t *craw, uchar *key, uint32_t nkey);
CRAWDB_API int crawdb_get_i(crawdb_t *craw, uint64_t i, uchar **out_key, uint32_t *out_nkey, uchar **out_val, uint32_t *out_nval);
CRAWDB_API int crawdb_cksum(uchar *val, uint32_t len, uint16_t *out_cksum);
CRAWDB_API int crawdb_index(crawdb_t *craw);
CRAWDB_API int crawdb_get_nkey(crawdb_t *craw, uint32_t *out_nkey);
CRAWDB_API int crawdb_get_ntotal(crawdb_t *craw, uint64_t *out_ntotal);
CRAWDB_API int crawdb_get_nsorted(crawdb_t *craw, uint64_t *out_nsorted);
CRAWDB_API int crawdb_get_nunsorted(crawdb_t *craw, uint64_t *out_nunsorted);
CRAWDB_API int crawdb_free(crawdb_t *craw);
