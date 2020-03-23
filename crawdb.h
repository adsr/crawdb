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
idx format

start - end  bytes: content
    0 - 4        4: CRAW
    4 - 5        1: vers
    5 - 9        4: nkey
    9 - 17       8: nsorted
   17 - 18       1: dead
                 [
    0 - n        n: key
    n - n+8      8: offset
  n+8 - n+12     4: len
 n+12 - n+14     2: cksum
                 ]*
*/

#define CRAWDB_OK                     0

#define CRAWDB_ERR                   -1
#define CRAWDB_ERR_BSEARCH           -2
#define CRAWDB_ERR_GET_BAD_KEY       -3
#define CRAWDB_ERR_GET_DATA_CKSUM    -4
#define CRAWDB_ERR_GET_DATA_READ     -5
#define CRAWDB_ERR_LOCK_EX           -6
#define CRAWDB_ERR_LOCK_UN           -7
#define CRAWDB_ERR_LSEARCH           -8
#define CRAWDB_ERR_OPEN_BAD_HEADER   -9
#define CRAWDB_ERR_OPEN_BAD_NSORTED  -10
#define CRAWDB_ERR_OPEN_BAD_SIZE     -11
#define CRAWDB_ERR_OPEN_BAD_VERS     -12
#define CRAWDB_ERR_OPEN_DAT          -13
#define CRAWDB_ERR_OPEN_IDX          -14
#define CRAWDB_ERR_OPEN_LSEEK        -15
#define CRAWDB_ERR_OPEN_NKEY_ZERO    -16
#define CRAWDB_ERR_OPEN_READ_HEADER  -17
#define CRAWDB_ERR_OPEN_WRITE_HEADER -18
#define CRAWDB_ERR_SET_BAD_KEY       -19
#define CRAWDB_ERR_SET_IDX_DEAD      -20
#define CRAWDB_ERR_SET_LSEEK         -21
#define CRAWDB_ERR_SET_PREAD_DEAD    -22
#define CRAWDB_ERR_SET_WRITE_DAT     -23
#define CRAWDB_ERR_SET_WRITE_IDX     -24
#define CRAWDB_ERR_SWAP_COPY         -25
#define CRAWDB_ERR_SWAP_LSEEK        -26
#define CRAWDB_ERR_SWAP_RENAME       -27
#define CRAWDB_ERR_SWAP_WRITE_DEAD   -28

#define CRAWDB_HEADER_SIZE            18
#define CRAWDB_HEADER_VERS            1
#define CRAWDB_OFFSET_NSORTED         9
#define CRAWDB_OFFSET_DEAD            17
#define CRAWDB_API                    __attribute__ ((visibility ("default")))

#define try(__call) do {                            \
    if ((rv = (__call)) != CRAWDB_OK) {             \
        return rv;                                  \
    }                                               \
} while(0)

#define goto_if_err(__cond, __errv, __labl) do {    \
    if (__cond) {                                   \
        rv = (__errv);                              \
        goto __labl;                                \
    }                                               \
} while(0);

typedef struct crawdb_s crawdb_t;
typedef unsigned char uchar;

struct crawdb_s {
    char *dat_path;
    char *idx_path;
    int dat_fd;
    int idx_fd;
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
CRAWDB_API int crawdb_set(crawdb_t *craw, uchar *key, uint32_t nkey, uchar *val, uint32_t nval);
CRAWDB_API int crawdb_get(crawdb_t *craw, uchar *key, uint32_t nkey, uchar **out_val, uint32_t *out_nval);
CRAWDB_API int crawdb_index(crawdb_t *craw);
CRAWDB_API int crawdb_cksum(uchar *val, uint32_t len, uint16_t *out_cksum);
CRAWDB_API int crawdb_get_nkey(crawdb_t *craw, uint32_t *out_nkey);
CRAWDB_API int crawdb_get_ntotal(crawdb_t *craw, uint64_t *out_ntotal);
CRAWDB_API int crawdb_get_nsorted(crawdb_t *craw, uint64_t *out_nsorted);
CRAWDB_API int crawdb_get_nunsorted(crawdb_t *craw, uint64_t *out_nunsorted);
CRAWDB_API int crawdb_reload(crawdb_t *craw);
CRAWDB_API int crawdb_free(crawdb_t *craw);
