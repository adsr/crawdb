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

#define CRAWDB_OK             0
#define CRAWDB_ERR           -1
#define CRAWDB_HEADER_SIZE    18
#define CRAWDB_OFFSET_NSORTED 9
#define CRAWDB_OFFSET_DEAD    17
#define CRAWDB_API            __attribute__ ((visibility ("default")))

typedef struct crawdb_s crawdb_t;
typedef unsigned char uchar;

struct crawdb_s {
    char *path_idx;
    char *path_dat;
    int fd_idx;
    int fd_dat;
    long size_idx;
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

CRAWDB_API int crawdb_new(char *path_idx, char *path_dat, uint32_t nkey, crawdb_t **out_craw);
CRAWDB_API int crawdb_open(char *path_idx, char *path_dat, crawdb_t **out_craw);
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
