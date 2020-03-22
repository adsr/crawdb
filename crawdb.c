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
idx:
   0 - 4      4: CRAW
   4 - 5      1: vers
   5 - 9      4: nkey
   9 - 17     8: nsorted
  17 - 18     1: dead
[
   0 - n      n: key
   n - n+8    8: offset
 n+8 - n+12   4: len
n+12 - n+14   2: cksum
]*

dat:
<data>
*/

typedef struct crawdb_s crawdb_t;
typedef unsigned char uchar;

struct crawdb_s {
    char *path_idx;
    int fd_idx;
    int fd_dat;
    long size_idx;
    long size_dat;
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

#define CRAWDB_OK             0
#define CRAWDB_ERR           -1
#define CRAWDB_HEADER_SIZE    18
#define CRAWDB_OFFSET_NSORTED 9
#define CRAWDB_OFFSET_DEAD    17

int crawdb_new(char *path_idx, char *path_dat, uint32_t nkey, crawdb_t **out_craw);
int crawdb_open(char *path_idx, char *path_dat, crawdb_t **out_craw);
int _crawdb_new_or_open(int is_new, char *path_idx, char *path_dat, uint32_t nkey, crawdb_t **out_craw);
int crawdb_set(crawdb_t *craw, uchar *key, uint32_t nkey, uchar *val, uint32_t nval);
int crawdb_cksum(uchar *val, uint32_t len, uint16_t *out_cksum);
int crawdb_get(crawdb_t *craw, uchar *key, uint32_t nkey, uchar **out_val, uint32_t *out_nval);
int _crawdb_get_bsearch(crawdb_t *craw, uchar *key, uint32_t nkey, int *out_found, uint64_t *out_offset, uint32_t *out_len, uint16_t *out_cksum);
int _crawdb_get_lsearch(crawdb_t *craw, uchar *key, uint32_t nkey, int *out_found, uint64_t *out_offset, uint32_t *out_len, uint16_t *out_cksum);
int _crawdb_get_data(crawdb_t *craw, uint64_t offset, uint32_t len, uint16_t cksum, uchar **out_val, uint32_t *out_nval);
int crawdb_index(crawdb_t *craw);
int _crawdb_index_copy(crawdb_t *craw, long *out_size_idx, int *out_fd_copy, char **out_path_copy);
int _crawdb_index_sort(crawdb_t *craw, char *path_copy, long size_copy, int *inout_fd_copy, char **out_path_new, int *out_fd_new, long *out_size_new);
int _crawdb_index_swap(crawdb_t *craw, char *path_new, int fd_new, long size_idx, long size_new);
int crawdb_free(crawdb_t *craw);
int _crawdb_lock(crawdb_t *craw);
int _crawdb_unlock(crawdb_t *craw);

int crawdb_new(char *path_idx, char *path_dat, uint32_t nkey, crawdb_t **out_craw) {
    return _crawdb_new_or_open(1, path_idx, path_dat, nkey, out_craw);
}

int crawdb_open(char *path_idx, char *path_dat, crawdb_t **out_craw) {
    return _crawdb_new_or_open(0, path_idx, path_dat, 0, out_craw);
}

int _crawdb_new_or_open(int is_new, char *path_idx, char *path_dat, uint32_t nkey, crawdb_t **out_craw) {
    int fd_idx;
    int fd_dat;
    crawdb_t *craw;
    ssize_t iorv;
    off_t size_idx;
    off_t size_dat;
    uchar header[CRAWDB_HEADER_SIZE];
    int flags;

    fd_idx = -1;
    fd_dat = -1;
    craw = NULL;

    /* Set open flags */
    flags = O_RDWR | O_CREAT | O_APPEND;
    if (is_new) {
        flags |= O_TRUNC;
    }

    /* Open idx */
    fd_idx = open(path_idx, flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd_idx < 0) {
        goto _crawdb_new_or_open_err;
    }

    /* Open dat */
    fd_dat = open(path_dat, flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd_dat < 0) {
        goto _crawdb_new_or_open_err;
    }

    if (is_new) {
        /* Write idx header */
        header[0] = 'C';
        header[1] = 'R';
        header[2] = 'A';
        header[3] = 'W';
        header[4] = 1;                  /* version */
        memcpy(header + 5, &nkey, 4);   /* keylen*/
        memset(header + 9, 0, 8);       /* nsorted */
        header[CRAWDB_OFFSET_DEAD] = 0; /* deadflag */
        iorv = write(fd_idx, header, CRAWDB_HEADER_SIZE);
        if (iorv != CRAWDB_HEADER_SIZE) {
            goto _crawdb_new_or_open_err;
        }
    } else {
        /* Read idx header */
        iorv = read(fd_idx, header, CRAWDB_HEADER_SIZE);
        if (iorv != CRAWDB_HEADER_SIZE) {
            goto _crawdb_new_or_open_err;
        }

        /* Check header */
        if (   header[0] != 'C'
            || header[1] != 'R'
            || header[2] != 'A'
            || header[3] != 'W'
        ) {
            goto _crawdb_new_or_open_err;
        }
    }

    /* Determine idx size */
    /* TODO ensure value makes sense */
    size_idx = lseek(fd_idx, 0, SEEK_END);
    if (size_idx < 0) {
        goto _crawdb_new_or_open_err;
    }

    /* Determine dat size */
    size_dat = lseek(fd_dat, 0, SEEK_END);
    if (size_dat < 0) {
        goto _crawdb_new_or_open_err;
    }

    /* Allocate struct */
    craw = calloc(1, sizeof(crawdb_t));
    craw->path_idx  = strdup(path_idx);
    craw->fd_idx    = fd_idx;
    craw->fd_dat    = fd_dat;
    craw->size_idx  = (uint64_t)size_idx;
    craw->size_dat  = (uint64_t)size_dat;
    craw->vers      = (uint8_t)header[4];
    memcpy(&craw->nkey, header + 5, 4); /* TODO portability */
    memcpy(&craw->nsorted, header + 9, 8);
    craw->dead      = (uint8_t)header[17];
    craw->nrec      = craw->nkey + 8 + 4 + 2; /* key (nkey) + offset (8) + len (4) + cksum (2) */
    craw->ntotal    = (craw->size_idx - CRAWDB_HEADER_SIZE) / craw->nrec;
    if (craw->nsorted > craw->ntotal) {
        craw->nsorted = craw->ntotal; /* TODO error */
    }
    craw->nunsorted = craw->ntotal - craw->nsorted;
    *out_craw = craw;
    return CRAWDB_OK;

_crawdb_new_or_open_err:
    if (fd_idx >= 0) close(fd_idx);
    if (fd_dat >= 0) close(fd_dat);
    return CRAWDB_ERR;
}

int crawdb_set(crawdb_t *craw, uchar *key, uint32_t nkey, uchar *val, uint32_t nval) {
    uint16_t cksum;
    uint8_t dead;
    off_t offset;
    uint64_t offset64;
    ssize_t iorv;

    /* Calc checksum */
    crawdb_cksum(val, nval, &cksum);

    /* Prep rec */
    if (nkey > craw->nkey) {
        nkey = craw->nkey; /* TODO error */
    }
    if (!craw->rec) craw->rec = malloc(craw->nrec);
    memset(craw->rec, 0, craw->nrec);
    memcpy(craw->rec,                   key,    nkey);  /* [0    -> n]    key    (n) */
                                                        /* [n    -> n+8]  offset (8) (below) */
    memcpy(craw->rec + craw->nkey + 8,  &nval,  4);     /* [n+8  -> n+12] len    (4) */
    memcpy(craw->rec + craw->nkey + 12, &cksum, 2);     /* [n+12 -> n+14] cksum  (2) */

    /* Lock */
    if (_crawdb_lock(craw) != CRAWDB_OK) {
        return CRAWDB_ERR;
    }

    /* Check dead flag */
    dead = 0;
    if (pread(craw->fd_idx, &dead, 1, CRAWDB_OFFSET_DEAD) != 1 || dead != 0) {
        goto crawdb_set_err;
    }

    /* Get dat offset */
    offset = lseek(craw->fd_dat, 0, SEEK_END);
    if (offset < 0) {
        goto crawdb_set_err;
    }

    /* Set offset in rec */
    offset64 = (uint64_t)offset;
    memcpy(craw->rec + craw->nkey,      &offset64,  8); /* [n    -> n+8]  offset (8) */

    /* Write dat */
    iorv = write(craw->fd_dat, val, (size_t)nval);
    if (iorv != (ssize_t)nval) {
        goto crawdb_set_err;
    }

    /* Write idx rec */
    iorv = write(craw->fd_idx, craw->rec, craw->nrec);
    if (iorv != (ssize_t)craw->nrec) {
        goto crawdb_set_err;
    }

    /* Unlock */
    _crawdb_unlock(craw);
    return CRAWDB_OK;

crawdb_set_err:

    _crawdb_unlock(craw);
    return CRAWDB_ERR;
}


int crawdb_get(crawdb_t *craw, uchar *okey, uint32_t nokey, uchar **out_val, uint32_t *out_nval) {
    int rv;
    int found;
    uint64_t offset;
    uint32_t len;
    uint16_t cksum;
    uchar *key;

    found = 0;

    /* Allocate rec */
    if (!craw->rec) craw->rec = malloc(craw->nrec);

    /* Pad key */
    if (nokey < craw->nkey) {
        key = calloc(1, craw->nkey);
        memcpy(key, okey, nokey);
    } else if (nokey == craw->nkey) {
        key = okey;
    } else {
        /* TODO error nokey > craw->nkey */
        key = okey;
    }

    if (craw->nsorted > 0) {
        /* Try binary search */
        rv = _crawdb_get_bsearch(craw, key, craw->nkey, &found, &offset, &len, &cksum);
        if (rv != CRAWDB_OK) {
            return rv;
        }
    }

    if (!found) {
        if (craw->nunsorted > 0) {
            /* Try linear search */
            rv =_crawdb_get_lsearch(craw, key, craw->nkey, &found, &offset, &len, &cksum);
            if (rv != CRAWDB_OK) {
                return rv;
            }
        }
    }

    /* Key not found */
    if (!found) {
        return CRAWDB_ERR;
    }

    /* Fetch data */
    return _crawdb_get_data(craw, offset, len, cksum, out_val, out_nval);
}

int crawdb_cksum(uchar *val, uint32_t len, uint16_t *out_cksum) {
    int i;
    uint16_t data;
    uint16_t crc;

    crc = 0xffff;

    if (len <= 0) {
        *out_cksum = ~crc;
        return CRAWDB_OK;
    }

    do {
        for (i = 0, data=0x00ff & (uint16_t)(*val++); i < 8; i++, data >>= 1) {
            if ((crc & 0x0001) ^ (data & 0x0001)) {
                crc = (crc >> 1) ^ 0x8408;
            } else {
                crc >>= 1;
            }
        }
    } while (--len);

    crc = ~crc;
    data = crc;
    crc = (crc << 8) | (data >> 8 & 0xff);

    *out_cksum = crc;
    return CRAWDB_OK;
}

int _crawdb_get_bsearch(crawdb_t *craw, uchar *key, uint32_t nkey, int *out_found, uint64_t *out_offset, uint32_t *out_len, uint16_t *out_cksum) {
    uint64_t start;
    uint64_t end;
    uint64_t look;
    uint64_t look_offset;
    int rv;

    start = 0;
    end = craw->nsorted - 1;

    while (end >= start) {
        look = (start + end) / 2;
        look_offset = CRAWDB_HEADER_SIZE + (look * craw->nrec);
        if (pread(craw->fd_idx, craw->rec, craw->nrec, look_offset) != craw->nrec) {
            return CRAWDB_ERR;
        }
        rv = memcmp(craw->rec, key, nkey);
        if (rv == 0) {
            memcpy(out_offset, craw->rec + nkey, 8);
            memcpy(out_len,    craw->rec + nkey + 8, 4);
            memcpy(out_cksum,  craw->rec + nkey + 8 + 4, 2);
            *out_found = 1;
            return CRAWDB_OK;
        } else if (rv < 0) {
            start = look + 1;
        } else if (rv > 0) {
            end = look - 1;
        }
    }

    *out_found = 0;
    return CRAWDB_OK;
}

int _crawdb_get_lsearch(crawdb_t *craw, uchar *key, uint32_t nkey, int *out_found, uint64_t *out_offset, uint32_t *out_len, uint16_t *out_cksum) {
    uint64_t start;
    uint64_t end;
    uint64_t look;
    uint64_t look_offset;
    int rv;

    start = 0;
    end = craw->nunsorted - 1;

    for (look = end; look >= start; look--) {
        look_offset = CRAWDB_HEADER_SIZE + ((craw->nsorted + look) * craw->nrec);
        if (pread(craw->fd_idx, craw->rec, craw->nrec, look_offset) != craw->nrec) {
            return CRAWDB_ERR;
        }
        rv = memcmp(craw->rec, key, nkey);
        if (rv == 0) {
            memcpy(out_offset, craw->rec + nkey, 8);
            memcpy(out_len,    craw->rec + nkey + 8, 4);
            memcpy(out_cksum,  craw->rec + nkey + 8 + 4, 2);
            *out_found = 1;
            return CRAWDB_OK;
        }
    }

    *out_found = 0;
    return CRAWDB_OK;
}

int _crawdb_get_data(crawdb_t *craw, uint64_t offset, uint32_t len, uint16_t cksum, uchar **out_val, uint32_t *out_nval) {
    uint16_t dat_cksum;

    /* Allocate data buf */
    if (!craw->data) {
        craw->data = malloc(len);
    } else if (len > craw->ndata) {
        craw->data = realloc(craw->data, len);
    }

    /* Read from dat file */
    if (pread(craw->fd_dat, craw->data, len, offset) != len) {
        return CRAWDB_ERR;
    }

    /* Calc and compare checksum */
    dat_cksum = 0;
    crawdb_cksum(craw->data, len, &dat_cksum);
    if (dat_cksum != cksum) {
        return CRAWDB_ERR;
    }

    *out_val = craw->data;
    *out_nval = len;
    return CRAWDB_OK;
}

int crawdb_index(crawdb_t *craw) {
    char *path_copy;
    char *path_new;
    int fd_copy;
    int fd_new;
    int fd_idx;
    long size_idx;
    long size_new;
    int rv;

    path_copy = NULL;
    path_new = NULL;
    fd_copy = -1;
    fd_new = -1;

    /* Reopen fd_idx without O_APPEND */
    fd_idx = open(craw->path_idx, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd_idx < 0) {
        goto crawdb_index_err;
    }
    close(craw->fd_idx);
    craw->fd_idx = fd_idx;

    /* Copy index */
    if (_crawdb_index_copy(craw, &size_idx, &fd_copy, &path_copy) != CRAWDB_OK) {
        goto crawdb_index_err;
    }

    /* Sort copy of index into new index */
    fd_new = -1;
    if (_crawdb_index_sort(craw, path_copy, size_idx, &fd_copy, &path_new, &fd_new, &size_new) != CRAWDB_OK) {
        goto crawdb_index_err;
    }

    /* Swap in new index */
    if (_crawdb_index_swap(craw, path_new, fd_new, size_idx, size_new) != CRAWDB_OK) {
        goto crawdb_index_err;
    }

    /* Reopen fd_idx with O_APPEND */
    fd_idx = open(craw->path_idx, O_RDWR | O_CREAT | O_APPEND, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd_idx < 0) {
        goto crawdb_index_err;
    }
    close(craw->fd_idx);
    craw->fd_idx = fd_idx;
    fd_idx = -1;

    rv = CRAWDB_OK;
    goto crawdb_index_ok;

crawdb_index_err:
    rv = CRAWDB_ERR;

crawdb_index_ok:
    if (path_copy) free(path_copy);
    if (path_new) free(path_new);
    if (fd_idx >= 0) close(fd_idx);
    return rv;
}

int _crawdb_index_copy(crawdb_t *craw, long *out_size_idx, int *out_fd_copy, char **out_path_copy) {
    char *path_copy;
    size_t path_copy_len;
    int fd_copy;
    long size_idx;
    ssize_t iorv;

    path_copy = NULL;
    fd_copy = -1;

    /* Lock */
    if (_crawdb_lock(craw) != CRAWDB_OK) {
        return CRAWDB_ERR;
    }

    /* Determine idx size */
    /* TODO ensure value makes sense */
    size_idx = lseek(craw->fd_idx, 0, SEEK_END);
    if (size_idx < 0) {
        goto _crawdb_index_copy_err;
    }

    /* Open file */
    path_copy_len = strlen(craw->path_idx) + 5; /* ".copy" (5) */
    path_copy = malloc(path_copy_len + 1);
    snprintf(path_copy, path_copy_len + 1, "%s.copy", craw->path_idx);
    fd_copy = open(path_copy, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd_copy < 0) {
        goto _crawdb_index_copy_err;
    }

    /* Copy index */
    if (lseek(craw->fd_idx, 0, SEEK_SET) < 0) {
        goto _crawdb_index_copy_err;
    }
    if (lseek(fd_copy, 0, SEEK_SET) < 0) {
        goto _crawdb_index_copy_err;
    }
    iorv = copy_file_range(craw->fd_idx, NULL, fd_copy, NULL, (size_t)craw->size_idx, 0);
    if (iorv != (ssize_t)craw->size_idx) {
        goto _crawdb_index_copy_err;
    }

    *out_fd_copy = fd_copy;
    *out_size_idx = size_idx;
    *out_path_copy = path_copy;
    _crawdb_unlock(craw);
    return CRAWDB_OK;

_crawdb_index_copy_err:
    _crawdb_unlock(craw);
    if (path_copy) free(path_copy);
    if (fd_copy >= 0) close(fd_copy);
    return CRAWDB_ERR;
}

int _crawdb_index_sort_cmp(const void *a, const void *b, void *arg) {
    crawdb_t *craw;
    craw = arg;
    return memcmp(a, b, craw->nkey);
}

int _crawdb_index_sort(crawdb_t *craw, char *path_copy, long size_copy, int *inout_fd_copy, char **out_path_new, int *out_fd_new, long *out_size_new) {
    char *path_new;
    int fd_new;
    long size_new;
    size_t path_new_len;
    ssize_t iorv;
    uchar *buf;
    uint64_t i;

    buf = NULL;
    path_new = NULL;
    fd_new = -1;

    /* Open file */
    path_new_len = strlen(craw->path_idx) + 4; /* ".new" (4) */
    path_new = malloc(path_new_len + 1);
    snprintf(path_new, path_new_len + 1, "%s.new", craw->path_idx);
    fd_new = open(path_new, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd_new < 0) {
        goto _crawdb_index_sort_err;
    }

    /* Read copy into memory for sorting */
    buf = malloc(size_copy);
    if (lseek(*inout_fd_copy, CRAWDB_HEADER_SIZE, SEEK_SET) < 0) {
        goto _crawdb_index_sort_err;
    }
    iorv = read(*inout_fd_copy, buf, size_copy - CRAWDB_HEADER_SIZE);
    if (iorv != size_copy - CRAWDB_HEADER_SIZE) {
        goto _crawdb_index_sort_err;
    }

    /* Copy header */
    if (lseek(*inout_fd_copy, 0, SEEK_SET) < 0) {
        goto _crawdb_index_sort_err;
    }
    if (copy_file_range(*inout_fd_copy, NULL, fd_new, NULL, CRAWDB_HEADER_SIZE, 0) != CRAWDB_HEADER_SIZE) {
        goto _crawdb_index_sort_err;
    }

    /* Set nsorted */
    if (pwrite(fd_new, &craw->ntotal, 8, CRAWDB_OFFSET_NSORTED) != 8) {
        return CRAWDB_ERR;
    }

    /* Close and delete copy file */
    close(*inout_fd_copy);
    *inout_fd_copy = -1;
    unlink(path_copy);

    /* Sort records */
    qsort_r(buf, craw->ntotal, craw->nrec, _crawdb_index_sort_cmp, craw);

    /* Write sorted records to new */
    for (i = 0; i < craw->ntotal; i++) {
        iorv = pwrite(fd_new, buf + (craw->nrec * i), craw->nrec, CRAWDB_HEADER_SIZE + (craw->nrec * i));
        if (iorv != craw->nrec) {
            goto _crawdb_index_sort_err;
        }
    }
    free(buf);

    /* Get size of new file */
    /* TODO ensure value makes sense */
    size_new = lseek(fd_new, 0, SEEK_END);
    if (size_new < 0) {
        goto _crawdb_index_sort_err;
    }

    *out_fd_new = fd_new;
    *out_path_new = path_new;
    *out_size_new = size_new;
    return CRAWDB_OK;

_crawdb_index_sort_err:
    if (buf) free(buf);
    if (path_new) free(path_new);
    if (fd_new >= 0) close(fd_new);
    return CRAWDB_ERR;
}

int _crawdb_index_swap(crawdb_t *craw, char *path_new, int fd_new, long size_idx, long size_new) {
    loff_t offset_dst;
    loff_t offset_src;
    long size_idx_again;
    size_t copy_len;
    uint8_t dead;

    /* Lock */
    if (_crawdb_lock(craw) != CRAWDB_OK) {
        return CRAWDB_ERR;
    }

    /* Determine idx size again */
    /* TODO ensure value makes sense */
    size_idx_again = lseek(craw->fd_idx, 0, SEEK_END);
    if (size_idx_again < 0) {
        return CRAWDB_ERR;
    }

    /* Copy records that came in while we were indexing */
    if (size_idx_again > size_idx) {
        copy_len = (size_t)(size_idx_again - size_idx);
        offset_src = (loff_t)size_idx;
        offset_dst = (loff_t)size_new;
        if (copy_file_range(craw->fd_idx, &offset_src, fd_new, &offset_dst, copy_len, 0) != (ssize_t)copy_len) {
            return CRAWDB_ERR;
        }
    }

    /* Rename idx to new */
    if (rename(path_new, craw->path_idx) != 0) {
        return CRAWDB_ERR;
    }

    /* Write dead byte on old idx */
    /* TODO Have to open without O_APPEND for this to work */
    dead = 1;
    if (pwrite(craw->fd_idx, &dead, 1, CRAWDB_OFFSET_DEAD) != 1) {
        return CRAWDB_ERR;
    }

    _crawdb_unlock(craw);
    return CRAWDB_OK;
}

int crawdb_free(crawdb_t *craw) {
    if (craw->fd_idx >= 0) close(craw->fd_idx);
    if (craw->fd_dat >= 0) close(craw->fd_dat);
    if (craw->rec) free(craw->rec);
    free(craw->path_idx);
    free(craw);
    return CRAWDB_OK;
}

int _crawdb_lock(crawdb_t *craw) {
    if (flock(craw->fd_idx, LOCK_EX) != 0) {
        return CRAWDB_ERR;
    }
    return CRAWDB_OK;
}

int _crawdb_unlock(crawdb_t *craw) {
    if (flock(craw->fd_idx, LOCK_UN) != 0) {
        return CRAWDB_ERR;
    }
    return CRAWDB_OK;
}

void usage(FILE *fp, int exit_code) {
    fprintf(fp, "Usage:\n");
    fprintf(fp, "  crawdb -i <idx> -d <dat> -N\n");
    fprintf(fp, "  crawdb -i <idx> -d <dat> -S -k key -v val\n");
    fprintf(fp, "  crawdb -i <idx> -d <dat> -G -k key\n");
    fprintf(fp, "  crawdb -i <idx> -d <dat> -I\n");
    fprintf(fp, "\n");
    fprintf(fp, "Options:\n");
    fprintf(fp, "  -h, --help             Show this help\n");
    fprintf(fp, "  -N, --action-init      Init a database (use with -n)\n");
    fprintf(fp, "  -S, --action-set       Set data (use with -k, -v)\n");
    fprintf(fp, "  -G, --action-get       Get data (use with -k)\n");
    fprintf(fp, "  -I, --action-index     Index a database\n");
    fprintf(fp, "  -i, --path-idx=<path>  Use index file at `path`\n");
    fprintf(fp, "  -d, --path-dat=<path>  Use data file at `path`\n");
    fprintf(fp, "  -k, --key=<key>        Set or get `key`\n");
    fprintf(fp, "  -v, --val=<val>        Set `key` to `val`\n");
    fprintf(fp, "  -n, --key-size=<n>     Set key size to `n` (default=32)\n");
    exit(exit_code);
}

int main(int argc, char **argv) {
    char action;
    char *dat;
    char *idx;
    char *key;
    crawdb_t *craw;
    int c;
    int help;
    int rv;
    char *val;
    uchar *oval;
    uint32_t nkey;
    uint32_t nval;

    action = 0;
    dat = NULL;
    idx = NULL;
    key = NULL;
    craw = NULL;
    help = 0;
    rv = 0;
    val = NULL;
    oval = NULL;
    nkey = 0;
    nval = 0;

    struct option long_opts[] = {
        { "help",         no_argument,       NULL, 'h' },
        { "path-idx",     required_argument, NULL, 'i' },
        { "path-dat",     required_argument, NULL, 'd' },
        { "key",          required_argument, NULL, 'k' },
        { "val",          required_argument, NULL, 'v' },
        { "action-init",  no_argument,       NULL, 'N' },
        { "action-set",   no_argument,       NULL, 'S' },
        { "action-get",   no_argument,       NULL, 'G' },
        { "action-index", no_argument,       NULL, 'I' },
        { "key-size",     required_argument, NULL, 'n' },
        { 0,              0,                 0,    0   }
    };

    while ((c = getopt_long(argc, argv, "hi:d:k:v:NSGIn:", long_opts, NULL)) != -1) {
        switch (c) {
            case 'h': help = 1;      break;
            case 'i': idx = optarg;  break;
            case 'd': dat = optarg;  break;
            case 'k': key = optarg;  break;
            case 'v': val = optarg;  break;
            case 'N':
            case 'S':
            case 'G':
            case 'I': action = c;    break;
            case 'n': nkey = strtol(optarg, NULL, 10); break;
        }
    }

    if (help) {
        usage(stdout, 0);
    }

    if (!idx || !dat) {
        fprintf(stderr, "Expected `--path-idx` and `--path-dat`\n");
        usage(stderr, 0);
    }

    if (strchr("SGI", action) != NULL) {
        if ((rv = crawdb_open(idx, dat, &craw)) != CRAWDB_OK) {
            goto main_err;
        }
    }

    switch (action) {
        case 'N':
            nkey = (nkey < 1 ? 32 : nkey);
            rv = crawdb_new(idx, dat, nkey, &craw);
            break;
        case 'S':
            if (!key || !val) {
                fprintf(stderr, "Expected `--key` and `--val` with `--action-set`\n");
                usage(stderr, 1);
            }
            rv = crawdb_set(craw, (uchar*)key, strlen(key), (uchar*)val, strlen(val));
            break;
        case 'G':
            if (!key) {
                fprintf(stderr, "Expected `--key` with `--action-get`\n");
                usage(stderr, 1);
            }
            if ((rv = crawdb_open(idx, dat, &craw)) != CRAWDB_OK) {
                goto main_err;
            }
            rv = crawdb_get(craw, (uchar*)key, strlen(key), &oval, &nval);
            if (rv == CRAWDB_OK) {
                write(STDOUT_FILENO, oval, nval);
            }
            break;
        case 'I':
            rv = crawdb_index(craw);
            break;
        default:
            fprintf(stderr, "Expected `--action-*` param\n");
            usage(stderr, 1);
            break;
    }

    if (craw) crawdb_free(craw);

main_err:

    return rv;
}
