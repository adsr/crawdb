#include "crawdb.h"

static int _crawdb_get_bsearch(crawdb_t *craw, uchar *key, uint32_t nkey, int *out_found, uint64_t *out_offset, uint32_t *out_len, uint16_t *out_cksum);
static int _crawdb_get_lsearch(crawdb_t *craw, uchar *key, uint32_t nkey, int *out_found, uint64_t *out_offset, uint32_t *out_len, uint16_t *out_cksum);
static int _crawdb_get_data(crawdb_t *craw, uint64_t offset, uint32_t len, uint16_t cksum, uchar **out_val, uint32_t *out_nval);
static int _crawdb_index_copy(crawdb_t *craw, int *out_fd_copy, char **out_path_copy);
static int _crawdb_index_sort_cmp(const void *a, const void *b, void *arg);
static int _crawdb_index_sort(crawdb_t *craw, char *path_copy, int *inout_fd_copy, char **out_path_new, int *out_fd_new, long *out_size_new);
static int _crawdb_index_swap(crawdb_t *craw, char *path_new, int fd_new, long size_new);
static int _crawdb_reload_for_index(crawdb_t *craw);
static int _crawdb_open(int is_new, int for_index, crawdb_t *reload, char *idx_path, char *dat_path, uint32_t nkey, crawdb_t **out_craw);
static int _crawdb_set_idx_size(crawdb_t *craw, uint64_t idx_size);
static int _crawdb_lock(crawdb_t *craw);
static int _crawdb_unlock(crawdb_t *craw);
static int _crawdb_unlock_if_locked(crawdb_t *craw);

int crawdb_new(char *idx_path, char *dat_path, uint32_t nkey, crawdb_t **out_craw) {
    return _crawdb_open(1, 0, NULL, idx_path, dat_path, nkey, out_craw);
}

int crawdb_open(char *idx_path, char *dat_path, crawdb_t **out_craw) {
    return _crawdb_open(0, 0, NULL, idx_path, dat_path, 0, out_craw);
}

int crawdb_reload(crawdb_t *craw) {
    crawdb_t *craw_ignore;
    return _crawdb_open(0, 0, craw, craw->idx_path, craw->dat_path, 0, &craw_ignore);
}

int crawdb_set(crawdb_t *craw, uchar *key, uint32_t nkey, uchar *val, uint32_t nval) {
    int rv;
    off_t offset;
    ssize_t iorv;
    uint16_t cksum;
    uint64_t offset64;
    uint8_t dead;

    /* Check key len */
    goto_if_err(nkey > craw->nkey, CRAWDB_ERR_SET_BAD_KEY, crawdb_set_err);
    goto_if_err(nkey < 1,          CRAWDB_ERR_SET_BAD_KEY, crawdb_set_err);

    /* Calc checksum */
    crawdb_cksum(val, nval, &cksum);

    /* Prep index record */
    if (!craw->rec) {
        craw->rec = malloc(craw->nrec);
    }
    memset(craw->rec, 0, craw->nrec);
    memcpy(craw->rec,                   key,    nkey);  /* [0    -> n]    key    (n) */
                                                        /* [n    -> n+8]  offset (8) (below) */
    memcpy(craw->rec + craw->nkey + 8,  &nval,  4);     /* [n+8  -> n+12] len    (4) */
    memcpy(craw->rec + craw->nkey + 12, &cksum, 2);     /* [n+12 -> n+14] cksum  (2) */

    /* Lock */
    try(_crawdb_lock(craw));

    /* Check dead flag */
    dead = 0;
    iorv = pread(craw->fd_idx, &dead, 1, CRAWDB_OFFSET_DEAD);
    goto_if_err(iorv != 1, CRAWDB_ERR_SET_PREAD_DEAD, crawdb_set_err);
    goto_if_err(dead != 0, CRAWDB_ERR_SET_IDX_DEAD, crawdb_set_err);

    /* Get dat offset */
    offset = lseek(craw->fd_dat, 0, SEEK_END);
    goto_if_err(offset < 0, CRAWDB_ERR_SET_LSEEK, crawdb_set_err);

    /* Set offset in index record */
    offset64 = (uint64_t)offset;
    memcpy(craw->rec + craw->nkey,      &offset64,  8); /* [n    -> n+8]  offset (8) */

    /* Write dat */
    iorv = write(craw->fd_dat, val, (size_t)nval);
    goto_if_err(iorv != (ssize_t)nval, CRAWDB_ERR_SET_WRITE_DAT, crawdb_set_err);

    /* Write idx rec */
    iorv = write(craw->fd_idx, craw->rec, craw->nrec);
    goto_if_err(iorv != (ssize_t)craw->nrec, CRAWDB_ERR_SET_WRITE_IDX, crawdb_set_err);

    /* Unlock */
    try(_crawdb_unlock(craw));
    return CRAWDB_OK;

crawdb_set_err:
    _crawdb_unlock_if_locked(craw);
    return rv;
}


int crawdb_get(crawdb_t *craw, uchar *orig_key, uint32_t orig_nkey, uchar **out_val, uint32_t *out_nval) {
    int rv;
    int rc;
    int found;
    uint64_t offset;
    uint32_t len;
    uint16_t cksum;
    uchar *key;

    /* Maybe allocate rec */
    if (!craw->rec) {
        craw->rec = malloc(craw->nrec);
    }

    /* Check key len */
    goto_if_err(orig_nkey > craw->nkey, CRAWDB_ERR_GET_BAD_KEY, crawdb_get_err);
    goto_if_err(orig_nkey < 1,          CRAWDB_ERR_GET_BAD_KEY, crawdb_get_err);

    /* Maybe pad key */
    if (orig_nkey < craw->nkey) {
        key = calloc(1, craw->nkey); /* TODO scratch key buffer */
        memcpy(key, orig_key, orig_nkey);
    } else { /* orig_nkey == craw->nkey */
        key = orig_key;
    }

    found = 0;

    if (craw->nsorted > 0) {
        /* Try binary search */
        rc = _crawdb_get_bsearch(craw, key, craw->nkey, &found, &offset, &len, &cksum);
        goto_if_err(rc != CRAWDB_OK, rc, crawdb_get_err);
    }

    if (!found) {
        if (craw->nunsorted > 0) {
            /* Try linear search */
            rc =_crawdb_get_lsearch(craw, key, craw->nkey, &found, &offset, &len, &cksum);
            goto_if_err(rc != CRAWDB_OK, rc, crawdb_get_err);
        }
    }

    /* Key not found */
    if (!found) {
        *out_val = NULL;
        *out_nval = 0;
        goto crawdb_get_ok;
    }

    /* Fetch data */
    rc = _crawdb_get_data(craw, offset, len, cksum, out_val, out_nval);
    goto_if_err(rc != CRAWDB_OK, rc, crawdb_get_err);

crawdb_get_ok:
    if (key != orig_key) free(key); /* TODO scratch key buffer */
    return CRAWDB_OK;

crawdb_get_err:
    return rv;
}

int crawdb_cksum(uchar *val, uint32_t len, uint16_t *out_cksum) {
    int i;
    uint16_t data;
    uint16_t crc;

    /* Apply CRC-16 checksum algorithm */

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


int crawdb_index(crawdb_t *craw) {
    int rv;
    int rc;
    char *path_copy;
    char *path_new;
    int fd_copy;
    int fd_new;
    long size_new;

    path_copy = NULL;
    path_new = NULL;
    fd_copy = -1;
    fd_new = -1;

    /* Copy index */
    rc =_crawdb_index_copy(craw, &fd_copy, &path_copy);
    goto_if_err(rc != CRAWDB_OK, rc, crawdb_index_end);

    /* Sort into new index */
    fd_new = -1;
    rc = _crawdb_index_sort(craw, path_copy, &fd_copy, &path_new, &fd_new, &size_new);
    goto_if_err(rc != CRAWDB_OK, rc, crawdb_index_end);

    /* Swap in new index and copy intermediate records */
    rc = _crawdb_index_swap(craw, path_new, fd_new, size_new);
    goto_if_err(rc != CRAWDB_OK, rc, crawdb_index_end);

    rv = CRAWDB_OK;

crawdb_index_end:
    if (path_copy) free(path_copy);
    if (path_new) free(path_new);
    if (fd_copy >= 0) close(fd_copy);
    if (fd_new >= 0) close(fd_new);
    return rv;
}

int crawdb_get_nkey(crawdb_t *craw, uint32_t *out_nkey) {
    *out_nkey = craw->nkey;
    return CRAWDB_OK;
}

int crawdb_get_ntotal(crawdb_t *craw, uint64_t *out_ntotal) {
    *out_ntotal = craw->ntotal;
    return CRAWDB_OK;
}

int crawdb_get_nsorted(crawdb_t *craw, uint64_t *out_nsorted) {
    *out_nsorted = craw->nsorted;
    return CRAWDB_OK;
}

int crawdb_get_nunsorted(crawdb_t *craw, uint64_t *out_nunsorted) {
    *out_nunsorted = craw->nunsorted;
    return CRAWDB_OK;
}

int crawdb_get_keylen(crawdb_t *craw, uint32_t *out_nkey) {
    *out_nkey = craw->nkey;
    return CRAWDB_OK;
}

int crawdb_free(crawdb_t *craw) {
    if (craw->fd_idx >= 0) close(craw->fd_idx);
    if (craw->fd_dat >= 0) close(craw->fd_dat);
    if (craw->rec) free(craw->rec);
    if (craw->data) free(craw->data);
    free(craw->idx_path);
    free(craw->dat_path);
    free(craw);
    return CRAWDB_OK;
}

static int _crawdb_get_bsearch(crawdb_t *craw, uchar *key, uint32_t nkey, int *out_found, uint64_t *out_offset, uint32_t *out_len, uint16_t *out_cksum) {
    uint64_t start;
    uint64_t end;
    uint64_t look;
    uint64_t look_offset;
    int rv;

    start = 0;
    end = craw->nsorted - 1;

    /* Binary search sorted idx records for key */
    while (end >= start) {
        look = (start + end) / 2;
        look_offset = CRAWDB_HEADER_SIZE + (look * craw->nrec);
        if (pread(craw->fd_idx, craw->rec, craw->nrec, look_offset) != craw->nrec) {
            return CRAWDB_ERR_BSEARCH;
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

static int _crawdb_get_lsearch(crawdb_t *craw, uchar *key, uint32_t nkey, int *out_found, uint64_t *out_offset, uint32_t *out_len, uint16_t *out_cksum) {
    uint64_t start;
    uint64_t end;
    uint64_t look;
    uint64_t look_offset;
    int rv;

    start = 0;
    end = craw->nunsorted - 1;

    /* Reverse linear search unsorted idx records for key */
    for (look = end; look >= start; look--) {
        look_offset = CRAWDB_HEADER_SIZE + ((craw->nsorted + look) * craw->nrec);
        if (pread(craw->fd_idx, craw->rec, craw->nrec, look_offset) != craw->nrec) {
            return CRAWDB_ERR_LSEARCH;
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

static int _crawdb_get_data(crawdb_t *craw, uint64_t offset, uint32_t len, uint16_t cksum, uchar **out_val, uint32_t *out_nval) {
    uint16_t dat_cksum;

    /* Allocate data buf */
    if (!craw->data) {
        craw->data = malloc(len);
    } else if (len > craw->ndata) {
        craw->data = realloc(craw->data, len);
    }

    /* Read from dat file */
    if (pread(craw->fd_dat, craw->data, len, offset) != len) {
        return CRAWDB_ERR_GET_DATA_READ;
    }

    /* Calc and compare checksum */
    dat_cksum = 0;
    crawdb_cksum(craw->data, len, &dat_cksum);
    if (dat_cksum != cksum) {
        return CRAWDB_ERR_GET_DATA_CKSUM;
    }

    *out_val = craw->data;
    *out_nval = len;
    return CRAWDB_OK;
}

static int _crawdb_index_copy(crawdb_t *craw, int *out_fd_copy, char **out_path_copy) {
    int rv;
    int rc;
    char *path_copy;
    size_t path_copy_len;
    int fd_copy;
    ssize_t iorv;
    off_t offset;

    path_copy = NULL;
    fd_copy = -1;

    /* Lock */
    rc = _crawdb_lock(craw);
    goto_if_err(rc != CRAWDB_OK, rc, _crawdb_index_copy_err);

    /* Reload without O_APPEND */
    rc = _crawdb_reload_for_index(craw);
    goto_if_err(rc != CRAWDB_OK, rc, _crawdb_index_copy_err);

    /* Open copy file */
    path_copy_len = strlen(craw->idx_path) + 5; /* ".copy" (5) */
    path_copy = malloc(path_copy_len + 1);
    snprintf(path_copy, path_copy_len + 1, "%s.copy", craw->idx_path);
    fd_copy = open(path_copy, O_RDWR | O_CREAT | O_TRUNC, 00644);
    goto_if_err(fd_copy < 0, CRAWDB_ERR_INDEX_OPEN_COPY, _crawdb_index_copy_err);

    /* Copy index */
    offset = lseek(craw->fd_idx, 0, SEEK_SET);
    goto_if_err(offset < 0, CRAWDB_ERR_INDEX_LSEEK, _crawdb_index_copy_err);
    offset = lseek(fd_copy, 0, SEEK_SET);
    goto_if_err(offset < 0, CRAWDB_ERR_INDEX_LSEEK, _crawdb_index_copy_err);
    iorv = copy_file_range(craw->fd_idx, NULL, fd_copy, NULL, (size_t)craw->idx_size, 0);
    goto_if_err(iorv != (ssize_t)craw->idx_size, CRAWDB_ERR_INDEX_COPY, _crawdb_index_copy_err);

    /* Unlock */
    rc = _crawdb_unlock(craw);
    goto_if_err(rc != CRAWDB_OK, rc, _crawdb_index_copy_err);

    *out_fd_copy = fd_copy;
    *out_path_copy = path_copy;
    return CRAWDB_OK;

_crawdb_index_copy_err:
    _crawdb_unlock_if_locked(craw);
    if (path_copy) free(path_copy);
    if (fd_copy >= 0) close(fd_copy);
    return rv;
}

static int _crawdb_index_sort_cmp(const void *a, const void *b, void *arg) {
    crawdb_t *craw;
    craw = arg;
    return memcmp(a, b, craw->nkey);
}

static int _crawdb_index_sort(crawdb_t *craw, char *path_copy, int *inout_fd_copy, char **out_path_new, int *out_fd_new, long *out_size_new) {
    int rv;
    char *path_new;
    int fd_new;
    long size_new;
    size_t path_new_len;
    ssize_t iorv;
    uchar *buf;
    uint64_t i;
    off_t offset;

    path_new = NULL;
    fd_new = -1;
    buf = NULL;

    /* Open file */
    path_new_len = strlen(craw->idx_path) + 4; /* ".new" (4) */
    path_new = malloc(path_new_len + 1);
    snprintf(path_new, path_new_len + 1, "%s.new", craw->idx_path);
    fd_new = open(path_new, O_RDWR | O_CREAT | O_TRUNC, 00644);
    goto_if_err(fd_new < 0, CRAWDB_ERR_SORT_OPEN_NEW, _crawdb_index_sort_err);

    /* Read copy into memory for sorting */
    buf = malloc(craw->idx_size);
    iorv = pread(*inout_fd_copy, buf, craw->idx_size - CRAWDB_HEADER_SIZE, CRAWDB_HEADER_SIZE);
    goto_if_err(iorv != craw->idx_size - CRAWDB_HEADER_SIZE, CRAWDB_ERR_SORT_READ, _crawdb_index_sort_err);

    /* Copy header */
    offset = lseek(*inout_fd_copy, 0, SEEK_SET);
    goto_if_err(offset < 0, CRAWDB_ERR_SORT_LSEEK, _crawdb_index_sort_err);
    iorv = copy_file_range(*inout_fd_copy, NULL, fd_new, NULL, CRAWDB_HEADER_SIZE, 0);
    goto_if_err(iorv != CRAWDB_HEADER_SIZE, CRAWDB_ERR_SORT_COPY_HEADER, _crawdb_index_sort_err);

    /* Set nsorted */
    iorv = pwrite(fd_new, &craw->ntotal, 8, CRAWDB_OFFSET_NSORTED);
    goto_if_err(iorv != 8, CRAWDB_ERR_SORT_WRITE_NSORTED, _crawdb_index_sort_err);

    /* Close and delete copy file */
    close(*inout_fd_copy);
    *inout_fd_copy = -1;
    unlink(path_copy);

    /* Sort records */
    qsort_r(buf, craw->ntotal, craw->nrec, _crawdb_index_sort_cmp, craw);

    /* Write sorted records to new */
    for (i = 0; i < craw->ntotal; i++) {
        iorv = pwrite(fd_new, buf + (craw->nrec * i), craw->nrec, CRAWDB_HEADER_SIZE + (craw->nrec * i));
        goto_if_err(iorv != craw->nrec, CRAWDB_ERR_SORT_WRITE_REC, _crawdb_index_sort_err);
    }
    free(buf);

    /* Get size of new file */
    size_new = lseek(fd_new, 0, SEEK_END);
    goto_if_err(size_new < 0, CRAWDB_ERR_SORT_LSEEK, _crawdb_index_sort_err);

    *out_path_new = path_new;
    *out_fd_new = fd_new;
    *out_size_new = size_new;
    return CRAWDB_OK;

_crawdb_index_sort_err:
    if (path_new) free(path_new);
    if (fd_new >= 0) close(fd_new);
    if (buf) free(buf);
    return rv;
}

static int _crawdb_index_swap(crawdb_t *craw, char *path_new, int fd_new, long size_new) {
    int rv;
    int rc;
    ssize_t iorv;
    loff_t offset_dst;
    loff_t offset_src;
    long idx_size_after;
    size_t copy_len;
    uint8_t dead;

    /* Lock */
    try(_crawdb_lock(craw));

    /* Determine idx size again */
    /* TODO ensure value makes sense */
    idx_size_after = lseek(craw->fd_idx, 0, SEEK_END);
    return_if_err(idx_size_after < 0, CRAWDB_ERR_SWAP_LSEEK);

    /* Copy records that came in while we were indexing */
    if (idx_size_after > craw->idx_size) {
        copy_len = (size_t)(idx_size_after - craw->idx_size);
        offset_src = (loff_t)craw->idx_size;
        offset_dst = (loff_t)size_new;
        iorv = copy_file_range(craw->fd_idx, &offset_src, fd_new, &offset_dst, copy_len, 0);
        return_if_err(iorv != (ssize_t)copy_len, CRAWDB_ERR_SWAP_COPY);
    }

    /* Rename idx to new */
    rc = rename(path_new, craw->idx_path);
    return_if_err(rc != CRAWDB_OK, CRAWDB_ERR_SWAP_RENAME);

    /* Write dead byte on old idx */
    dead = 1;
    iorv = pwrite(craw->fd_idx, &dead, 1, CRAWDB_OFFSET_DEAD);
    return_if_err(iorv != 1, CRAWDB_ERR_SWAP_WRITE_DEAD);

    /* Reload for O_APPEND */
    try(crawdb_reload(craw));

    /* Unlock */
    try(_crawdb_unlock(craw));

    return CRAWDB_OK;
}

static int _crawdb_reload_for_index(crawdb_t *craw) {
    crawdb_t *craw_ignore;
    return _crawdb_open(0, 1, craw, craw->idx_path, craw->dat_path, 0, &craw_ignore);
}

static int _crawdb_open(int is_new, int for_index, crawdb_t *reload, char *idx_path, char *dat_path, uint32_t nkey, crawdb_t **out_craw) {
    int rv;
    int rc;
    crawdb_t *craw;
    int fd_dat;
    int fd_idx;
    int flags;
    off_t idx_size;
    ssize_t iorv;
    uchar header[CRAWDB_HEADER_SIZE];
    char *craw_str;

    craw_str = "CRAW";
    craw = NULL;
    fd_idx = -1;
    fd_dat = -1;

    /* Set open flags */
    flags = O_RDWR | O_CREAT;
    if (is_new) {
        flags |= O_TRUNC;
    }
    if (!for_index) {
        flags |= O_APPEND;
    }

    /* Open idx */
    fd_idx = open(idx_path, flags, 00644);
    goto_if_err(fd_idx < 0, CRAWDB_ERR_OPEN_IDX, _crawdb_open_err);

    /* Open dat */
    fd_dat = open(dat_path, flags, 00644);
    goto_if_err(fd_dat < 0, CRAWDB_ERR_OPEN_DAT, _crawdb_open_err);

    if (is_new) {
        /* Error if nkey lt 1 */
        goto_if_err(nkey < 1, CRAWDB_ERR_OPEN_NKEY_ZERO, _crawdb_open_err);

        /* Write idx header */
        memcpy(header, craw_str, 4);
        header[4] = CRAWDB_HEADER_VERS; /* version */
        memcpy(header + 5, &nkey, 4);   /* keylen */
        memset(header + 9, 0, 8);       /* nsorted */
        header[CRAWDB_OFFSET_DEAD] = 0; /* deadflag */
        iorv = write(fd_idx, header, CRAWDB_HEADER_SIZE);
        goto_if_err(iorv != CRAWDB_HEADER_SIZE, CRAWDB_ERR_OPEN_WRITE_HEADER, _crawdb_open_err);

    } else {
        /* Read idx header */
        iorv = read(fd_idx, header, CRAWDB_HEADER_SIZE);
        goto_if_err(iorv != CRAWDB_HEADER_SIZE, CRAWDB_ERR_OPEN_READ_HEADER, _crawdb_open_err);

        /* Check header */
        rc = strncmp((const char*)header, craw_str, 4);
        goto_if_err(rc != 0, CRAWDB_ERR_OPEN_BAD_HEADER, _crawdb_open_err);

        /* Check header version */
        goto_if_err(header[4] != CRAWDB_HEADER_VERS, CRAWDB_ERR_OPEN_BAD_VERS, _crawdb_open_err);
    }

    /* Reuse or allocate new struct */
    if (reload) {
        craw = reload;
        if (craw->fd_idx >= 0) close(craw->fd_idx);
        if (craw->fd_dat >= 0) close(craw->fd_dat);
    } else {
        craw = calloc(1, sizeof(crawdb_t));
        craw->idx_path  = strdup(idx_path);
        craw->dat_path  = strdup(dat_path);
    }

    /* Set fields */
    craw->fd_idx = fd_idx;
    craw->fd_dat = fd_dat;
    craw->vers = (uint8_t)header[4];
    memcpy(&craw->nkey, header + 5, 4); /* TODO endianness */
    memcpy(&craw->nsorted, header + 9, 8);
    craw->dead = (uint8_t)header[CRAWDB_OFFSET_DEAD];
    craw->nrec = craw->nkey + 8 + 4 + 2; /* key (nkey) + offset (8) + len (4) + cksum (2) */

    /* Get index size */
    idx_size = lseek(fd_idx, 0, SEEK_END);
    goto_if_err(idx_size < 0, CRAWDB_ERR_OPEN_LSEEK, _crawdb_open_err);

    /* Set index size */
    rc = _crawdb_set_idx_size(craw, idx_size);
    goto_if_err(rc != CRAWDB_OK, rc, _crawdb_open_err);

    *out_craw = craw;
    return CRAWDB_OK;

_crawdb_open_err:
    if (!reload && craw) {
        if (craw->idx_path) free(craw->idx_path);
        if (craw->dat_path) free(craw->dat_path);
        free(craw);
    }
    if (fd_idx >= 0) close(fd_idx);
    if (fd_dat >= 0) close(fd_dat);
    return rv;
}

static int _crawdb_set_idx_size(crawdb_t *craw, uint64_t idx_size) {
    if ((idx_size - CRAWDB_HEADER_SIZE) % craw->nrec != 0) {
        return CRAWDB_ERR_BAD_IDX_SIZE;
    }
    craw->idx_size = idx_size;
    craw->ntotal = (idx_size - CRAWDB_HEADER_SIZE) / craw->nrec;
    if (craw->nsorted > craw->ntotal) {
        return CRAWDB_ERR_BAD_NSORTED;
    }
    craw->nunsorted = craw->ntotal - craw->nsorted;
    return CRAWDB_OK;
}

static int _crawdb_lock(crawdb_t *craw) {
    if (flock(craw->fd_idx, LOCK_EX) != 0) {
        return CRAWDB_ERR_LOCK_EX;
    }
    craw->locked = 1;
    return CRAWDB_OK;
}

static int _crawdb_unlock(crawdb_t *craw) {
    if (flock(craw->fd_idx, LOCK_UN) != 0) {
        return CRAWDB_ERR_LOCK_UN;
    }
    craw->locked = 0;
    return CRAWDB_OK;
}

static int _crawdb_unlock_if_locked(crawdb_t *craw) {
    if (craw->locked) {
        return _crawdb_unlock(craw);
    }
    return CRAWDB_OK;
}

#ifdef CRAWDB_MAIN

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
            /* INIT */
            nkey = (nkey < 1 ? 32 : nkey);
            rv = crawdb_new(idx, dat, nkey, &craw);
            break;

        case 'S':
            /* SET */
            if (!key || !val) {
                fprintf(stderr, "Expected `--key` and `--val` with `--action-set`\n");
                usage(stderr, 1);
            }
            rv = crawdb_set(craw, (uchar*)key, strlen(key), (uchar*)val, strlen(val));
            break;

        case 'G':
            /* GET */
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
            /* INDEX */
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

#endif /* CRAWDB_MAIN */
