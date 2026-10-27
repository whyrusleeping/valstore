#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <spdk/env.h>
#include <spdk/nvme.h>

#include <errno.h>
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define VALS_CHUNK (128 * 1024)
#define VALS_DMA_POOL (8 * 1024 * 1024)
#define VALS_AIO_SLOT (16 * 1024)
#define VALS_AIO_SLOTS 1024
#define VALS_AIO_MAGIC 0x56414c5341494f52ULL
#define VALS_AIO_W_MAGIC 0x56414c5341494f57ULL

struct vals_spdk_ctrlr {
    struct spdk_nvme_ctrlr *ctrlr;
    struct spdk_nvme_ns *ns;
    uint32_t sector_size;
    uint64_t num_sectors;
};

struct vals_spdk_dev {
    struct spdk_nvme_ns *ns;
    struct spdk_nvme_qpair *qpair;
    void *bounce;
    void *dma_pool;
    size_t dma_pool_size;
    void **aio_free;
    int aio_top;
    uint64_t aio_inflight;
    uint32_t sector_size;
    uint64_t num_sectors;
    struct vals_spdk_ctrlr *owned_ctrlr;
};

static void *vals_dma_get(struct vals_spdk_dev *d, uint64_t nbytes, bool *pooled) {
    if (nbytes <= VALS_AIO_SLOT && d->aio_top > 0) {
        *pooled = true;
        return d->aio_free[--d->aio_top];
    }
    *pooled = false;
    return spdk_dma_zmalloc(nbytes, 0x1000, NULL);
}

static void vals_dma_put(struct vals_spdk_dev *d, void *ptr, bool pooled) {
    if (pooled) {
        if (d->aio_top >= VALS_AIO_SLOTS) {
            fprintf(stderr, "VALS SPDK FATAL: aio DMA pool overflow\n");
            abort();
        }
        d->aio_free[d->aio_top++] = ptr;
    } else {
        spdk_free(ptr);
    }
}

static inline size_t vals_align_up(size_t x, size_t a) {
    return (x + a - 1) & ~(a - 1);
}

static bool g_env_inited = false;

int vals_spdk_env_init(const char *name, int shm_id, int main_core,
                       int mem_size_mb) {
    if (g_env_inited) {
        return 0;
    }
    struct spdk_env_opts opts;
    opts.opts_size = sizeof(opts);
    spdk_env_opts_init(&opts);
    opts.name = (name != NULL && name[0] != '\0') ? name : "vals";
    if (shm_id >= 0) {
        opts.shm_id = shm_id;
    }
    opts.unlink_hugepage = true;
    if (mem_size_mb > 0) {
        opts.mem_size = mem_size_mb;
    }
    char core_buf[32];
    if (main_core >= 0) {
        snprintf(core_buf, sizeof(core_buf), "[%d]", main_core);
        opts.core_mask = core_buf;
    }
    if (spdk_env_init(&opts) < 0) {
        return -1;
    }
    if (main_core >= 0) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(main_core, &set);
        sched_setaffinity(0, sizeof(set), &set);
    } else {
        long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
        if (ncpu > 0) {
            cpu_set_t set;
            CPU_ZERO(&set);
            for (long i = 0; i < ncpu && i < CPU_SETSIZE; i++) {
                CPU_SET(i, &set);
            }
            sched_setaffinity(0, sizeof(set), &set);
        }
    }
    g_env_inited = true;
    return 0;
}

struct vals_spdk_ctrlr *vals_spdk_attach(const char *pci_addr, uint32_t nsid,
                                         uint32_t *out_sector_size,
                                         uint64_t *out_num_sectors) {
    struct spdk_nvme_transport_id trid;
    memset(&trid, 0, sizeof(trid));
    spdk_nvme_trid_populate_transport(&trid, SPDK_NVME_TRANSPORT_PCIE);
    snprintf(trid.traddr, sizeof(trid.traddr), "%s", pci_addr);

    struct spdk_nvme_ctrlr *ctrlr = spdk_nvme_connect(&trid, NULL, 0);
    if (ctrlr == NULL) {
        return NULL;
    }
    struct spdk_nvme_ns *ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
    if (ns == NULL || !spdk_nvme_ns_is_active(ns)) {
        spdk_nvme_detach(ctrlr);
        return NULL;
    }
    struct vals_spdk_ctrlr *c = calloc(1, sizeof(*c));
    if (c == NULL) {
        spdk_nvme_detach(ctrlr);
        return NULL;
    }
    c->ctrlr = ctrlr;
    c->ns = ns;
    c->sector_size = spdk_nvme_ns_get_sector_size(ns);
    c->num_sectors = spdk_nvme_ns_get_num_sectors(ns);
    if (out_sector_size != NULL) {
        *out_sector_size = c->sector_size;
    }
    if (out_num_sectors != NULL) {
        *out_num_sectors = c->num_sectors;
    }
    return c;
}

int vals_spdk_ctrlr_serial(const struct vals_spdk_ctrlr *c, char *out,
                           size_t out_len) {
    if (c == NULL || out == NULL || out_len == 0) {
        return -1;
    }
    const struct spdk_nvme_ctrlr_data *data = spdk_nvme_ctrlr_get_data(c->ctrlr);
    if (data == NULL) {
        return -1;
    }
    size_t len = sizeof(data->sn);
    while (len > 0 && (data->sn[len - 1] == ' ' || data->sn[len - 1] == '\0')) {
        len--;
    }
    if (len + 1 > out_len) {
        return -1;
    }
    memcpy(out, data->sn, len);
    out[len] = '\0';
    return 0;
}

void vals_spdk_detach(struct vals_spdk_ctrlr *c) {
    if (c == NULL) {
        return;
    }
    spdk_nvme_detach(c->ctrlr);
    free(c);
}

struct vals_spdk_dev *vals_spdk_open_qpair(struct vals_spdk_ctrlr *c) {
    if (c == NULL) {
        return NULL;
    }
    struct spdk_nvme_qpair *qpair =
        spdk_nvme_ctrlr_alloc_io_qpair(c->ctrlr, NULL, 0);
    if (qpair == NULL) {
        return NULL;
    }
    void *bounce = spdk_dma_zmalloc(VALS_CHUNK, 0x1000, NULL);
    void *dma_pool = spdk_dma_zmalloc(VALS_DMA_POOL, 0x1000, NULL);
    struct vals_spdk_dev *d = calloc(1, sizeof(*d));
    void **aio_free = (void **)calloc(VALS_AIO_SLOTS, sizeof(void *));
    if (bounce == NULL || dma_pool == NULL || d == NULL || aio_free == NULL) {
        spdk_free(bounce);
        spdk_free(dma_pool);
        free(d);
        free(aio_free);
        spdk_nvme_ctrlr_free_io_qpair(qpair);
        return NULL;
    }
    d->ns = c->ns;
    d->qpair = qpair;
    d->bounce = bounce;
    d->dma_pool = dma_pool;
    d->dma_pool_size = VALS_DMA_POOL;
    d->aio_free = aio_free;
    for (int i = 0; i < VALS_AIO_SLOTS; i++) {
        void *slot = spdk_dma_zmalloc(VALS_AIO_SLOT, 0x1000, NULL);
        if (slot == NULL) {
            break;
        }
        d->aio_free[d->aio_top++] = slot;
    }
    d->sector_size = c->sector_size;
    d->num_sectors = c->num_sectors;
    d->owned_ctrlr = NULL;
    return d;
}

struct vals_spdk_dev *vals_spdk_open(const char *pci_addr, uint32_t nsid,
                                     uint32_t *out_sector_size,
                                     uint64_t *out_num_sectors) {
    struct vals_spdk_ctrlr *c =
        vals_spdk_attach(pci_addr, nsid, out_sector_size, out_num_sectors);
    if (c == NULL) {
        return NULL;
    }
    struct vals_spdk_dev *d = vals_spdk_open_qpair(c);
    if (d == NULL) {
        vals_spdk_detach(c);
        return NULL;
    }
    d->owned_ctrlr = c;
    return d;
}

struct vals_cpl {
    bool done;
    bool err;
};

static void vals_io_cb(void *arg, const struct spdk_nvme_cpl *cpl) {
    struct vals_cpl *s = (struct vals_cpl *)arg;
    s->err = spdk_nvme_cpl_is_error(cpl);
    s->done = true;
}

static int vals_poll(struct vals_spdk_dev *d, struct vals_cpl *s) {
    while (!s->done) {
        spdk_nvme_qpair_process_completions(d->qpair, 0);
    }
    return s->err ? -EIO : 0;
}

int vals_spdk_read(struct vals_spdk_dev *d, uint64_t lba, void *out,
                   uint64_t nbytes) {
    uint8_t *dst = (uint8_t *)out;
    while (nbytes > 0) {
        uint64_t chunk = nbytes < VALS_CHUNK ? nbytes : VALS_CHUNK;
        uint32_t lba_count = (uint32_t)(chunk / d->sector_size);
        struct vals_cpl s = {false, false};
        int rc;
        while ((rc = spdk_nvme_ns_cmd_read(d->ns, d->qpair, d->bounce, lba, lba_count,
                                           vals_io_cb, &s, 0)) == -ENOMEM) {
            spdk_nvme_qpair_process_completions(d->qpair, 0);
            s = (struct vals_cpl){false, false};
        }
        if (rc != 0) {
            return rc;
        }
        rc = vals_poll(d, &s);
        if (rc != 0) {
            return rc;
        }
        memcpy(dst, d->bounce, chunk);
        dst += chunk;
        lba += lba_count;
        nbytes -= chunk;
    }
    return 0;
}

int vals_spdk_write(struct vals_spdk_dev *d, uint64_t lba, const void *in,
                    uint64_t nbytes) {
    const uint8_t *src = (const uint8_t *)in;
    while (nbytes > 0) {
        uint64_t chunk = nbytes < VALS_CHUNK ? nbytes : VALS_CHUNK;
        uint32_t lba_count = (uint32_t)(chunk / d->sector_size);
        memcpy(d->bounce, src, chunk);
        struct vals_cpl s = {false, false};
        int rc;
        while ((rc = spdk_nvme_ns_cmd_write(d->ns, d->qpair, d->bounce, lba, lba_count,
                                            vals_io_cb, &s, 0)) == -ENOMEM) {
            spdk_nvme_qpair_process_completions(d->qpair, 0);
            s = (struct vals_cpl){false, false};
        }
        if (rc != 0) {
            return rc;
        }
        rc = vals_poll(d, &s);
        if (rc != 0) {
            return rc;
        }
        src += chunk;
        lba += lba_count;
        nbytes -= chunk;
    }
    return 0;
}

struct vals_batch_cpl {
    int remaining;
    bool err;
};

static void vals_batch_cb(void *arg, const struct spdk_nvme_cpl *cpl) {
    struct vals_batch_cpl *s = (struct vals_batch_cpl *)arg;
    if (spdk_nvme_cpl_is_error(cpl)) {
        s->err = true;
    }
    s->remaining--;
}

static int vals_read_one(struct vals_spdk_dev *d, uint64_t lba, void *out,
                         uint64_t nbytes) {
    void *buf = spdk_dma_zmalloc(nbytes, 0x1000, NULL);
    if (buf == NULL) {
        return -ENOMEM;
    }
    struct vals_batch_cpl s = {.remaining = 1, .err = false};
    uint32_t lba_count = (uint32_t)(nbytes / d->sector_size);
    int rc;
    while ((rc = spdk_nvme_ns_cmd_read(d->ns, d->qpair, buf, lba, lba_count,
                                       vals_batch_cb, &s, 0)) == -ENOMEM) {
        spdk_nvme_qpair_process_completions(d->qpair, 0);
    }
    if (rc != 0) {
        s.remaining = 0;
        spdk_free(buf);
        return rc;
    }
    while (s.remaining > 0) {
        spdk_nvme_qpair_process_completions(d->qpair, 0);
    }
    if (!s.err) {
        memcpy(out, buf, nbytes);
    }
    int ret = s.err ? -EIO : 0;
    spdk_free(buf);
    return ret;
}

int vals_spdk_read_batch(struct vals_spdk_dev *d, const uint64_t *lbas,
                         const uint64_t *nbytes, void *const *out_ptrs,
                         uint32_t n) {
    const size_t ALIGN = 4096;
    uint32_t i = 0;
    while (i < n) {
        if (nbytes[i] > d->dma_pool_size) {
            int rc = vals_read_one(d, lbas[i], out_ptrs[i], nbytes[i]);
            if (rc != 0) {
                return rc;
            }
            i++;
            continue;
        }

        size_t used = 0;
        uint32_t j = i;
        while (j < n && nbytes[j] <= d->dma_pool_size) {
            size_t off = vals_align_up(used, ALIGN);
            if (off + nbytes[j] > d->dma_pool_size) {
                break;
            }
            used = off + nbytes[j];
            j++;
        }

        struct vals_batch_cpl s = {.remaining = (int)(j - i), .err = false};
        size_t off = 0;
        uint32_t submitted = i;
        int err_rc = 0;
        for (uint32_t k = i; k < j; k++) {
            off = vals_align_up(off, ALIGN);
            void *buf = (char *)d->dma_pool + off;
            uint32_t lba_count = (uint32_t)(nbytes[k] / d->sector_size);
            int rc;
            while ((rc = spdk_nvme_ns_cmd_read(d->ns, d->qpair, buf, lbas[k],
                                               lba_count, vals_batch_cb, &s,
                                               0)) == -ENOMEM) {
                spdk_nvme_qpair_process_completions(d->qpair, 0);
            }
            if (rc != 0) {
                err_rc = rc;
                break;
            }
            submitted = k + 1;
            off += nbytes[k];
        }
        s.remaining -= (int)(j - submitted);
        while (s.remaining > 0) {
            spdk_nvme_qpair_process_completions(d->qpair, 0);
        }
        if (err_rc != 0) {
            return err_rc;
        }
        if (s.err) {
            return -EIO;
        }

        off = 0;
        for (uint32_t k = i; k < j; k++) {
            off = vals_align_up(off, ALIGN);
            memcpy(out_ptrs[k], (char *)d->dma_pool + off, nbytes[k]);
            off += nbytes[k];
        }
        i = j;
    }
    return 0;
}

struct vals_aio {
    uint64_t magic;
    struct vals_spdk_dev *d;
    void *dma;
    void *out;
    uint64_t nbytes;
    bool pooled;
    void (*complete)(void *, bool);
    void *ctx;
};

static void vals_aio_cb(void *arg, const struct spdk_nvme_cpl *cpl) {
    struct vals_aio *op = (struct vals_aio *)arg;
    if (op->magic != VALS_AIO_MAGIC) {
        fprintf(stderr, "VALS SPDK FATAL: aio read completion magic mismatch op=%p\n",
                (void *)op);
        abort();
    }
    if (op->d->aio_inflight == 0) {
        fprintf(stderr, "VALS SPDK FATAL: aio read in-flight underflow op=%p\n",
                (void *)op);
        abort();
    }
    op->d->aio_inflight--;
    bool success = !spdk_nvme_cpl_is_error(cpl);
    if (op->out != NULL) {
        if (success) {
            memcpy(op->out, op->dma, op->nbytes);
        }
        vals_dma_put(op->d, op->dma, op->pooled);
    }
    void (*complete)(void *, bool) = op->complete;
    void *ctx = op->ctx;
    op->magic = 0;
    free(op);
    complete(ctx, success);
}

int vals_spdk_aio_read(struct vals_spdk_dev *d, uint64_t lba, void *out,
                       uint64_t nbytes, void (*complete)(void *, bool),
                       void *ctx) {
    struct vals_aio *op = (struct vals_aio *)calloc(1, sizeof(*op));
    if (op == NULL) {
        return -ENOMEM;
    }
    op->magic = VALS_AIO_MAGIC;
    if (spdk_vtophys(out, NULL) != SPDK_VTOPHYS_ERROR) {
        op->dma = out;
        op->out = NULL;
        op->pooled = false;
    } else {
        op->dma = vals_dma_get(d, nbytes, &op->pooled);
        if (op->dma == NULL) {
            op->magic = 0;
            free(op);
            return -ENOMEM;
        }
        op->out = out;
    }
    op->d = d;
    op->nbytes = nbytes;
    op->complete = complete;
    op->ctx = ctx;

    uint32_t lba_count = (uint32_t)(nbytes / d->sector_size);
    int rc;
    while ((rc = spdk_nvme_ns_cmd_read(d->ns, d->qpair, op->dma, lba, lba_count,
                                       vals_aio_cb, op, 0)) == -ENOMEM) {
        spdk_nvme_qpair_process_completions(d->qpair, 0);
    }
    if (rc != 0) {
        if (op->out != NULL) {
            vals_dma_put(d, op->dma, op->pooled);
        }
        op->magic = 0;
        free(op);
        return rc;
    }
    d->aio_inflight++;
    return 0;
}

void *vals_spdk_dma_alloc(uint64_t nbytes) {
    return spdk_dma_zmalloc(nbytes, 0x1000, NULL);
}

void vals_spdk_dma_free(void *p) {
    spdk_free(p);
}

struct vals_aio_w {
    uint64_t magic;
    struct vals_spdk_dev *d;
    void *dma;
    bool pooled;
    void (*complete)(void *, bool);
    void *ctx;
};

static void vals_aio_w_cb(void *arg, const struct spdk_nvme_cpl *cpl) {
    struct vals_aio_w *op = (struct vals_aio_w *)arg;
    if (op->magic != VALS_AIO_W_MAGIC) {
        fprintf(stderr, "VALS SPDK FATAL: aio write completion magic mismatch op=%p\n",
                (void *)op);
        abort();
    }
    if (op->d->aio_inflight == 0) {
        fprintf(stderr, "VALS SPDK FATAL: aio write in-flight underflow op=%p\n",
                (void *)op);
        abort();
    }
    op->d->aio_inflight--;
    bool success = !spdk_nvme_cpl_is_error(cpl);
    vals_dma_put(op->d, op->dma, op->pooled);
    void (*complete)(void *, bool) = op->complete;
    void *ctx = op->ctx;
    op->magic = 0;
    free(op);
    complete(ctx, success);
}

int vals_spdk_aio_write(struct vals_spdk_dev *d, uint64_t lba, const void *in,
                        uint64_t nbytes, void (*complete)(void *, bool),
                        void *ctx) {
    struct vals_aio_w *op = (struct vals_aio_w *)calloc(1, sizeof(*op));
    if (op == NULL) {
        return -ENOMEM;
    }
    op->magic = VALS_AIO_W_MAGIC;
    op->dma = vals_dma_get(d, nbytes, &op->pooled);
    if (op->dma == NULL) {
        op->magic = 0;
        free(op);
        return -ENOMEM;
    }
    memcpy(op->dma, in, nbytes);
    op->d = d;
    op->complete = complete;
    op->ctx = ctx;

    uint32_t lba_count = (uint32_t)(nbytes / d->sector_size);
    int rc;
    while ((rc = spdk_nvme_ns_cmd_write(d->ns, d->qpair, op->dma, lba, lba_count,
                                        vals_aio_w_cb, op, 0)) == -ENOMEM) {
        spdk_nvme_qpair_process_completions(d->qpair, 0);
    }
    if (rc != 0) {
        vals_dma_put(d, op->dma, op->pooled);
        op->magic = 0;
        free(op);
        return rc;
    }
    d->aio_inflight++;
    return 0;
}

int vals_spdk_poll(struct vals_spdk_dev *d) {
    return spdk_nvme_qpair_process_completions(d->qpair, 0);
}

struct vals_aio_f {
    uint64_t magic;
    struct vals_spdk_dev *d;
    void (*complete)(void *, bool);
    void *ctx;
};

#define VALS_AIO_F_MAGIC 0x56414c5341494f46ULL

static void vals_aio_f_cb(void *arg, const struct spdk_nvme_cpl *cpl) {
    struct vals_aio_f *op = (struct vals_aio_f *)arg;
    if (op->magic != VALS_AIO_F_MAGIC) {
        fprintf(stderr, "VALS SPDK FATAL: aio flush completion magic mismatch op=%p\n",
                (void *)op);
        abort();
    }
    if (op->d->aio_inflight == 0) {
        fprintf(stderr, "VALS SPDK FATAL: aio flush in-flight underflow op=%p\n",
                (void *)op);
        abort();
    }
    op->d->aio_inflight--;
    bool success = !spdk_nvme_cpl_is_error(cpl);
    void (*complete)(void *, bool) = op->complete;
    void *ctx = op->ctx;
    op->magic = 0;
    free(op);
    complete(ctx, success);
}

int vals_spdk_aio_flush(struct vals_spdk_dev *d, void (*complete)(void *, bool),
                        void *ctx) {
    struct vals_aio_f *op = (struct vals_aio_f *)calloc(1, sizeof(*op));
    if (op == NULL) {
        return -ENOMEM;
    }
    op->magic = VALS_AIO_F_MAGIC;
    op->d = d;
    op->complete = complete;
    op->ctx = ctx;
    int rc;
    while ((rc = spdk_nvme_ns_cmd_flush(d->ns, d->qpair, vals_aio_f_cb, op)) == -ENOMEM) {
        spdk_nvme_qpair_process_completions(d->qpair, 0);
    }
    if (rc != 0) {
        op->magic = 0;
        free(op);
        return rc;
    }
    d->aio_inflight++;
    return 0;
}

int vals_spdk_flush(struct vals_spdk_dev *d) {
    struct vals_cpl s = {false, false};
    int rc;
    while ((rc = spdk_nvme_ns_cmd_flush(d->ns, d->qpair, vals_io_cb, &s)) == -ENOMEM) {
        spdk_nvme_qpair_process_completions(d->qpair, 0);
        s = (struct vals_cpl){false, false};
    }
    if (rc != 0) {
        return rc;
    }
    return vals_poll(d, &s);
}

int vals_spdk_deallocate(struct vals_spdk_dev *d, const uint64_t *lbas,
                         const uint32_t *counts, uint32_t n) {
    if (!(spdk_nvme_ns_get_flags(d->ns) & SPDK_NVME_NS_DEALLOCATE_SUPPORTED)) {
        return -ENOTSUP;
    }
    if (n == 0 || n > 256) {
        return -EINVAL;
    }
    struct spdk_nvme_dsm_range ranges[256] = {0};
    for (uint32_t i = 0; i < n; i++) {
        ranges[i].starting_lba = lbas[i];
        ranges[i].length = counts[i];
    }
    struct vals_cpl s = {false, false};
    int rc;
    while ((rc = spdk_nvme_ns_cmd_dataset_management(d->ns, d->qpair,
        SPDK_NVME_DSM_ATTR_DEALLOCATE, ranges, n, vals_io_cb, &s)) == -ENOMEM) {
        if (d->aio_inflight == 0) {
            return -ENOMEM;
        }
        int completed = spdk_nvme_qpair_process_completions(d->qpair, 0);
        if (completed < 0) {
            return completed;
        }
        s = (struct vals_cpl){false, false};
    }
    if (rc != 0) {
        return rc;
    }
    return vals_poll(d, &s);
}

void vals_spdk_close(struct vals_spdk_dev *d) {
    if (d == NULL) {
        return;
    }
    if (d->aio_inflight != 0) {
        fprintf(stderr, "VALS SPDK FATAL: closing qpair with aio in flight\n");
        abort();
    }
    if (d->bounce != NULL) {
        spdk_free(d->bounce);
    }
    if (d->dma_pool != NULL) {
        spdk_free(d->dma_pool);
    }
    if (d->aio_free != NULL) {
        while (d->aio_top > 0) {
            spdk_free(d->aio_free[--d->aio_top]);
        }
        free(d->aio_free);
    }
    if (d->qpair != NULL) {
        spdk_nvme_ctrlr_free_io_qpair(d->qpair);
    }
    if (d->owned_ctrlr != NULL) {
        vals_spdk_detach(d->owned_ctrlr);
    }
    free(d);
}
