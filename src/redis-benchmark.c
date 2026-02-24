/* Redis benchmark utility.
 *
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "fmacros.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>
#include <math.h>
#include <pthread.h>

#include <sdscompat.h> /* Use hiredis' sds compat header that maps sds calls to their hi_ variants */
#include <sds.h> /* Use hiredis sds. */
#include "ae.h"
#include <hiredis.h>
#ifdef USE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <hiredis_ssl.h>
#endif
#include "adlist.h"
#include "dict.h"
#include "zmalloc.h"
#include "atomicvar.h"
#include "crc16_slottable.h"
#include "hdr_histogram.h"
#include "cli_common.h"
#include "mt19937-64.h"

#include <hiredis_mux.h>
#include <arpa/inet.h>

#define UNUSED(V) ((void) V)
#define RANDPTR_INITIAL_SIZE 8
#define DEFAULT_LATENCY_PRECISION 3
#define MAX_LATENCY_PRECISION 4
#define MAX_THREADS 500
#define CLUSTER_SLOTS 16384
#define CONFIG_LATENCY_HISTOGRAM_MIN_VALUE 10L          /* >= 10 usecs */
#define CONFIG_LATENCY_HISTOGRAM_MAX_VALUE 3000000L          /* <= 3 secs(us precision) */
#define CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE 3000000L   /* <= 3 secs(us precision) */
#define SHOW_THROUGHPUT_INTERVAL 250  /* 250ms */

#define MUX_DEFAULT_STREAMS 10  /* Default number of mux streams per connection */

/* MUX frame constants for benchmark-level frame encode/decode */
#define BENCH_MUX_HEADER_SIZE 10
#define BENCH_MUX_MAGIC       0xAA
#define BENCH_MUX_FRAME_DATA  0x01
#define BENCH_MUX_TYPE_MASK   0x0F

/* Forward declarations for MUX shared connection */
typedef struct muxSharedConn muxSharedConn;

#define CLIENT_GET_EVENTLOOP(c) \
    (c->thread_id >= 0 ? config.threads[c->thread_id]->el : config.el)

struct benchmarkThread;
struct clusterNode;
struct redisConfig;

static struct config {
    aeEventLoop *el;
    cliConnInfo conn_info;
    const char *hostsocket;
    int tls;
    struct cliSSLconfig sslconfig;
    int numclients;
    redisAtomic int liveclients;
    int requests;
    redisAtomic int requests_issued;
    redisAtomic int requests_finished;
    redisAtomic int previous_requests_finished;
    int last_printed_bytes;
    long long previous_tick;
    int keysize;
    int datasize;
    int randomkeys;
    int randomkeys_keyspacelen;
    int keepalive;
    int pipeline;
    long long start;
    long long totlatency;
    const char *title;
    list *clients;
    int quiet;
    int csv;
    int loop;
    int idlemode;
    sds input_dbnumstr;
    char *tests;
    int stdinarg; /* get last arg from stdin. (-x option) */
    int precision;
    int num_threads;
    struct benchmarkThread **threads;
    int cluster_mode;
    int cluster_node_count;
    struct clusterNode **cluster_nodes;
    struct redisConfig *redis_config;
    struct hdr_histogram* latency_histogram;
    struct hdr_histogram* current_sec_latency_histogram;
    redisAtomic int is_fetching_slots;
    redisAtomic int is_updating_slots;
    redisAtomic int slots_last_update;
    int enable_tracking;
    pthread_mutex_t liveclients_mutex;
    pthread_mutex_t is_updating_slots_mutex;
    int resp3; /* use RESP3 */
    int mux_mode;         /* Enable MUX (stream-multiplexed) mode */
    int mux_streams;      /* Number of MUX streams per connection */
} config;

typedef struct _client {
    redisContext *context;
    sds obuf;
    char **randptr;         /* Pointers to :rand: strings inside the command buf */
    size_t randlen;         /* Number of pointers in client->randptr */
    size_t randfree;        /* Number of unused pointers in client->randptr */
    char **stagptr;         /* Pointers to slot hashtags (cluster mode only) */
    size_t staglen;         /* Number of pointers in client->stagptr */
    size_t stagfree;        /* Number of unused pointers in client->stagptr */
    size_t written;         /* Bytes of 'obuf' already written */
    long long start;        /* Start time of a request */
    long long latency;      /* Request latency */
    int pending;            /* Number of pending requests (replies to consume) */
    int prefix_pending;     /* If non-zero, number of pending prefix commands. Commands
                               such as auth and select are prefixed to the pipeline of
                               benchmark commands and discarded after the first send. */
    int prefixlen;          /* Size in bytes of the pending prefix commands */
    int thread_id;
    struct clusterNode *cluster_node;
    int slots_last_update;
    /* MUX mode state */
    int mux_hello_done;     /* Whether MUX HELLO handshake reply has been consumed */
    uint32_t mux_stream_id; /* Stream ID for this client in MUX shared connection */
    muxSharedConn *mux_conn; /* Back-pointer to shared MUX connection (NULL if not MUX) */
    redisReader *mux_reader; /* Per-client RESP reader for MUX mode (parses demuxed data) */
} *client;

/* Threads. */

/* Per-thread shared MUX connection: one TCP conn, N logical streams */
struct muxSharedConn {
    redisContext *ctx;          /* The single TCP connection */
    int fd;                     /* fd for event registration */
    int hello_done;             /* Whether HELLO MULTIPLEX handshake completed */
    int hello_pending;          /* Whether HELLO command is pending reply */

    /* Client registry: stream_id -> client mapping */
    client *clients[1024];      /* Direct-mapped: clients[stream_id/2] (stream_id=1,3,5...) */
    int num_clients;            /* Number of registered clients */
    uint32_t next_stream_id;    /* Next stream ID to assign (1,3,5...) */

    /* Write queue: clients that need their obuf flushed via MUX frames */
    client **write_queue;       /* Array of clients needing write */
    int write_queue_len;
    int write_queue_cap;

    /* MUX frame receive buffer (raw bytes from socket) */
    char *recvbuf;
    size_t recvbuf_len;
    size_t recvbuf_alloc;

    /* Frame parse state machine */
    int parsing_payload;        /* 0=reading header, 1=reading payload */
    unsigned char frame_header[BENCH_MUX_HEADER_SIZE];
    int header_bytes_read;
    uint8_t current_flags;
    uint32_t current_stream_id;
    uint32_t current_payload_len;
    char *payload_buf;
    size_t payload_buf_len;
    size_t payload_buf_alloc;

    /* Send buffer for batched MUX frames */
    char *sendbuf;
    size_t sendbuf_len;
    size_t sendbuf_alloc;
    size_t sendbuf_pos;         /* Bytes already sent */

    int thread_id;              /* Owner thread index */
};

typedef struct benchmarkThread {
    int index;
    pthread_t thread;
    aeEventLoop *el;
    muxSharedConn *mux_conn;    /* Shared MUX connection for this thread (NULL if not MUX mode) */
} benchmarkThread;

/* Cluster. */
typedef struct clusterNode {
    char *ip;
    int port;
    sds name;
    int flags;
    sds replicate;  /* Master ID if node is a slave */
    int *slots;
    int slots_count;
    int *updated_slots;         /* Used by updateClusterSlotsConfiguration */
    int updated_slots_count;    /* Used by updateClusterSlotsConfiguration */
    int replicas_count;
    sds *migrating; /* An array of sds where even strings are slots and odd
                     * strings are the destination node IDs. */
    sds *importing; /* An array of sds where even strings are slots and odd
                     * strings are the source node IDs. */
    int migrating_count; /* Length of the migrating array (migrating slots*2) */
    int importing_count; /* Length of the importing array (importing slots*2) */
    struct redisConfig *redis_config;
} clusterNode;

typedef struct redisConfig {
    sds save;
    sds appendonly;
} redisConfig;

/* Prototypes */
static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void createMissingClients(client c);
static benchmarkThread *createBenchmarkThread(int index);
static void freeBenchmarkThread(benchmarkThread *thread);
static void freeBenchmarkThreads(void);
static void *execBenchmarkThread(void *ptr);
static void randomizeClientKey(client c);
static void setClusterKeyHashTag(client c);
static int processReply(client c, void *reply);

/* MUX shared connection prototypes */
static muxSharedConn *muxSharedConnCreate(int thread_id);
static void muxSharedConnFree(muxSharedConn *mc);
static void muxSharedConnRegisterClient(muxSharedConn *mc, client c);
static void muxSharedConnQueueWrite(muxSharedConn *mc, client c);
static void muxSharedReadHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void muxSharedWriteHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void muxSharedConnScheduleWrite(muxSharedConn *mc);
static clusterNode *createClusterNode(char *ip, int port);
static redisConfig *getRedisConfig(const char *ip, int port,
                                   const char *hostsocket);
static redisContext *getRedisContext(const char *ip, int port,
                                     const char *hostsocket);
static void freeRedisConfig(redisConfig *cfg);
static int fetchClusterSlotsConfiguration(client c);
static void updateClusterSlotsConfiguration(void);
int showThroughput(struct aeEventLoop *eventLoop, long long id,
                   void *clientData);

/* Dict callbacks */
static uint64_t dictSdsHash(const void *key);
static int dictSdsKeyCompare(dict *d, const void *key1, const void *key2);

/* Implementation */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

static long long mstime(void) {
    return ustime()/1000;
}

static uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

static int dictSdsKeyCompare(dict *d, const void *key1, const void *key2)
{
    int l1,l2;
    UNUSED(d);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

static redisContext *getRedisContext(const char *ip, int port,
                                     const char *hostsocket)
{
    redisContext *ctx = NULL;
    redisReply *reply =  NULL;
    if (hostsocket == NULL)
        ctx = redisConnect(ip, port);
    else
        ctx = redisConnectUnix(hostsocket);
    if (ctx == NULL || ctx->err) {
        fprintf(stderr,"Could not connect to Redis at ");
        char *err = (ctx != NULL ? ctx->errstr : "");
        if (hostsocket == NULL)
            fprintf(stderr,"%s:%d: %s\n",ip,port,err);
        else
            fprintf(stderr,"%s: %s\n",hostsocket,err);
        goto cleanup;
    }
    if (config.tls==1) {
        const char *err = NULL;
        if (cliSecureConnection(ctx, config.sslconfig, &err) == REDIS_ERR && err) {
            fprintf(stderr, "Could not negotiate a TLS connection: %s\n", err);
            goto cleanup;
        }
    }
    if (config.conn_info.auth == NULL)
        return ctx;
    if (config.conn_info.user == NULL)
        reply = redisCommand(ctx,"AUTH %s", config.conn_info.auth);
    else
        reply = redisCommand(ctx,"AUTH %s %s", config.conn_info.user, config.conn_info.auth);
    if (reply != NULL) {
        if (reply->type == REDIS_REPLY_ERROR) {
            if (hostsocket == NULL)
                fprintf(stderr, "Node %s:%d replied with error:\n%s\n", ip, port, reply->str);
            else
                fprintf(stderr, "Node %s replied with error:\n%s\n", hostsocket, reply->str);
            freeReplyObject(reply);
            redisFree(ctx);
            exit(1);
        }
        freeReplyObject(reply);
        return ctx;
    }
    fprintf(stderr, "ERROR: failed to fetch reply from ");
    if (hostsocket == NULL)
        fprintf(stderr, "%s:%d\n", ip, port);
    else
        fprintf(stderr, "%s\n", hostsocket);
cleanup:
    freeReplyObject(reply);
    redisFree(ctx);
    return NULL;
}



static redisConfig *getRedisConfig(const char *ip, int port,
                                   const char *hostsocket)
{
    redisConfig *cfg = zcalloc(sizeof(*cfg));
    if (!cfg) return NULL;
    redisContext *c = NULL;
    redisReply *reply = NULL, *sub_reply = NULL;
    c = getRedisContext(ip, port, hostsocket);
    if (c == NULL) {
        freeRedisConfig(cfg);
        exit(1);
    }
    redisAppendCommand(c, "CONFIG GET %s", "save");
    redisAppendCommand(c, "CONFIG GET %s", "appendonly");
    int abort_test = 0;
    int i = 0;
    void *r = NULL;
    for (; i < 2; i++) {
        int res = redisGetReply(c, &r);
        if (reply) freeReplyObject(reply);
        reply = res == REDIS_OK ? ((redisReply *) r) : NULL;
        if (res != REDIS_OK || !r) goto fail;
        if (reply->type == REDIS_REPLY_ERROR) {
            goto fail;
        }
        if (reply->type != REDIS_REPLY_ARRAY || reply->elements < 2) goto fail;
        sub_reply = reply->element[1];
        char *value = sub_reply->str;
        if (!value) value = "";
        switch (i) {
        case 0: cfg->save = sdsnew(value); break;
        case 1: cfg->appendonly = sdsnew(value); break;
        }
    }
    freeReplyObject(reply);
    redisFree(c);
    return cfg;
fail:
    if (reply && reply->type == REDIS_REPLY_ERROR &&
        !strncmp(reply->str,"NOAUTH",6)) {
        if (hostsocket == NULL)
            fprintf(stderr, "Node %s:%d replied with error:\n%s\n", ip, port, reply->str);
        else
            fprintf(stderr, "Node %s replied with error:\n%s\n", hostsocket, reply->str);
        abort_test = 1;
    }
    freeReplyObject(reply);
    redisFree(c);
    freeRedisConfig(cfg);
    if (abort_test) exit(1);
    return NULL;
}
static void freeRedisConfig(redisConfig *cfg) {
    if (cfg->save) sdsfree(cfg->save);
    if (cfg->appendonly) sdsfree(cfg->appendonly);
    zfree(cfg);
}

/* ========================== MUX Shared Connection ========================== */

/* Encode a MUX frame header into buf (must be >= BENCH_MUX_HEADER_SIZE bytes) */
static void benchMuxEncodeHeader(unsigned char *buf, uint8_t flags,
                                  uint32_t stream_id, uint32_t payload_len) {
    buf[0] = BENCH_MUX_MAGIC;
    buf[1] = flags;
    buf[2] = (stream_id >> 24) & 0xFF;
    buf[3] = (stream_id >> 16) & 0xFF;
    buf[4] = (stream_id >> 8) & 0xFF;
    buf[5] = stream_id & 0xFF;
    buf[6] = (payload_len >> 24) & 0xFF;
    buf[7] = (payload_len >> 16) & 0xFF;
    buf[8] = (payload_len >> 8) & 0xFF;
    buf[9] = payload_len & 0xFF;
}

/* Helper: grow a dynamic buffer */
static int benchMuxBufGrow(char **buf, size_t *alloc, size_t len, size_t needed) {
    size_t required = len + needed;
    if (required <= *alloc) return 0;
    size_t newalloc = *alloc ? *alloc : 4096;
    while (newalloc < required) newalloc *= 2;
    char *newbuf = zrealloc(*buf, newalloc);
    if (!newbuf) return -1;
    *buf = newbuf;
    *alloc = newalloc;
    return 0;
}

static muxSharedConn *muxSharedConnCreate(int thread_id) {
    muxSharedConn *mc = zcalloc(sizeof(muxSharedConn));
    mc->thread_id = thread_id;
    mc->next_stream_id = 1; /* Client streams use odd IDs: 1, 3, 5, ... */

    /* Establish the single TCP connection (blocking mode for handshake) */
    const char *ip = config.conn_info.hostip;
    int port = config.conn_info.hostport;
    if (config.hostsocket) {
        mc->ctx = redisConnectUnix(config.hostsocket);
    } else {
        mc->ctx = redisConnect(ip, port);
    }
    if (mc->ctx->err) {
        fprintf(stderr, "MUX: Could not connect to Redis at %s:%d: %s\n",
                ip, port, mc->ctx->errstr);
        exit(1);
    }

    if (config.tls == 1) {
        const char *err = NULL;
        if (cliSecureConnection(mc->ctx, config.sslconfig, &err) == REDIS_ERR && err) {
            fprintf(stderr, "Could not negotiate a TLS connection: %s\n", err);
            exit(1);
        }
    }

    mc->ctx->reader->maxbuf = 0;

    /* Send AUTH if needed */
    if (config.conn_info.auth) {
        redisReply *reply;
        if (config.conn_info.user)
            reply = redisCommand(mc->ctx, "AUTH %s %s", config.conn_info.user, config.conn_info.auth);
        else
            reply = redisCommand(mc->ctx, "AUTH %s", config.conn_info.auth);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            fprintf(stderr, "MUX AUTH error: %s\n", reply ? reply->str : mc->ctx->errstr);
            exit(1);
        }
        freeReplyObject(reply);
    }

    if (config.conn_info.input_dbnum != 0) {
        redisReply *reply = redisCommand(mc->ctx, "SELECT %s", config.input_dbnumstr);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            fprintf(stderr, "MUX SELECT error: %s\n", reply ? reply->str : mc->ctx->errstr);
            exit(1);
        }
        freeReplyObject(reply);
    }

    /* Send HELLO 3 MULTIPLEX to enable MUX mode */
    redisReply *reply = redisCommand(mc->ctx, "HELLO 3 MULTIPLEX");
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        fprintf(stderr, "MUX HELLO error: %s\n", reply ? reply->str : mc->ctx->errstr);
        exit(1);
    }
    freeReplyObject(reply);

    mc->hello_done = 1;
    mc->fd = mc->ctx->fd;

    /* Switch to non-blocking mode for event-driven I/O */
    int flags = fcntl(mc->fd, F_GETFL);
    fcntl(mc->fd, F_SETFL, flags | O_NONBLOCK);
    mc->ctx->flags &= ~REDIS_BLOCK;
    mc->ctx->flags |= REDIS_CONNECTED;

    /* Drain any leftover data from hiredis reader (MUX frames may have arrived
     * piggy-backed with the HELLO reply) */
    redisReader *reader = mc->ctx->reader;
    size_t leftover = reader->len - reader->pos;
    if (leftover > 0) {
        benchMuxBufGrow(&mc->recvbuf, &mc->recvbuf_alloc, 0, leftover);
        memcpy(mc->recvbuf, reader->buf + reader->pos, leftover);
        mc->recvbuf_len = leftover;
        reader->pos = reader->len;
    }

    /* Allocate write queue */
    mc->write_queue_cap = 128;
    mc->write_queue = zmalloc(sizeof(client) * mc->write_queue_cap);

    return mc;
}

static void muxSharedConnFree(muxSharedConn *mc) {
    if (!mc) return;
    if (mc->ctx) redisFree(mc->ctx);
    zfree(mc->recvbuf);
    zfree(mc->payload_buf);
    zfree(mc->sendbuf);
    zfree(mc->write_queue);
    zfree(mc);
}

/* Register a client (with its stream_id) in the shared connection's lookup table */
static void muxSharedConnRegisterClient(muxSharedConn *mc, client c) {
    uint32_t idx = c->mux_stream_id / 2; /* stream_id 1->0, 3->1, 5->2, etc. */
    if (idx < 1024) {
        mc->clients[idx] = c;
    }
    mc->num_clients++;
}

/* Look up a client by stream_id */
static client muxSharedConnLookupClient(muxSharedConn *mc, uint32_t stream_id) {
    uint32_t idx = stream_id / 2;
    if (idx < 1024) return mc->clients[idx];
    return NULL;
}

/* Queue a client for writing (its obuf has data to send as MUX frame) */
static void muxSharedConnQueueWrite(muxSharedConn *mc, client c) {
    if (mc->write_queue_len >= mc->write_queue_cap) {
        mc->write_queue_cap *= 2;
        mc->write_queue = zrealloc(mc->write_queue, sizeof(client) * mc->write_queue_cap);
    }
    mc->write_queue[mc->write_queue_len++] = c;
}

/* Process received MUX frames: parse complete frames from recvbuf,
 * feed RESP payloads to the correct client's mux_reader, and try to
 * extract + process replies. */
static void muxSharedConnProcessFrames(muxSharedConn *mc) {
    while (1) {
        if (!mc->parsing_payload) {
            /* Try to read frame header */
            int needed = BENCH_MUX_HEADER_SIZE - mc->header_bytes_read;
            size_t avail = mc->recvbuf_len;
            if ((int)avail < needed && mc->header_bytes_read == 0 && avail < BENCH_MUX_HEADER_SIZE) break;
            int tocopy = (int)avail < needed ? (int)avail : needed;
            memcpy(mc->frame_header + mc->header_bytes_read, mc->recvbuf, tocopy);
            mc->header_bytes_read += tocopy;
            /* Consume from recvbuf */
            mc->recvbuf_len -= tocopy;
            if (mc->recvbuf_len > 0)
                memmove(mc->recvbuf, mc->recvbuf + tocopy, mc->recvbuf_len);

            if (mc->header_bytes_read < BENCH_MUX_HEADER_SIZE) break;

            /* Decode header */
            if (mc->frame_header[0] != BENCH_MUX_MAGIC) {
                fprintf(stderr, "MUX: invalid frame magic 0x%02x\n", mc->frame_header[0]);
                exit(1);
            }
            mc->current_flags = mc->frame_header[1];
            mc->current_stream_id = ((uint32_t)mc->frame_header[2] << 24) |
                                    ((uint32_t)mc->frame_header[3] << 16) |
                                    ((uint32_t)mc->frame_header[4] << 8) |
                                    (uint32_t)mc->frame_header[5];
            mc->current_payload_len = ((uint32_t)mc->frame_header[6] << 24) |
                                      ((uint32_t)mc->frame_header[7] << 16) |
                                      ((uint32_t)mc->frame_header[8] << 8) |
                                      (uint32_t)mc->frame_header[9];
            mc->header_bytes_read = 0;

            if (mc->current_payload_len == 0) {
                /* Zero-length frame (PONG etc.), skip */
                continue;
            }

            mc->parsing_payload = 1;
            mc->payload_buf_len = 0;
            benchMuxBufGrow(&mc->payload_buf, &mc->payload_buf_alloc, 0, mc->current_payload_len);
        }

        /* Read payload */
        uint32_t remaining = mc->current_payload_len - (uint32_t)mc->payload_buf_len;
        size_t avail = mc->recvbuf_len;
        uint32_t tocopy = (uint32_t)avail < remaining ? (uint32_t)avail : remaining;
        if (tocopy > 0) {
            memcpy(mc->payload_buf + mc->payload_buf_len, mc->recvbuf, tocopy);
            mc->payload_buf_len += tocopy;
            mc->recvbuf_len -= tocopy;
            if (mc->recvbuf_len > 0)
                memmove(mc->recvbuf, mc->recvbuf + tocopy, mc->recvbuf_len);
        }

        if (mc->payload_buf_len < mc->current_payload_len) break;

        /* Full frame received */
        mc->parsing_payload = 0;
        uint8_t frame_type = mc->current_flags & BENCH_MUX_TYPE_MASK;

        if (frame_type == BENCH_MUX_FRAME_DATA) {
            /* Find the client for this stream_id */
            client c = muxSharedConnLookupClient(mc, mc->current_stream_id);
            if (c && c->mux_reader) {
                /* Feed RESP data to this client's reader */
                redisReaderFeed(c->mux_reader, mc->payload_buf, mc->current_payload_len);

                /* Try to extract and process replies */
                void *reply = NULL;
                while (c->pending) {
                    if (redisReaderGetReply(c->mux_reader, &reply) != REDIS_OK) {
                        fprintf(stderr, "Error: %s\n", c->mux_reader->errstr);
                        exit(1);
                    }
                    if (reply != NULL) {
                        if (c->latency < 0) c->latency = ustime() - c->start;
                        int ret = processReply(c, reply);
                        if (ret == 1) break; /* clientDone called */
                    } else {
                        break; /* Need more data */
                    }
                }
            }
        }
        /* Other frame types silently ignored */
    }
}

/* Read handler for the shared MUX connection fd */
static void muxSharedReadHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    muxSharedConn *mc = privdata;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Read raw data from socket */
    char tmpbuf[1024 * 64];
    ssize_t nread = read(fd, tmpbuf, sizeof(tmpbuf));
    if (nread <= 0) {
        if (nread == -1 && (errno == EAGAIN || errno == EINTR)) return;
        fprintf(stderr, "MUX: read error: %s\n",
                nread == 0 ? "connection closed" : strerror(errno));
        exit(1);
    }

    /* Append to recvbuf */
    benchMuxBufGrow(&mc->recvbuf, &mc->recvbuf_alloc, mc->recvbuf_len, (size_t)nread);
    memcpy(mc->recvbuf + mc->recvbuf_len, tmpbuf, nread);
    mc->recvbuf_len += nread;

    /* Process frames */
    muxSharedConnProcessFrames(mc);
}

/* Write handler for the shared MUX connection fd.
 * Collects pending writes from all queued clients, wraps each in a MUX frame,
 * and sends them in a single batch. */
static void muxSharedWriteHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    muxSharedConn *mc = privdata;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* First, flush any leftover sendbuf from previous partial write */
    if (mc->sendbuf_len > mc->sendbuf_pos) {
        ssize_t nwritten = write(fd, mc->sendbuf + mc->sendbuf_pos,
                                 mc->sendbuf_len - mc->sendbuf_pos);
        if (nwritten <= 0) {
            if (nwritten == -1 && (errno == EAGAIN || errno == EINTR)) return;
            fprintf(stderr, "MUX: write error: %s\n", strerror(errno));
            exit(1);
        }
        mc->sendbuf_pos += nwritten;
        if (mc->sendbuf_pos < mc->sendbuf_len) return; /* Still have data to flush */
        mc->sendbuf_len = 0;
        mc->sendbuf_pos = 0;
    }

    /* Build new batch from write queue */
    if (mc->write_queue_len == 0) {
        /* Nothing to write, unregister write event but keep read */
        aeDeleteFileEvent(el, fd, AE_WRITABLE);
        return;
    }

    /* Estimate total size and build MUX frames */
    mc->sendbuf_len = 0;
    mc->sendbuf_pos = 0;

    for (int i = 0; i < mc->write_queue_len; i++) {
        client c = mc->write_queue[i];
        size_t cmdlen = sdslen(c->obuf) - c->written;
        if (cmdlen <= 0) continue;

        /* Initialize timing for this request batch if not yet done */
        if (c->start == 0) {
            /* Enforce upper bound to number of requests. */
            int requests_issued = 0;
            atomicGetIncr(config.requests_issued, requests_issued, config.pipeline);
            if (requests_issued >= config.requests) {
                continue;
            }
            if (config.randomkeys) randomizeClientKey(c);
            c->start = ustime();
            c->latency = -1;
        }

        /* Grow sendbuf for header + payload */
        benchMuxBufGrow(&mc->sendbuf, &mc->sendbuf_alloc,
                        mc->sendbuf_len, BENCH_MUX_HEADER_SIZE + cmdlen);

        /* Write MUX frame header */
        benchMuxEncodeHeader((unsigned char *)(mc->sendbuf + mc->sendbuf_len),
                             BENCH_MUX_FRAME_DATA, c->mux_stream_id, (uint32_t)cmdlen);
        mc->sendbuf_len += BENCH_MUX_HEADER_SIZE;

        /* Copy RESP payload */
        memcpy(mc->sendbuf + mc->sendbuf_len, c->obuf + c->written, cmdlen);
        mc->sendbuf_len += cmdlen;

        /* Mark client's obuf as consumed and set up for reading replies */
        c->written = sdslen(c->obuf);
    }
    mc->write_queue_len = 0;

    /* Send the batch */
    if (mc->sendbuf_len > 0) {
        ssize_t nwritten = write(fd, mc->sendbuf, mc->sendbuf_len);
        if (nwritten <= 0) {
            if (nwritten == -1 && (errno == EAGAIN || errno == EINTR)) {
                /* Will retry on next write event */
                return;
            }
            fprintf(stderr, "MUX: write error: %s\n", strerror(errno));
            exit(1);
        }
        mc->sendbuf_pos = nwritten;
        if ((size_t)nwritten >= mc->sendbuf_len) {
            mc->sendbuf_len = 0;
            mc->sendbuf_pos = 0;
            /* All written: unregister write event */
            aeDeleteFileEvent(el, fd, AE_WRITABLE);
        }
    } else {
        aeDeleteFileEvent(el, fd, AE_WRITABLE);
    }
}

/* Schedule a write event on the shared MUX connection */
static void muxSharedConnScheduleWrite(muxSharedConn *mc) {
    aeEventLoop *el;
    if (mc->thread_id < 0) el = config.el;
    else el = config.threads[mc->thread_id]->el;
    aeCreateFileEvent(el, mc->fd, AE_WRITABLE, muxSharedWriteHandler, mc);
}

/* ========================== End MUX Shared Connection ========================== */

static void freeClient(client c) {
    aeEventLoop *el = CLIENT_GET_EVENTLOOP(c);
    listNode *ln;

    if (config.mux_mode && c->mux_conn) {
        /* In MUX mode, the fd belongs to the shared connection.
         * Don't delete fd events here (they belong to muxSharedConn).
         * Don't free c->context (it's shared). */
        if (c->thread_id >= 0) {
            int requests_finished = 0;
            atomicGet(config.requests_finished, requests_finished);
            if (requests_finished >= config.requests) {
                aeStop(el);
            }
        }
        /* Unregister from shared conn */
        uint32_t idx = c->mux_stream_id / 2;
        if (idx < 1024 && c->mux_conn->clients[idx] == c)
            c->mux_conn->clients[idx] = NULL;
        c->mux_conn->num_clients--;
        if (c->mux_reader) redisReaderFree(c->mux_reader);
        /* c->context is NULL in MUX shared mode, don't free */
        sdsfree(c->obuf);
        zfree(c->randptr);
        zfree(c->stagptr);
        zfree(c);
        if (config.num_threads) pthread_mutex_lock(&(config.liveclients_mutex));
        config.liveclients--;
        ln = listSearchKey(config.clients, c);
        assert(ln != NULL);
        listDelNode(config.clients, ln);
        if (config.num_threads) pthread_mutex_unlock(&(config.liveclients_mutex));
        return;
    }

    aeDeleteFileEvent(el,c->context->fd,AE_WRITABLE);
    aeDeleteFileEvent(el,c->context->fd,AE_READABLE);
    if (c->thread_id >= 0) {
        int requests_finished = 0;
        atomicGet(config.requests_finished, requests_finished);
        if (requests_finished >= config.requests) {
            aeStop(el);
        }
    }
    redisFree(c->context);
    sdsfree(c->obuf);
    zfree(c->randptr);
    zfree(c->stagptr);
    zfree(c);
    if (config.num_threads) pthread_mutex_lock(&(config.liveclients_mutex));
    config.liveclients--;
    ln = listSearchKey(config.clients,c);
    assert(ln != NULL);
    listDelNode(config.clients,ln);
    if (config.num_threads) pthread_mutex_unlock(&(config.liveclients_mutex));
}

static void freeAllClients(void) {
    listNode *ln = config.clients->head, *next;

    while(ln) {
        next = ln->next;
        freeClient(ln->value);
        ln = next;
    }
}

static void resetClient(client c) {
    if (config.mux_mode && c->mux_conn) {
        /* MUX mode: don't touch fd events (they belong to muxSharedConn).
         * Queue this client for writing via the shared connection. */
        c->written = 0;
        c->pending = config.pipeline;
        /* Randomize keys and set start time for the new request batch */
        int requests_issued = 0;
        atomicGetIncr(config.requests_issued, requests_issued, config.pipeline);
        if (requests_issued >= config.requests) {
            freeClient(c);
            return;
        }
        if (config.randomkeys) randomizeClientKey(c);
        if (config.cluster_mode && c->staglen > 0) setClusterKeyHashTag(c);
        atomicGet(config.slots_last_update, c->slots_last_update);
        c->start = ustime();
        c->latency = -1;
        muxSharedConnQueueWrite(c->mux_conn, c);
        muxSharedConnScheduleWrite(c->mux_conn);
        return;
    }
    aeEventLoop *el = CLIENT_GET_EVENTLOOP(c);
    aeDeleteFileEvent(el,c->context->fd,AE_WRITABLE);
    aeDeleteFileEvent(el,c->context->fd,AE_READABLE);
    aeCreateFileEvent(el,c->context->fd,AE_WRITABLE,writeHandler,c);
    c->written = 0;
    c->pending = config.pipeline;
}

static void randomizeClientKey(client c) {
    size_t i;

    for (i = 0; i < c->randlen; i++) {
        char *p = c->randptr[i]+11;
        size_t r = 0;
        if (config.randomkeys_keyspacelen != 0)
            r = random() % config.randomkeys_keyspacelen;
        size_t j;

        for (j = 0; j < 12; j++) {
            *p = '0'+r%10;
            r/=10;
            p--;
        }
    }
}

static void setClusterKeyHashTag(client c) {
    assert(c->thread_id >= 0);
    clusterNode *node = c->cluster_node;
    assert(node);
    int is_updating_slots = 0;
    atomicGet(config.is_updating_slots, is_updating_slots);
    /* If updateClusterSlotsConfiguration is updating the slots array,
     * call updateClusterSlotsConfiguration is order to block the thread
     * since the mutex is locked. When the slots will be updated by the
     * thread that's actually performing the update, the execution of
     * updateClusterSlotsConfiguration won't actually do anything, since
     * the updated_slots_count array will be already NULL. */
    if (is_updating_slots) updateClusterSlotsConfiguration();
    int slot = node->slots[rand() % node->slots_count];
    const char *tag = crc16_slot_table[slot];
    int taglen = strlen(tag);
    size_t i;
    for (i = 0; i < c->staglen; i++) {
        char *p = c->stagptr[i] + 1;
        p[0] = tag[0];
        p[1] = (taglen >= 2 ? tag[1] : '}');
        p[2] = (taglen == 3 ? tag[2] : '}');
    }
}

static void clientDone(client c) {
    int requests_finished = 0;
    atomicGet(config.requests_finished, requests_finished);
    if (requests_finished >= config.requests) {
        freeClient(c);
        if (!config.num_threads && config.el) aeStop(config.el);
        return;
    }
    if (config.keepalive) {
        resetClient(c);
    } else {
        if (config.num_threads) pthread_mutex_lock(&(config.liveclients_mutex));
        config.liveclients--;
        createMissingClients(c);
        config.liveclients++;
        if (config.num_threads)
            pthread_mutex_unlock(&(config.liveclients_mutex));
        freeClient(c);
    }
}

/* Process a single RESP reply for both normal and MUX modes.
 * Returns 1 if the client is done (all pending replies consumed), 0 otherwise, -1 on error. */
static int processReply(client c, void *reply) {
    if (reply == (void*)REDIS_REPLY_ERROR) {
        fprintf(stderr,"Unexpected error reply, exiting...\n");
        exit(1);
    }
    redisReply *r = reply;
    if (r->type == REDIS_REPLY_ERROR) {
        if (c->cluster_node && c->staglen) {
            int fetch_slots = 0, do_wait = 0;
            if (!strncmp(r->str,"MOVED",5) || !strncmp(r->str,"ASK",3))
                fetch_slots = 1;
            else if (!strncmp(r->str,"CLUSTERDOWN",11)) {
                fetch_slots = 1;
                do_wait = 1;
                fprintf(stderr, "Error from server %s:%d: %s.\n",
                        c->cluster_node->ip,
                        c->cluster_node->port,
                        r->str);
            }
            if (do_wait) sleep(1);
            if (fetch_slots && !fetchClusterSlotsConfiguration(c))
                exit(1);
        } else {
            if (c->cluster_node) {
                fprintf(stderr, "Error from server %s:%d: %s\n",
                     c->cluster_node->ip,
                     c->cluster_node->port,
                     r->str);
            } else fprintf(stderr, "Error from server: %s\n", r->str);
            exit(1);
        }
    }

    freeReplyObject(reply);
    /* This is an OK for prefix commands such as auth and select.*/
    if (c->prefix_pending > 0) {
        c->prefix_pending--;
        c->pending--;
        /* Discard prefix commands on first response.*/
        if (c->prefixlen > 0) {
            size_t j;
            sdsrange(c->obuf, c->prefixlen, -1);
            for (j = 0; j < c->randlen; j++)
                c->randptr[j] -= c->prefixlen;
            for (j = 0; j < c->staglen; j++)
                c->stagptr[j] -= c->prefixlen;
            c->prefixlen = 0;
        }
        /* Once all prefix commands are done, activate hiredis MUX layer.
         * The hiredis MUX funcs will transparently handle frame encoding/decoding. */
        if (c->prefix_pending == 0 && config.mux_mode) {
            if (redisActivateMux(c->context, 1) != REDIS_OK) {
                fprintf(stderr, "Error: Failed to activate MUX: %s\n",
                        c->context->errstr);
                exit(1);
            }
            c->mux_hello_done = 1;
            /* sdsrange above removed the prefix from obuf, so obuf now
             * contains only the actual commands. Copy them into the hiredis
             * context obuf and send through MUX framing immediately. */
            size_t cmdlen = sdslen(c->obuf);
            if (cmdlen > 0) {
                hisds newbuf = hi_sdscatlen(c->context->obuf, c->obuf, cmdlen);
                if (newbuf == NULL) {
                    fprintf(stderr, "Error: Out of memory\n");
                    exit(1);
                }
                c->context->obuf = newbuf;
                c->written = cmdlen; /* Mark all as consumed from our obuf */

                /* Flush the MUX-framed data to the server */
                int done = 0;
                if (redisBufferWrite(c->context, &done) == REDIS_ERR) {
                    fprintf(stderr, "Error writing MUX data: %s\n",
                            c->context->errstr);
                    exit(1);
                }
                if (!done) {
                    /* Partial write; need to continue writing */
                    aeEventLoop *el = CLIENT_GET_EVENTLOOP(c);
                    aeDeleteFileEvent(el, c->context->fd, AE_READABLE);
                    aeCreateFileEvent(el, c->context->fd, AE_WRITABLE, writeHandler, c);
                }
                /* If done, stay in AE_READABLE mode to receive replies */
            }
        }
        return 0;
    }
    int requests_finished = 0;
    atomicGetIncr(config.requests_finished, requests_finished, 1);
    if (requests_finished < config.requests){
        if (config.num_threads == 0) {
            hdr_record_value(
            config.latency_histogram,
            (long)c->latency<=CONFIG_LATENCY_HISTOGRAM_MAX_VALUE ? (long)c->latency : CONFIG_LATENCY_HISTOGRAM_MAX_VALUE);
            hdr_record_value(
            config.current_sec_latency_histogram,
            (long)c->latency<=CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE ? (long)c->latency : CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE);
        } else {
            hdr_record_value_atomic(
            config.latency_histogram,
            (long)c->latency<=CONFIG_LATENCY_HISTOGRAM_MAX_VALUE ? (long)c->latency : CONFIG_LATENCY_HISTOGRAM_MAX_VALUE);
            hdr_record_value_atomic(
            config.current_sec_latency_histogram,
            (long)c->latency<=CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE ? (long)c->latency : CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE);
        }
    }
    c->pending--;
    if (c->pending == 0) {
        clientDone(c);
        return 1;
    }
    return 0;
}

/* MUX mode read handler: since hiredis MUX layer handles frame
 * decoding transparently, this just uses standard redisBufferRead. */
static void muxReadHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    void *reply = NULL;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    if (c->latency < 0) c->latency = ustime()-(c->start);

    if (redisBufferRead(c->context) != REDIS_OK) {
        fprintf(stderr,"Error: %s\n",c->context->errstr);
        exit(1);
    } else {
        while(c->pending) {
            if (redisGetReply(c->context,&reply) != REDIS_OK) {
                fprintf(stderr,"Error: %s\n",c->context->errstr);
                exit(1);
            }
            if (reply != NULL) {
                int ret = processReply(c, reply);
                if (ret == 1) break; /* clientDone called */
            } else {
                break;
            }
        }
    }
}

static void readHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    void *reply = NULL;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* In MUX mode, once the HELLO MULTIPLEX handshake is done and MUX funcs
     * are active, use the muxReadHandler which relies on hiredis MUX layer. */
    if (config.mux_mode && c->mux_hello_done) {
        muxReadHandler(el, fd, privdata, mask);
        return;
    }

    /* Calculate latency only for the first read event. This means that the
     * server already sent the reply and we need to parse it. Parsing overhead
     * is not part of the latency, so calculate it only once, here. */
    if (c->latency < 0) c->latency = ustime()-(c->start);

    if (redisBufferRead(c->context) != REDIS_OK) {
        fprintf(stderr,"Error: %s\n",c->context->errstr);
        exit(1);
    } else {
        while(c->pending) {
            if (redisGetReply(c->context,&reply) != REDIS_OK) {
                fprintf(stderr,"Error: %s\n",c->context->errstr);
                exit(1);
            }
            if (reply != NULL) {
                int ret = processReply(c, reply);
                if (ret == 1) break; /* clientDone called */
            } else {
                break;
            }
        }
    }
}

/* In MUX mode, the hiredis MUX write function automatically wraps RESP data
 * in MUX DATA frames, so we just send plain RESP commands in obuf. */

static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Initialize request when nothing was written. */
    if (c->written == 0) {
        /* Enforce upper bound to number of requests. */
        int requests_issued = 0;
        atomicGetIncr(config.requests_issued, requests_issued, config.pipeline);
        if (requests_issued >= config.requests) {
            return;
        }

        /* Really initialize: randomize keys and set start time. */
        if (config.randomkeys) randomizeClientKey(c);
        if (config.cluster_mode && c->staglen > 0) setClusterKeyHashTag(c);
        atomicGet(config.slots_last_update, c->slots_last_update);
        c->start = ustime();
        c->latency = -1;
    }

    /* In MUX mode after HELLO handshake, use hiredis redisBufferWrite so that
     * the MUX funcs->write() is called, which automatically wraps data in MUX frames.
     * We copy the un-sent portion of our obuf into the hiredis context obuf. */
    if (config.mux_mode && c->mux_hello_done) {
        const ssize_t writeLen = sdslen(c->obuf) - c->written;
        if (writeLen > 0) {
            hisds newbuf = hi_sdscatlen(c->context->obuf,
                                        c->obuf + c->written, writeLen);
            if (newbuf == NULL) {
                fprintf(stderr, "Error: Out of memory\n");
                freeClient(c);
                return;
            }
            c->context->obuf = newbuf;
            c->written = sdslen(c->obuf); /* Mark all as consumed from our side */
        }

        int done = 0;
        if (redisBufferWrite(c->context, &done) == REDIS_ERR) {
            fprintf(stderr, "Error writing to the server: %s\n", c->context->errstr);
            freeClient(c);
            return;
        }
        if (done) {
            aeDeleteFileEvent(el, c->context->fd, AE_WRITABLE);
            aeCreateFileEvent(el, c->context->fd, AE_READABLE, readHandler, c);
        }
        return;
    }

    /* Determine how much to write. In MUX mode before the handshake is done,
     * only send the prefix commands (HELLO etc.), not the actual commands.
     * The actual commands must be sent after MUX funcs are activated. */
    const ssize_t buflen = config.mux_mode && !c->mux_hello_done
                           ? (ssize_t)c->prefixlen
                           : (ssize_t)sdslen(c->obuf);
    const ssize_t writeLen = buflen-c->written;
    if (writeLen > 0) {
        void *ptr = c->obuf+c->written;
        while(1) {
            /* Optimistically try to write before checking if the file descriptor
             * is actually writable. At worst we get EAGAIN. */
            const ssize_t nwritten = cliWriteConn(c->context,ptr,writeLen);
            if (nwritten != writeLen) {
                if (nwritten == -1 && errno != EAGAIN) {
                    if (errno != EPIPE)
                        fprintf(stderr, "Error writing to the server: %s\n", strerror(errno));
                    freeClient(c);
                    return;
                } else if (nwritten > 0) {
                    c->written += nwritten;
                    return;
                }
            } else {
                c->written += nwritten;
                aeDeleteFileEvent(el,c->context->fd,AE_WRITABLE);
                aeCreateFileEvent(el,c->context->fd,AE_READABLE,readHandler,c);
                return;
            }
        }
    }
}

/* Create a benchmark client, configured to send the command passed as 'cmd' of
 * 'len' bytes.
 *
 * The command is copied N times in the client output buffer (that is reused
 * again and again to send the request to the server) accordingly to the configured
 * pipeline size.
 *
 * Also an initial SELECT command is prepended in order to make sure the right
 * database is selected, if needed. The initial SELECT will be discarded as soon
 * as the first reply is received.
 *
 * To create a client from scratch, the 'from' pointer is set to NULL. If instead
 * we want to create a client using another client as reference, the 'from' pointer
 * points to the client to use as reference. In such a case the following
 * information is take from the 'from' client:
 *
 * 1) The command line to use.
 * 2) The offsets of the __rand_int__ elements inside the command line, used
 *    for arguments randomization.
 *
 * Even when cloning another client, prefix commands are applied if needed.*/
static client createClient(char *cmd, size_t len, client from, int thread_id) {
    int j;
    int is_cluster_client = (config.cluster_mode && thread_id >= 0);
    client c = zmalloc(sizeof(struct _client));

    /* Initialize MUX fields */
    c->mux_hello_done = 0;
    c->mux_stream_id = 0;
    c->mux_conn = NULL;
    c->mux_reader = NULL;

    if (config.mux_mode && !is_cluster_client) {
        /* ===== MUX shared connection mode =====
         * Use the thread's shared TCP connection instead of creating a new one.
         * AUTH, SELECT, HELLO are already done in muxSharedConnCreate(). */
        benchmarkThread *thread = NULL;
        muxSharedConn *mc = NULL;
        if (thread_id >= 0 && config.threads) {
            thread = config.threads[thread_id];
            mc = thread->mux_conn;
        }
        /* For thread_id < 0 (single-threaded mode), we store the mux conn
         * in a static variable that we create on first call */
        static muxSharedConn *s_single_thread_mux = NULL;
        if (thread_id < 0) {
            if (!s_single_thread_mux)
                s_single_thread_mux = muxSharedConnCreate(-1);
            mc = s_single_thread_mux;
        }

        if (!mc) {
            fprintf(stderr, "MUX: no shared connection for thread %d\n", thread_id);
            exit(1);
        }

        c->context = NULL; /* No per-client connection in MUX mode */
        c->mux_conn = mc;
        c->mux_stream_id = mc->next_stream_id;
        mc->next_stream_id += 2; /* Client streams: 1, 3, 5, ... */
        c->mux_reader = redisReaderCreate();
        c->mux_reader->maxbuf = 0;
        c->mux_hello_done = 1; /* HELLO already done in muxSharedConnCreate */
        c->thread_id = thread_id;
        c->cluster_node = NULL;

        /* Build the request buffer: just the commands, no prefix */
        c->obuf = sdsempty();
        c->prefix_pending = 0;
        c->prefixlen = 0;

        if (from) {
            c->obuf = sdscatlen(c->obuf,
                from->obuf + from->prefixlen,
                sdslen(from->obuf) - from->prefixlen);
        } else {
            for (j = 0; j < config.pipeline; j++)
                c->obuf = sdscatlen(c->obuf, cmd, len);
        }

        c->written = 0;
        c->pending = config.pipeline;
        c->start = 0;
        c->latency = -1;
        c->randptr = NULL;
        c->randlen = 0;
        c->randfree = 0;
        c->stagptr = NULL;
        c->staglen = 0;
        c->stagfree = 0;

        /* Set up randomization pointers */
        if (config.randomkeys) {
            if (from) {
                c->randlen = from->randlen;
                c->randfree = 0;
                c->randptr = zmalloc(sizeof(char*) * c->randlen);
                for (j = 0; j < (int)c->randlen; j++) {
                    c->randptr[j] = c->obuf + (from->randptr[j] - from->obuf);
                    c->randptr[j] += c->prefixlen - from->prefixlen;
                }
            } else {
                char *p = c->obuf;
                c->randlen = 0;
                c->randfree = RANDPTR_INITIAL_SIZE;
                c->randptr = zmalloc(sizeof(char*) * c->randfree);
                while ((p = strstr(p, "__rand_int__")) != NULL) {
                    if (c->randfree == 0) {
                        c->randptr = zrealloc(c->randptr, sizeof(char*) * c->randlen * 2);
                        c->randfree += c->randlen;
                    }
                    c->randptr[c->randlen++] = p;
                    c->randfree--;
                    p += 12;
                }
            }
        }

        /* Register with shared connection */
        muxSharedConnRegisterClient(mc, c);

        /* Register read event on the shared fd (idempotent if already registered) */
        aeEventLoop *el = NULL;
        if (thread_id < 0) el = config.el;
        else el = thread->el;
        aeCreateFileEvent(el, mc->fd, AE_READABLE, muxSharedReadHandler, mc);

        /* Queue the initial write */
        muxSharedConnQueueWrite(mc, c);
        muxSharedConnScheduleWrite(mc);

        listAddNodeTail(config.clients, c);
        atomicIncr(config.liveclients, 1);
        atomicGet(config.slots_last_update, c->slots_last_update);
        return c;
    }

    /* ===== Normal (non-MUX) mode ===== */
    const char *ip = NULL;
    int port = 0;
    c->cluster_node = NULL;
    if (config.hostsocket == NULL || is_cluster_client) {
        if (!is_cluster_client) {
            ip = config.conn_info.hostip;
            port = config.conn_info.hostport;
        } else {
            int node_idx = 0;
            if (config.num_threads < config.cluster_node_count)
                node_idx = config.liveclients % config.cluster_node_count;
            else
                node_idx = thread_id % config.cluster_node_count;
            clusterNode *node = config.cluster_nodes[node_idx];
            assert(node != NULL);
            ip = (const char *) node->ip;
            port = node->port;
            c->cluster_node = node;
        }
        c->context = redisConnectNonBlock(ip,port);
    } else {
        c->context = redisConnectUnixNonBlock(config.hostsocket);
    }
    if (c->context->err) {
        fprintf(stderr,"Could not connect to Redis at ");
        if (config.hostsocket == NULL || is_cluster_client)
            fprintf(stderr,"%s:%d: %s\n",ip,port,c->context->errstr);
        else
            fprintf(stderr,"%s: %s\n",config.hostsocket,c->context->errstr);
        exit(1);
    }
    if (config.tls==1) {
        const char *err = NULL;
        if (cliSecureConnection(c->context, config.sslconfig, &err) == REDIS_ERR && err) {
            fprintf(stderr, "Could not negotiate a TLS connection: %s\n", err);
            exit(1);
        }
    }
    c->thread_id = thread_id;
    /* Suppress hiredis cleanup of unused buffers for max speed. */
    c->context->reader->maxbuf = 0;

    /* Build the request buffer:
     * Queue N requests accordingly to the pipeline size, or simply clone
     * the example client buffer. */
    c->obuf = sdsempty();
    /* Prefix the request buffer with AUTH and/or SELECT commands, if applicable.
     * These commands are discarded after the first response, so if the client is
     * reused the commands will not be used again. */
    c->prefix_pending = 0;
    if (config.conn_info.auth) {
        char *buf = NULL;
        int len;
        if (config.conn_info.user == NULL)
            len = redisFormatCommand(&buf, "AUTH %s", config.conn_info.auth);
        else
            len = redisFormatCommand(&buf, "AUTH %s %s",
                                     config.conn_info.user, config.conn_info.auth);
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    if (config.enable_tracking) {
        char *buf = NULL;
        int len = redisFormatCommand(&buf, "CLIENT TRACKING on");
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    /* If a DB number different than zero is selected, prefix our request
     * buffer with the SELECT command, that will be discarded the first
     * time the replies are received, so if the client is reused the
     * SELECT command will not be used again. */
    if (config.conn_info.input_dbnum != 0 && !is_cluster_client) {
        c->obuf = sdscatprintf(c->obuf,"*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
            (int)sdslen(config.input_dbnumstr),config.input_dbnumstr);
        c->prefix_pending++;
    }

    if (config.mux_mode) {
        /* MUX mode: send HELLO 3 MULTIPLEX to enable MUX */
        char *buf = NULL;
        int len = redisFormatCommand(&buf, "HELLO 3 MULTIPLEX");
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    } else if (config.resp3) {
        char *buf = NULL;
        int len = redisFormatCommand(&buf, "HELLO 3");
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    c->prefixlen = sdslen(c->obuf);
    /* Append the request itself. */
    if (from) {
        c->obuf = sdscatlen(c->obuf,
            from->obuf+from->prefixlen,
            sdslen(from->obuf)-from->prefixlen);
    } else {
        /* In MUX mode, plain RESP commands are sent in obuf.
         * The hiredis MUX write layer automatically wraps them in MUX frames. */
        for (j = 0; j < config.pipeline; j++)
            c->obuf = sdscatlen(c->obuf,cmd,len);
    }

    c->written = 0;
    c->pending = config.pipeline+c->prefix_pending;
    c->randptr = NULL;
    c->randlen = 0;
    c->stagptr = NULL;
    c->staglen = 0;

    /* Initialize MUX state */
    c->mux_hello_done = 0;

    /* Find substrings in the output buffer that need to be randomized. */
    if (config.randomkeys) {
        if (from) {
            c->randlen = from->randlen;
            c->randfree = 0;
            c->randptr = zmalloc(sizeof(char*)*c->randlen);
            /* copy the offsets. */
            for (j = 0; j < (int)c->randlen; j++) {
                c->randptr[j] = c->obuf + (from->randptr[j]-from->obuf);
                /* Adjust for the different select prefix length. */
                c->randptr[j] += c->prefixlen - from->prefixlen;
            }
        } else {
            char *p = c->obuf;

            c->randlen = 0;
            c->randfree = RANDPTR_INITIAL_SIZE;
            c->randptr = zmalloc(sizeof(char*)*c->randfree);
            while ((p = strstr(p,"__rand_int__")) != NULL) {
                if (c->randfree == 0) {
                    c->randptr = zrealloc(c->randptr,sizeof(char*)*c->randlen*2);
                    c->randfree += c->randlen;
                }
                c->randptr[c->randlen++] = p;
                c->randfree--;
                p += 12; /* 12 is strlen("__rand_int__). */
            }
        }
    }
    /* If cluster mode is enabled, set slot hashtags pointers. */
    if (config.cluster_mode) {
        if (from) {
            c->staglen = from->staglen;
            c->stagfree = 0;
            c->stagptr = zmalloc(sizeof(char*)*c->staglen);
            /* copy the offsets. */
            for (j = 0; j < (int)c->staglen; j++) {
                c->stagptr[j] = c->obuf + (from->stagptr[j]-from->obuf);
                /* Adjust for the different select prefix length. */
                c->stagptr[j] += c->prefixlen - from->prefixlen;
            }
        } else {
            char *p = c->obuf;

            c->staglen = 0;
            c->stagfree = RANDPTR_INITIAL_SIZE;
            c->stagptr = zmalloc(sizeof(char*)*c->stagfree);
            while ((p = strstr(p,"{tag}")) != NULL) {
                if (c->stagfree == 0) {
                    c->stagptr = zrealloc(c->stagptr,
                                          sizeof(char*) * c->staglen*2);
                    c->stagfree += c->staglen;
                }
                c->stagptr[c->staglen++] = p;
                c->stagfree--;
                p += 5; /* 5 is strlen("{tag}"). */
            }
        }
    }
    aeEventLoop *el = NULL;
    if (thread_id < 0) el = config.el;
    else {
        benchmarkThread *thread = config.threads[thread_id];
        el = thread->el;
    }
    if (config.idlemode == 0)
        aeCreateFileEvent(el,c->context->fd,AE_WRITABLE,writeHandler,c);
    else
        /* In idle mode, clients still need to register readHandler for catching errors */
        aeCreateFileEvent(el,c->context->fd,AE_READABLE,readHandler,c);

    listAddNodeTail(config.clients,c);
    atomicIncr(config.liveclients, 1);
    atomicGet(config.slots_last_update, c->slots_last_update);
    return c;
}

static void createMissingClients(client c) {
    int n = 0;
    while(config.liveclients < config.numclients) {
        int thread_id = -1;
        if (config.num_threads)
            thread_id = config.liveclients % config.num_threads;
        createClient(NULL,0,c,thread_id);

        /* Listen backlog is quite limited on most systems */
        if (++n > 64) {
            usleep(50000);
            n = 0;
        }
    }
}

static void showLatencyReport(void) {

    const float reqpersec = (float)config.requests_finished/((float)config.totlatency/1000.0f);
    const float p0 = ((float) hdr_min(config.latency_histogram))/1000.0f;
    const float p50 = hdr_value_at_percentile(config.latency_histogram, 50.0 )/1000.0f;
    const float p95 = hdr_value_at_percentile(config.latency_histogram, 95.0 )/1000.0f;
    const float p99 = hdr_value_at_percentile(config.latency_histogram, 99.0 )/1000.0f;
    const float p100 = ((float) hdr_max(config.latency_histogram))/1000.0f;
    const float avg = hdr_mean(config.latency_histogram)/1000.0f;

    if (!config.quiet && !config.csv) {
        printf("%*s\r", config.last_printed_bytes, " "); // ensure there is a clean line
        printf("====== %s ======\n", config.title);
        printf("  %d requests completed in %.2f seconds\n", config.requests_finished,
            (float)config.totlatency/1000);
        printf("  %d parallel clients\n", config.numclients);
        printf("  %d bytes payload\n", config.datasize);
        printf("  keep alive: %d\n", config.keepalive);
        if (config.cluster_mode) {
            printf("  cluster mode: yes (%d masters)\n",
                   config.cluster_node_count);
            int m ;
            for (m = 0; m < config.cluster_node_count; m++) {
                clusterNode *node =  config.cluster_nodes[m];
                redisConfig *cfg = node->redis_config;
                if (cfg == NULL) continue;
                printf("  node [%d] configuration:\n",m );
                printf("    save: %s\n",
                    sdslen(cfg->save) ? cfg->save : "NONE");
                printf("    appendonly: %s\n", cfg->appendonly);
            }
        } else {
            if (config.redis_config) {
                printf("  host configuration \"save\": %s\n",
                       config.redis_config->save);
                printf("  host configuration \"appendonly\": %s\n",
                       config.redis_config->appendonly);
            }
        }
        printf("  multi-thread: %s\n", (config.num_threads ? "yes" : "no"));
        if (config.num_threads)
            printf("  threads: %d\n", config.num_threads);

        printf("\n");
        printf("Latency by percentile distribution:\n");
        struct hdr_iter iter;
        long long previous_cumulative_count = -1;
        const long long total_count = config.latency_histogram->total_count;
        hdr_iter_percentile_init(&iter, config.latency_histogram, 1);
        struct hdr_iter_percentiles *percentiles = &iter.specifics.percentiles;
        while (hdr_iter_next(&iter))
        {
            const double value = iter.highest_equivalent_value / 1000.0f;
            const double percentile = percentiles->percentile;
            const long long cumulative_count = iter.cumulative_count;
            if( previous_cumulative_count != cumulative_count || cumulative_count == total_count ){
                printf("%3.3f%% <= %.3f milliseconds (cumulative count %lld)\n", percentile, value, cumulative_count);
            }
            previous_cumulative_count = cumulative_count;
        }
        printf("\n");
        printf("Cumulative distribution of latencies:\n");
        previous_cumulative_count = -1;
        hdr_iter_linear_init(&iter, config.latency_histogram, 100);
        while (hdr_iter_next(&iter))
        {
            const double value = iter.highest_equivalent_value / 1000.0f;
            const long long cumulative_count = iter.cumulative_count;
            const double percentile = ((double)cumulative_count/(double)total_count)*100.0;
            if( previous_cumulative_count != cumulative_count || cumulative_count == total_count ){
                printf("%3.3f%% <= %.3f milliseconds (cumulative count %lld)\n", percentile, value, cumulative_count);
            }
            /* After the 2 milliseconds latency to have percentages split
             * by decimals will just add a lot of noise to the output. */
            if(iter.highest_equivalent_value > 2000){
                hdr_iter_linear_set_value_units_per_bucket(&iter,1000);
            }
            previous_cumulative_count = cumulative_count;
        }
        printf("\n");
        printf("Summary:\n");
        printf("  throughput summary: %.2f requests per second\n", reqpersec);
        printf("  latency summary (msec):\n");
        printf("    %9s %9s %9s %9s %9s %9s\n", "avg", "min", "p50", "p95", "p99", "max");
        printf("    %9.3f %9.3f %9.3f %9.3f %9.3f %9.3f\n", avg, p0, p50, p95, p99, p100);
    } else if (config.csv) {
        printf("\"%s\",\"%.2f\",\"%.3f\",\"%.3f\",\"%.3f\",\"%.3f\",\"%.3f\",\"%.3f\"\n", config.title, reqpersec, avg, p0, p50, p95, p99, p100);
    } else {
        printf("%*s\r", config.last_printed_bytes, " "); // ensure there is a clean line
        printf("%s: %.2f requests per second, p50=%.3f msec\n", config.title, reqpersec, p50);
    }
}

static void initBenchmarkThreads(void) {
    int i;
    if (config.threads) freeBenchmarkThreads();
    config.threads = zmalloc(config.num_threads * sizeof(benchmarkThread*));
    for (i = 0; i < config.num_threads; i++) {
        benchmarkThread *thread = createBenchmarkThread(i);
        config.threads[i] = thread;
    }
}

static void startBenchmarkThreads(void) {
    int i;
    for (i = 0; i < config.num_threads; i++) {
        benchmarkThread *t = config.threads[i];
        if (pthread_create(&(t->thread), NULL, execBenchmarkThread, t)){
            fprintf(stderr, "FATAL: Failed to start thread %d.\n", i);
            exit(1);
        }
    }
    for (i = 0; i < config.num_threads; i++)
        pthread_join(config.threads[i]->thread, NULL);
}

static void benchmark(const char *title, char *cmd, int len) {
    client c;

    config.title = title;
    config.requests_issued = 0;
    config.requests_finished = 0;
    config.previous_requests_finished = 0;
    config.last_printed_bytes = 0;
    hdr_init(
        CONFIG_LATENCY_HISTOGRAM_MIN_VALUE,  // Minimum value
        CONFIG_LATENCY_HISTOGRAM_MAX_VALUE,  // Maximum value
        config.precision,  // Number of significant figures
        &config.latency_histogram);  // Pointer to initialise
    hdr_init(
        CONFIG_LATENCY_HISTOGRAM_MIN_VALUE,  // Minimum value
        CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE,  // Maximum value
        config.precision,  // Number of significant figures
        &config.current_sec_latency_histogram);  // Pointer to initialise

    if (config.num_threads) initBenchmarkThreads();

    int thread_id = config.num_threads > 0 ? 0 : -1;
    c = createClient(cmd,len,NULL,thread_id);
    createMissingClients(c);

    config.start = mstime();
    if (!config.num_threads) aeMain(config.el);
    else startBenchmarkThreads();
    config.totlatency = mstime()-config.start;

    showLatencyReport();
    freeAllClients();
    if (config.threads) freeBenchmarkThreads();
    if (config.current_sec_latency_histogram) hdr_close(config.current_sec_latency_histogram);
    if (config.latency_histogram) hdr_close(config.latency_histogram);

}

/* Thread functions. */

static benchmarkThread *createBenchmarkThread(int index) {
    benchmarkThread *thread = zmalloc(sizeof(*thread));
    if (thread == NULL) return NULL;
    thread->index = index;
    thread->el = aeCreateEventLoop(1024*10);
    thread->mux_conn = NULL;
    aeCreateTimeEvent(thread->el,1,showThroughput,(void *)thread,NULL);

    /* In MUX mode, create the per-thread shared connection */
    if (config.mux_mode) {
        thread->mux_conn = muxSharedConnCreate(index);
    }
    return thread;
}

static void freeBenchmarkThread(benchmarkThread *thread) {
    if (thread->mux_conn) muxSharedConnFree(thread->mux_conn);
    if (thread->el) aeDeleteEventLoop(thread->el);
    zfree(thread);
}

static void freeBenchmarkThreads(void) {
    int i = 0;
    for (; i < config.num_threads; i++) {
        benchmarkThread *thread = config.threads[i];
        if (thread) freeBenchmarkThread(thread);
    }
    zfree(config.threads);
    config.threads = NULL;
}

static void *execBenchmarkThread(void *ptr) {
    benchmarkThread *thread = (benchmarkThread *) ptr;
    aeMain(thread->el);
    return NULL;
}

/* Cluster helper functions. */

static clusterNode *createClusterNode(char *ip, int port) {
    clusterNode *node = zmalloc(sizeof(*node));
    if (!node) return NULL;
    node->ip = ip;
    node->port = port;
    node->name = NULL;
    node->flags = 0;
    node->replicate = NULL;
    node->replicas_count = 0;
    node->slots = zmalloc(CLUSTER_SLOTS * sizeof(int));
    node->slots_count = 0;
    node->updated_slots = NULL;
    node->updated_slots_count = 0;
    node->migrating = NULL;
    node->importing = NULL;
    node->migrating_count = 0;
    node->importing_count = 0;
    node->redis_config = NULL;
    return node;
}

static void freeClusterNode(clusterNode *node) {
    int i;
    if (node->name) sdsfree(node->name);
    if (node->replicate) sdsfree(node->replicate);
    if (node->migrating != NULL) {
        for (i = 0; i < node->migrating_count; i++) sdsfree(node->migrating[i]);
        zfree(node->migrating);
    }
    if (node->importing != NULL) {
        for (i = 0; i < node->importing_count; i++) sdsfree(node->importing[i]);
        zfree(node->importing);
    }
    /* If the node is not the reference node, that uses the address from
     * config.conn_info.hostip and config.conn_info.hostport, then the node ip has been
     * allocated by fetchClusterConfiguration, so it must be freed. */
    if (node->ip && strcmp(node->ip, config.conn_info.hostip) != 0) sdsfree(node->ip);
    if (node->redis_config != NULL) freeRedisConfig(node->redis_config);
    zfree(node->slots);
    zfree(node);
}

static void freeClusterNodes(void) {
    int i = 0;
    for (; i < config.cluster_node_count; i++) {
        clusterNode *n = config.cluster_nodes[i];
        if (n) freeClusterNode(n);
    }
    zfree(config.cluster_nodes);
    config.cluster_nodes = NULL;
}

static clusterNode **addClusterNode(clusterNode *node) {
    int count = config.cluster_node_count + 1;
    config.cluster_nodes = zrealloc(config.cluster_nodes,
                                    count * sizeof(*node));
    if (!config.cluster_nodes) return NULL;
    config.cluster_nodes[config.cluster_node_count++] = node;
    return config.cluster_nodes;
}

/* TODO: This should be refactored to use CLUSTER SLOTS, the migrating/importing
 * information is anyway not used.
 */
static int fetchClusterConfiguration(void) {
    int success = 1;
    redisContext *ctx = NULL;
    redisReply *reply =  NULL;
    ctx = getRedisContext(config.conn_info.hostip, config.conn_info.hostport, config.hostsocket);
    if (ctx == NULL) {
        exit(1);
    }
    clusterNode *firstNode = createClusterNode((char *) config.conn_info.hostip,
                                               config.conn_info.hostport);
    if (!firstNode) {success = 0; goto cleanup;}
    reply = redisCommand(ctx, "CLUSTER NODES");
    success = (reply != NULL);
    if (!success) goto cleanup;
    success = (reply->type != REDIS_REPLY_ERROR);
    if (!success) {
        if (config.hostsocket == NULL) {
            fprintf(stderr, "Cluster node %s:%d replied with error:\n%s\n",
                    config.conn_info.hostip, config.conn_info.hostport, reply->str);
        } else {
            fprintf(stderr, "Cluster node %s replied with error:\n%s\n",
                    config.hostsocket, reply->str);
        }
        goto cleanup;
    }
    char *lines = reply->str, *p, *line;
    while ((p = strstr(lines, "\n")) != NULL) {
        *p = '\0';
        line = lines;
        lines = p + 1;
        char *name = NULL, *addr = NULL, *flags = NULL, *master_id = NULL;
        int i = 0;
        while ((p = strchr(line, ' ')) != NULL) {
            *p = '\0';
            char *token = line;
            line = p + 1;
            switch(i++){
            case 0: name = token; break;
            case 1: addr = token; break;
            case 2: flags = token; break;
            case 3: master_id = token; break;
            }
            if (i == 8) break; // Slots
        }
        if (!flags) {
            fprintf(stderr, "Invalid CLUSTER NODES reply: missing flags.\n");
            success = 0;
            goto cleanup;
        }
        int myself = (strstr(flags, "myself") != NULL);
        int is_replica = (strstr(flags, "slave") != NULL ||
                         (master_id != NULL && master_id[0] != '-'));
        if (is_replica) continue;
        if (addr == NULL) {
            fprintf(stderr, "Invalid CLUSTER NODES reply: missing addr.\n");
            success = 0;
            goto cleanup;
        }
        clusterNode *node = NULL;
        char *ip = NULL;
        int port = 0;
        char *paddr = strrchr(addr, ':');
        if (paddr != NULL) {
            *paddr = '\0';
            ip = addr;
            addr = paddr + 1;
            /* If internal bus is specified, then just drop it. */
            if ((paddr = strchr(addr, '@')) != NULL) *paddr = '\0';
            port = atoi(addr);
        }
        if (myself) {
            node = firstNode;
            if (ip != NULL && strcmp(node->ip, ip) != 0) {
                node->ip = sdsnew(ip);
                node->port = port;
            }
        } else {
            node = createClusterNode(sdsnew(ip), port);
        }
        if (node == NULL) {
            success = 0;
            goto cleanup;
        }
        if (name != NULL) node->name = sdsnew(name);
        if (i == 8) {
            int remaining = strlen(line);
            while (remaining > 0) {
                p = strchr(line, ' ');
                if (p == NULL) p = line + remaining;
                remaining -= (p - line);

                char *slotsdef = line;
                *p = '\0';
                if (remaining) {
                    line = p + 1;
                    remaining--;
                } else line = p;
                char *dash = NULL;
                if (slotsdef[0] == '[') {
                    slotsdef++;
                    if ((p = strstr(slotsdef, "->-"))) { // Migrating
                        *p = '\0';
                        p += 3;
                        char *closing_bracket = strchr(p, ']');
                        if (closing_bracket) *closing_bracket = '\0';
                        sds slot = sdsnew(slotsdef);
                        sds dst = sdsnew(p);
                        node->migrating_count += 2;
                        node->migrating =
                            zrealloc(node->migrating,
                                (node->migrating_count * sizeof(sds)));
                        node->migrating[node->migrating_count - 2] =
                            slot;
                        node->migrating[node->migrating_count - 1] =
                            dst;
                    }  else if ((p = strstr(slotsdef, "-<-"))) {//Importing
                        *p = '\0';
                        p += 3;
                        char *closing_bracket = strchr(p, ']');
                        if (closing_bracket) *closing_bracket = '\0';
                        sds slot = sdsnew(slotsdef);
                        sds src = sdsnew(p);
                        node->importing_count += 2;
                        node->importing = zrealloc(node->importing,
                            (node->importing_count * sizeof(sds)));
                        node->importing[node->importing_count - 2] =
                            slot;
                        node->importing[node->importing_count - 1] =
                            src;
                    }
                } else if ((dash = strchr(slotsdef, '-')) != NULL) {
                    p = dash;
                    int start, stop;
                    *p = '\0';
                    start = atoi(slotsdef);
                    stop = atoi(p + 1);
                    while (start <= stop) {
                        int slot = start++;
                        node->slots[node->slots_count++] = slot;
                    }
                } else if (p > slotsdef) {
                    int slot = atoi(slotsdef);
                    node->slots[node->slots_count++] = slot;
                }
            }
        }
        if (node->slots_count == 0) {
            fprintf(stderr,
                    "WARNING: Master node %s:%d has no slots, skipping...\n",
                    node->ip, node->port);
            continue;
        }
        if (!addClusterNode(node)) {
            success = 0;
            goto cleanup;
        }
    }
cleanup:
    if (ctx) redisFree(ctx);
    if (!success) {
        if (config.cluster_nodes) freeClusterNodes();
    }
    if (reply) freeReplyObject(reply);
    return success;
}

/* Request the current cluster slots configuration by calling CLUSTER SLOTS
 * and atomically update the slots after a successful reply. */
static int fetchClusterSlotsConfiguration(client c) {
    UNUSED(c);
    int success = 1, is_fetching_slots = 0, last_update = 0;
    size_t i;
    atomicGet(config.slots_last_update, last_update);
    if (c->slots_last_update < last_update) {
        c->slots_last_update = last_update;
        return -1;
    }
    redisReply *reply = NULL;
    atomicGetIncr(config.is_fetching_slots, is_fetching_slots, 1);
    if (is_fetching_slots) return -1; //TODO: use other codes || errno ?
    atomicSet(config.is_fetching_slots, 1);
    fprintf(stderr,
            "WARNING: Cluster slots configuration changed, fetching new one...\n");
    const char *errmsg = "Failed to update cluster slots configuration";
    static dictType dtype = {
        dictSdsHash,               /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictSdsKeyCompare,         /* key compare */
        NULL,                      /* key destructor */
        NULL,                      /* val destructor */
        NULL                       /* allow to expand */
    };
    /* printf("[%d] fetchClusterSlotsConfiguration\n", c->thread_id); */
    dict *masters = dictCreate(&dtype);
    redisContext *ctx = NULL;
    for (i = 0; i < (size_t) config.cluster_node_count; i++) {
        clusterNode *node = config.cluster_nodes[i];
        assert(node->ip != NULL);
        assert(node->name != NULL);
        assert(node->port);
        /* Use first node as entry point to connect to. */
        if (ctx == NULL) {
            ctx = getRedisContext(node->ip, node->port, NULL);
            if (!ctx) {
                success = 0;
                goto cleanup;
            }
        }
        if (node->updated_slots != NULL)
            zfree(node->updated_slots);
        node->updated_slots = NULL;
        node->updated_slots_count = 0;
        dictReplace(masters, node->name, node) ;
    }
    reply = redisCommand(ctx, "CLUSTER SLOTS");
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        success = 0;
        if (reply)
            fprintf(stderr,"%s\nCLUSTER SLOTS ERROR: %s\n",errmsg,reply->str);
        goto cleanup;
    }
    assert(reply->type == REDIS_REPLY_ARRAY);
    for (i = 0; i < reply->elements; i++) {
        redisReply *r = reply->element[i];
        assert(r->type == REDIS_REPLY_ARRAY);
        assert(r->elements >= 3);
        int from, to, slot;
        from = r->element[0]->integer;
        to = r->element[1]->integer;
        redisReply *nr =  r->element[2];
        assert(nr->type == REDIS_REPLY_ARRAY && nr->elements >= 3);
        assert(nr->element[2]->str != NULL);
        sds name =  sdsnew(nr->element[2]->str);
        dictEntry *entry = dictFind(masters, name);
        if (entry == NULL) {
            success = 0;
            fprintf(stderr, "%s: could not find node with ID %s in current "
                            "configuration.\n", errmsg, name);
            if (name) sdsfree(name);
            goto cleanup;
        }
        sdsfree(name);
        clusterNode *node = dictGetVal(entry);
        if (node->updated_slots == NULL)
            node->updated_slots = zcalloc(CLUSTER_SLOTS * sizeof(int));
        for (slot = from; slot <= to; slot++)
            node->updated_slots[node->updated_slots_count++] = slot;
    }
    updateClusterSlotsConfiguration();
cleanup:
    freeReplyObject(reply);
    redisFree(ctx);
    dictRelease(masters);
    atomicSet(config.is_fetching_slots, 0);
    return success;
}

/* Atomically update the new slots configuration. */
static void updateClusterSlotsConfiguration(void) {
    pthread_mutex_lock(&config.is_updating_slots_mutex);
    atomicSet(config.is_updating_slots, 1);
    int i;
    for (i = 0; i < config.cluster_node_count; i++) {
        clusterNode *node = config.cluster_nodes[i];
        if (node->updated_slots != NULL) {
            int *oldslots = node->slots;
            node->slots = node->updated_slots;
            node->slots_count = node->updated_slots_count;
            node->updated_slots = NULL;
            node->updated_slots_count = 0;
            zfree(oldslots);
        }
    }
    atomicSet(config.is_updating_slots, 0);
    atomicIncr(config.slots_last_update, 1);
    pthread_mutex_unlock(&config.is_updating_slots_mutex);
}

/* Generate random data for redis benchmark. See #7196. */
static void genBenchmarkRandomData(char *data, int count) {
    static uint32_t state = 1234;
    int i = 0;

    while (count--) {
        state = (state*1103515245+12345);
        data[i++] = '0'+((state>>16)&63);
    }
}

/* Returns number of consumed options. */
int parseOptions(int argc, char **argv) {
    int i;
    int lastarg;
    int exit_status = 1;
    char *tls_usage;

    for (i = 1; i < argc; i++) {
        lastarg = (i == (argc-1));

        if (!strcmp(argv[i],"-c")) {
            if (lastarg) goto invalid;
            config.numclients = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-v") || !strcmp(argv[i], "--version")) {
            sds version = cliVersion();
            printf("redis-benchmark %s\n", version);
            sdsfree(version);
            exit(0);
        } else if (!strcmp(argv[i],"-n")) {
            if (lastarg) goto invalid;
            config.requests = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-k")) {
            if (lastarg) goto invalid;
            config.keepalive = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-h")) {
            if (lastarg) goto invalid;
            sdsfree(config.conn_info.hostip);
            config.conn_info.hostip = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i],"-p")) {
            if (lastarg) goto invalid;
            config.conn_info.hostport = atoi(argv[++i]);
            if (config.conn_info.hostport < 0 || config.conn_info.hostport > 65535) {
                fprintf(stderr, "Invalid server port.\n");
                exit(1);
            }
        } else if (!strcmp(argv[i],"-s")) {
            if (lastarg) goto invalid;
            config.hostsocket = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-x")) {
            config.stdinarg = 1;
        } else if (!strcmp(argv[i],"-a") ) {
            if (lastarg) goto invalid;
            config.conn_info.auth = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i],"--user")) {
            if (lastarg) goto invalid;
            config.conn_info.user = sdsnew(argv[++i]);
        } else if (!strcmp(argv[i],"-u") && !lastarg) {
            parseRedisUri(argv[++i],"redis-benchmark",&config.conn_info,&config.tls);
            if (config.conn_info.hostport < 0 || config.conn_info.hostport > 65535) {
                fprintf(stderr, "Invalid server port.\n");
                exit(1);
            }
            config.input_dbnumstr = sdsfromlonglong(config.conn_info.input_dbnum);
        } else if (!strcmp(argv[i],"-3")) {
            config.resp3 = 1;
        } else if (!strcmp(argv[i],"-d")) {
            if (lastarg) goto invalid;
            config.datasize = atoi(argv[++i]);
            if (config.datasize < 1) config.datasize=1;
            if (config.datasize > 1024*1024*1024) config.datasize = 1024*1024*1024;
        } else if (!strcmp(argv[i],"-P")) {
            if (lastarg) goto invalid;
            config.pipeline = atoi(argv[++i]);
            if (config.pipeline <= 0) config.pipeline=1;
        } else if (!strcmp(argv[i],"-r")) {
            if (lastarg) goto invalid;
            const char *next = argv[++i], *p = next;
            if (*p == '-') {
                p++;
                if (*p < '0' || *p > '9') goto invalid;
            }
            config.randomkeys = 1;
            config.randomkeys_keyspacelen = atoi(next);
            if (config.randomkeys_keyspacelen < 0)
                config.randomkeys_keyspacelen = 0;
        } else if (!strcmp(argv[i],"-q")) {
            config.quiet = 1;
        } else if (!strcmp(argv[i],"--csv")) {
            config.csv = 1;
        } else if (!strcmp(argv[i],"-l")) {
            config.loop = 1;
        } else if (!strcmp(argv[i],"-I")) {
            config.idlemode = 1;
        } else if (!strcmp(argv[i],"-e")) {
            fprintf(stderr,
                    "WARNING: -e option has no effect. "
                    "We now immediately exit on error to avoid false results.\n");
        } else if (!strcmp(argv[i],"--seed")) {
            if (lastarg) goto invalid;
            int rand_seed = atoi(argv[++i]);
            srandom(rand_seed);
            init_genrand64(rand_seed);
        } else if (!strcmp(argv[i],"-t")) {
            if (lastarg) goto invalid;
            /* We get the list of tests to run as a string in the form
             * get,set,lrange,...,test_N. Then we add a comma before and
             * after the string in order to make sure that searching
             * for ",testname," will always get a match if the test is
             * enabled. */
            config.tests = sdsnew(",");
            config.tests = sdscat(config.tests,(char*)argv[++i]);
            config.tests = sdscat(config.tests,",");
            sdstolower(config.tests);
        } else if (!strcmp(argv[i],"--dbnum")) {
            if (lastarg) goto invalid;
            config.conn_info.input_dbnum = atoi(argv[++i]);
            config.input_dbnumstr = sdsfromlonglong(config.conn_info.input_dbnum);
        } else if (!strcmp(argv[i],"--precision")) {
            if (lastarg) goto invalid;
            config.precision = atoi(argv[++i]);
            if (config.precision < 0) config.precision = DEFAULT_LATENCY_PRECISION;
            if (config.precision > MAX_LATENCY_PRECISION) config.precision = MAX_LATENCY_PRECISION;
        } else if (!strcmp(argv[i],"--threads")) {
             if (lastarg) goto invalid;
             config.num_threads = atoi(argv[++i]);
             if (config.num_threads > MAX_THREADS) {
                 fprintf(stderr,
                         "WARNING: Too many threads, limiting threads to %d.\n",
                         MAX_THREADS);
                config.num_threads = MAX_THREADS;
             } else if (config.num_threads < 0) config.num_threads = 0;
        } else if (!strcmp(argv[i],"--cluster")) {
            config.cluster_mode = 1;
    } else if (!strcmp(argv[i],"--enable-tracking")) {
            config.enable_tracking = 1;
        } else if (!strcmp(argv[i],"--mux")) {
            config.mux_mode = 1;
        } else if (!strcmp(argv[i],"--mux-streams")) {
            if (lastarg) goto invalid;
            config.mux_streams = atoi(argv[++i]);
            if (config.mux_streams <= 0) config.mux_streams = MUX_DEFAULT_STREAMS;
            if (config.mux_streams > 1000) config.mux_streams = 1000;
        } else if (!strcmp(argv[i],"--help")) {
            exit_status = 0;
            goto usage;
        #ifdef USE_OPENSSL
        } else if (!strcmp(argv[i],"--tls")) {
            config.tls = 1;
        } else if (!strcmp(argv[i],"--sni")) {
            if (lastarg) goto invalid;
            config.sslconfig.sni = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"--cacertdir")) {
            if (lastarg) goto invalid;
            config.sslconfig.cacertdir = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"--cacert")) {
            if (lastarg) goto invalid;
            config.sslconfig.cacert = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"--insecure")) {
            config.sslconfig.skip_cert_verify = 1;
        } else if (!strcmp(argv[i],"--cert")) {
            if (lastarg) goto invalid;
            config.sslconfig.cert = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"--key")) {
            if (lastarg) goto invalid;
            config.sslconfig.key = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"--tls-ciphers")) {
            if (lastarg) goto invalid;
            config.sslconfig.ciphers = strdup(argv[++i]);
        #ifdef TLS1_3_VERSION
        } else if (!strcmp(argv[i],"--tls-ciphersuites")) {
            if (lastarg) goto invalid;
            config.sslconfig.ciphersuites = strdup(argv[++i]);
        #endif
        #endif
        } else {
            /* Assume the user meant to provide an option when the arg starts
             * with a dash. We're done otherwise and should use the remainder
             * as the command and arguments for running the benchmark. */
            if (argv[i][0] == '-') goto invalid;
            return i;
        }
    }

    return i;

invalid:
    printf("Invalid option \"%s\" or option argument missing\n\n",argv[i]);

usage:
    tls_usage =
#ifdef USE_OPENSSL
" --tls              Establish a secure TLS connection.\n"
" --sni <host>       Server name indication for TLS.\n"
" --cacert <file>    CA Certificate file to verify with.\n"
" --cacertdir <dir>  Directory where trusted CA certificates are stored.\n"
"                    If neither cacert nor cacertdir are specified, the default\n"
"                    system-wide trusted root certs configuration will apply.\n"
" --insecure         Allow insecure TLS connection by skipping cert validation.\n"
" --cert <file>      Client certificate to authenticate with.\n"
" --key <file>       Private key file to authenticate with.\n"
" --tls-ciphers <list> Sets the list of preferred ciphers (TLSv1.2 and below)\n"
"                    in order of preference from highest to lowest separated by colon (\":\").\n"
"                    See the ciphers(1ssl) manpage for more information about the syntax of this string.\n"
#ifdef TLS1_3_VERSION
" --tls-ciphersuites <list> Sets the list of preferred ciphersuites (TLSv1.3)\n"
"                    in order of preference from highest to lowest separated by colon (\":\").\n"
"                    See the ciphers(1ssl) manpage for more information about the syntax of this string,\n"
"                    and specifically for TLSv1.3 ciphersuites.\n"
#endif
#endif
"";

    printf(
"%s%s%s", /* Split to avoid strings longer than 4095 (-Woverlength-strings). */
"Usage: redis-benchmark [OPTIONS] [COMMAND ARGS...]\n\n"
"Options:\n"
" -h <hostname>      Server hostname (default 127.0.0.1)\n"
" -p <port>          Server port (default 6379)\n"
" -s <socket>        Server socket (overrides host and port)\n"
" -a <password>      Password for Redis Auth\n"
" --user <username>  Used to send ACL style 'AUTH username pass'. Needs -a.\n"
" -u <uri>           Server URI on format redis://user:password@host:port/dbnum\n"
"                    User, password and dbnum are optional. For authentication\n"
"                    without a username, use username 'default'. For TLS, use\n"
"                    the scheme 'rediss'.\n"
" -c <clients>       Number of parallel connections (default 50).\n"
"                    Note: If --cluster is used then number of clients has to be\n"
"                    the same or higher than the number of nodes.\n"
" -n <requests>      Total number of requests (default 100000)\n"
" -d <size>          Data size of SET/GET value in bytes (default 3)\n"
" --dbnum <db>       SELECT the specified db number (default 0)\n"
" -3                 Start session in RESP3 protocol mode.\n"
" --threads <num>    Enable multi-thread mode.\n"
" --cluster          Enable cluster mode.\n"
"                    If the command is supplied on the command line in cluster\n"
"                    mode, the key must contain \"{tag}\". Otherwise, the\n"
"                    command will not be sent to the right cluster node.\n"
" --enable-tracking  Send CLIENT TRACKING on before starting benchmark.\n"
" --mux              Enable MUX (stream-multiplexed) mode.\n"
" --mux-streams <n>  Number of MUX streams per connection (default 10).\n"
" -k <boolean>       1=keep alive 0=reconnect (default 1)\n"
" -r <keyspacelen>   Use random keys for SET/GET/INCR, random values for SADD,\n"
"                    random members and scores for ZADD.\n"
"                    Using this option the benchmark will expand the string\n"
"                    __rand_int__ inside an argument with a 12 digits number in\n"
"                    the specified range from 0 to keyspacelen-1. The\n"
"                    substitution changes every time a command is executed.\n"
"                    Default tests use this to hit random keys in the specified\n"
"                    range.\n"
"                    Note: If -r is omitted, all commands in a benchmark will\n"
"                    use the same key.\n"
" -P <numreq>        Pipeline <numreq> requests. Default 1 (no pipeline).\n"
" -q                 Quiet. Just show query/sec values\n"
" --precision        Number of decimal places to display in latency output (default 0)\n"
" --csv              Output in CSV format\n"
" -l                 Loop. Run the tests forever\n"
" -t <tests>         Only run the comma separated list of tests. The test\n"
"                    names are the same as the ones produced as output.\n"
"                    The -t option is ignored if a specific command is supplied\n"
"                    on the command line.\n"
" -I                 Idle mode. Just open N idle connections and wait.\n"
" -x                 Read last argument from STDIN.\n"
" --seed <num>       Set the seed for random number generator. Default seed is based on time.\n",
tls_usage,
" --help             Output this help and exit.\n"
" --version          Output version and exit.\n\n"
"Examples:\n\n"
" Run the benchmark with the default configuration against 127.0.0.1:6379:\n"
"   $ redis-benchmark\n\n"
" Use 20 parallel clients, for a total of 100k requests, against 192.168.1.1:\n"
"   $ redis-benchmark -h 192.168.1.1 -p 6379 -n 100000 -c 20\n\n"
" Fill 127.0.0.1:6379 with about 1 million keys only using the SET test:\n"
"   $ redis-benchmark -t set -n 1000000 -r 100000000\n\n"
" Benchmark 127.0.0.1:6379 for a few commands producing CSV output:\n"
"   $ redis-benchmark -t ping,set,get -n 100000 --csv\n\n"
" Benchmark a specific command line:\n"
"   $ redis-benchmark -r 10000 -n 10000 eval 'return redis.call(\"ping\")' 0\n\n"
" Fill a list with 10000 random elements:\n"
"   $ redis-benchmark -r 10000 -n 10000 lpush mylist __rand_int__\n\n"
" On user specified command lines __rand_int__ is replaced with a random integer\n"
" with a range of values selected by the -r option.\n"
    );
    exit(exit_status);
}

int showThroughput(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    benchmarkThread *thread = (benchmarkThread *)clientData;
    int liveclients = 0;
    int requests_finished = 0;
    int previous_requests_finished = 0;
    long long current_tick = mstime();
    atomicGet(config.liveclients, liveclients);
    atomicGet(config.requests_finished, requests_finished);
    atomicGet(config.previous_requests_finished, previous_requests_finished);

    if (liveclients == 0 && requests_finished != config.requests) {
        fprintf(stderr,"All clients disconnected... aborting.\n");
        exit(1);
    }
    if (config.num_threads && requests_finished >= config.requests) {
        aeStop(eventLoop);
        return AE_NOMORE;
    }
    if (config.csv) return SHOW_THROUGHPUT_INTERVAL;
    /* only first thread output throughput */
    if (thread != NULL && thread->index != 0) {
        return SHOW_THROUGHPUT_INTERVAL;
    }
    if (config.idlemode == 1) {
        printf("clients: %d\r", config.liveclients);
        fflush(stdout);
        return SHOW_THROUGHPUT_INTERVAL;
    }
    const float dt = (float)(current_tick-config.start)/1000.0;
    const float rps = (float)requests_finished/dt;
    const float instantaneous_dt = (float)(current_tick-config.previous_tick)/1000.0;
    const float instantaneous_rps = (float)(requests_finished-previous_requests_finished)/instantaneous_dt;
    config.previous_tick = current_tick;
    atomicSet(config.previous_requests_finished,requests_finished);
    printf("%*s\r", config.last_printed_bytes, " "); /* ensure there is a clean line */
    int printed_bytes = printf("%s: rps=%.1f (overall: %.1f) avg_msec=%.3f (overall: %.3f)\r", config.title, instantaneous_rps, rps, hdr_mean(config.current_sec_latency_histogram)/1000.0f, hdr_mean(config.latency_histogram)/1000.0f);
    config.last_printed_bytes = printed_bytes;
    hdr_reset(config.current_sec_latency_histogram);
    fflush(stdout);
    return SHOW_THROUGHPUT_INTERVAL;
}

/* Return true if the named test was selected using the -t command line
 * switch, or if all the tests are selected (no -t passed by user). */
int test_is_selected(const char *name) {
    char buf[256];
    int l = strlen(name);

    if (config.tests == NULL) return 1;
    buf[0] = ',';
    memcpy(buf+1,name,l);
    buf[l+1] = ',';
    buf[l+2] = '\0';
    return strstr(config.tests,buf) != NULL;
}

int main(int argc, char **argv) {
    int i;
    char *data, *cmd, *tag;
    int len;

    client c;

    srandom(time(NULL) ^ getpid());
    init_genrand64(ustime() ^ getpid());
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    memset(&config.sslconfig, 0, sizeof(config.sslconfig));
    config.numclients = 50;
    config.requests = 100000;
    config.liveclients = 0;
    config.el = aeCreateEventLoop(1024*10);
    aeCreateTimeEvent(config.el,1,showThroughput,NULL,NULL);
    config.keepalive = 1;
    config.datasize = 3;
    config.pipeline = 1;
    config.randomkeys = 0;
    config.randomkeys_keyspacelen = 0;
    config.quiet = 0;
    config.csv = 0;
    config.loop = 0;
    config.idlemode = 0;
    config.clients = listCreate();
    config.conn_info.hostip = sdsnew("127.0.0.1");
    config.conn_info.hostport = 6379;
    config.hostsocket = NULL;
    config.tests = NULL;
    config.conn_info.input_dbnum = 0;
    config.stdinarg = 0;
    config.conn_info.auth = NULL;
    config.precision = DEFAULT_LATENCY_PRECISION;
    config.num_threads = 0;
    config.threads = NULL;
    config.cluster_mode = 0;
    config.cluster_node_count = 0;
    config.cluster_nodes = NULL;
    config.redis_config = NULL;
    config.is_fetching_slots = 0;
    config.is_updating_slots = 0;
    config.slots_last_update = 0;
    config.enable_tracking = 0;
    config.resp3 = 0;
    config.mux_mode = 0;
    config.mux_streams = MUX_DEFAULT_STREAMS;

    i = parseOptions(argc,argv);
    argc -= i;
    argv += i;

    tag = "";

#ifdef USE_OPENSSL
    if (config.tls) {
        cliSecureInit();
    }
#endif

    if (config.cluster_mode) {
        // We only include the slot placeholder {tag} if cluster mode is enabled
        tag = ":{tag}";

        /* Fetch cluster configuration. */
        if (!fetchClusterConfiguration() || !config.cluster_nodes) {
            if (!config.hostsocket) {
                fprintf(stderr, "Failed to fetch cluster configuration from "
                                "%s:%d\n", config.conn_info.hostip, config.conn_info.hostport);
            } else {
                fprintf(stderr, "Failed to fetch cluster configuration from "
                                "%s\n", config.hostsocket);
            }
            exit(1);
        }
        if (config.cluster_node_count <= 1) {
            fprintf(stderr, "Invalid cluster: %d node(s).\n",
                    config.cluster_node_count);
            exit(1);
        }
        printf("Cluster has %d master nodes:\n\n", config.cluster_node_count);
        int i = 0;
        for (; i < config.cluster_node_count; i++) {
            clusterNode *node = config.cluster_nodes[i];
            if (!node) {
                fprintf(stderr, "Invalid cluster node #%d\n", i);
                exit(1);
            }
            printf("Master %d: ", i);
            if (node->name) printf("%s ", node->name);
            printf("%s:%d\n", node->ip, node->port);
            node->redis_config = getRedisConfig(node->ip, node->port, NULL);
            if (node->redis_config == NULL) {
                fprintf(stderr, "WARNING: Could not fetch node CONFIG %s:%d\n",
                        node->ip, node->port);
            }
        }
        printf("\n");
        /* Automatically set thread number to node count if not specified
         * by the user. */
        if (config.num_threads == 0)
            config.num_threads = config.cluster_node_count;
    } else {
        config.redis_config =
            getRedisConfig(config.conn_info.hostip, config.conn_info.hostport, config.hostsocket);
        if (config.redis_config == NULL) {
            fprintf(stderr, "WARNING: Could not fetch server CONFIG\n");
        }
    }
    if (config.num_threads > 0) {
        pthread_mutex_init(&(config.liveclients_mutex), NULL);
        pthread_mutex_init(&(config.is_updating_slots_mutex), NULL);
    }

    if (config.keepalive == 0) {
        fprintf(stderr,
                "WARNING: Keepalive disabled. You probably need "
                "'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and "
                "'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order "
                "to use a lot of clients/requests\n");
    }
    if (argc > 0 && config.tests != NULL) {
        fprintf(stderr, "WARNING: Option -t is ignored.\n");
    }

    if (config.idlemode) {
        printf("Creating %d idle connections and waiting forever (Ctrl+C when done)\n", config.numclients);
        int thread_id = -1, use_threads = (config.num_threads > 0);
        if (use_threads) {
            thread_id = 0;
            initBenchmarkThreads();
        }
        c = createClient("",0,NULL,thread_id); /* will never receive a reply */
        createMissingClients(c);
        if (use_threads) startBenchmarkThreads();
        else aeMain(config.el);
        /* and will wait for every */
    }
    if(config.csv){
        printf("\"test\",\"rps\",\"avg_latency_ms\",\"min_latency_ms\",\"p50_latency_ms\",\"p95_latency_ms\",\"p99_latency_ms\",\"max_latency_ms\"\n");
    }
    /* Run benchmark with command in the remainder of the arguments. */
    if (argc) {
        sds title = sdsnew(argv[0]);
        for (i = 1; i < argc; i++) {
            title = sdscatlen(title, " ", 1);
            title = sdscatlen(title, (char*)argv[i], strlen(argv[i]));
        }
        sds *sds_args = getSdsArrayFromArgv(argc, argv, 0);
        if (!sds_args) {
            fprintf(stderr, "Invalid quoted string\n");
            return 1;
        }
        if (config.stdinarg) {
            sds_args = sds_realloc(sds_args,(argc + 1) * sizeof(sds));
            sds_args[argc] = readArgFromStdin();
            argc++;
        }
        /* Setup argument length */
        size_t *argvlen = zmalloc(argc*sizeof(size_t));
        for (i = 0; i < argc; i++)
            argvlen[i] = sdslen(sds_args[i]);
        do {
            len = redisFormatCommandArgv(&cmd,argc,(const char**)sds_args,argvlen);
            // adjust the datasize to the parsed command
            config.datasize = len;
            benchmark(title,cmd,len);
            free(cmd);
        } while(config.loop);
        sdsfreesplitres(sds_args, argc);

        sdsfree(title);
        if (config.redis_config != NULL) freeRedisConfig(config.redis_config);
        zfree(argvlen);
        return 0;
    }

    /* Run default benchmark suite. */
    data = zmalloc(config.datasize+1);
    do {
        genBenchmarkRandomData(data, config.datasize);
        data[config.datasize] = '\0';

        if (test_is_selected("ping_inline") || test_is_selected("ping"))
            benchmark("PING_INLINE","PING\r\n",6);

        if (test_is_selected("ping_mbulk") || test_is_selected("ping")) {
            len = redisFormatCommand(&cmd,"PING");
            benchmark("PING_MBULK",cmd,len);
            free(cmd);
        }

        if (test_is_selected("set")) {
            len = redisFormatCommand(&cmd,"SET key%s:__rand_int__ %s",tag,data);
            benchmark("SET",cmd,len);
            free(cmd);
        }

        if (test_is_selected("get")) {
            len = redisFormatCommand(&cmd,"GET key%s:__rand_int__",tag);
            benchmark("GET",cmd,len);
            free(cmd);
        }

        if (test_is_selected("incr")) {
            len = redisFormatCommand(&cmd,"INCR counter%s:__rand_int__",tag);
            benchmark("INCR",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lpush")) {
            len = redisFormatCommand(&cmd,"LPUSH mylist%s %s",tag,data);
            benchmark("LPUSH",cmd,len);
            free(cmd);
        }

        if (test_is_selected("rpush")) {
            len = redisFormatCommand(&cmd,"RPUSH mylist%s %s",tag,data);
            benchmark("RPUSH",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lpop")) {
            len = redisFormatCommand(&cmd,"LPOP mylist%s",tag);
            benchmark("LPOP",cmd,len);
            free(cmd);
        }

        if (test_is_selected("rpop")) {
            len = redisFormatCommand(&cmd,"RPOP mylist%s",tag);
            benchmark("RPOP",cmd,len);
            free(cmd);
        }

        if (test_is_selected("sadd")) {
            len = redisFormatCommand(&cmd,
                "SADD myset%s element:__rand_int__",tag);
            benchmark("SADD",cmd,len);
            free(cmd);
        }

        if (test_is_selected("hset")) {
            len = redisFormatCommand(&cmd,
                "HSET myhash%s element:__rand_int__ %s",tag,data);
            benchmark("HSET",cmd,len);
            free(cmd);
        }

        if (test_is_selected("spop")) {
            len = redisFormatCommand(&cmd,"SPOP myset%s",tag);
            benchmark("SPOP",cmd,len);
            free(cmd);
        }

        if (test_is_selected("zadd")) {
            char *score = "0";
            if (config.randomkeys) score = "__rand_int__";
            len = redisFormatCommand(&cmd,
                "ZADD myzset%s %s element:__rand_int__",tag,score);
            benchmark("ZADD",cmd,len);
            free(cmd);
        }

        if (test_is_selected("zpopmin")) {
            len = redisFormatCommand(&cmd,"ZPOPMIN myzset%s",tag);
            benchmark("ZPOPMIN",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") ||
            test_is_selected("lrange_100") ||
            test_is_selected("lrange_300") ||
            test_is_selected("lrange_500") ||
            test_is_selected("lrange_600"))
        {
            len = redisFormatCommand(&cmd,"LPUSH mylist%s %s",tag,data);
            benchmark("LPUSH (needed to benchmark LRANGE)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_100")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist%s 0 99",tag);
            benchmark("LRANGE_100 (first 100 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_300")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist%s 0 299",tag);
            benchmark("LRANGE_300 (first 300 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_500")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist%s 0 499",tag);
            benchmark("LRANGE_500 (first 500 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_600")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist%s 0 599",tag);
            benchmark("LRANGE_600 (first 600 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("mset")) {
            const char *cmd_argv[21];
            cmd_argv[0] = "MSET";
            sds key_placeholder = sdscatprintf(sdsnew(""),"key%s:__rand_int__",tag);
            for (i = 1; i < 21; i += 2) {
                cmd_argv[i] = key_placeholder;
                cmd_argv[i+1] = data;
            }
            len = redisFormatCommandArgv(&cmd,21,cmd_argv,NULL);
            benchmark("MSET (10 keys)",cmd,len);
            free(cmd);
            sdsfree(key_placeholder);
        }

        if (test_is_selected("xadd")) {
            len = redisFormatCommand(&cmd,"XADD mystream%s * myfield %s", tag, data);
            benchmark("XADD",cmd,len);
            free(cmd); 
        }        

        if (!config.csv) printf("\n");
    } while(config.loop);

    zfree(data);
    freeCliConnInfo(config.conn_info);
    if (config.redis_config != NULL) freeRedisConfig(config.redis_config);

    return 0;
}
