/*
 * Copyright (c) 2024-Present, Redis Ltd.
 * All rights reserved.
 *
 * Hiredis MUX (Stream Multiplexing) support implementation.
 *
 * This module provides a transparent MUX layer that wraps/unwraps
 * MUX frames around RESP data. It follows the same pattern as ssl.c:
 * replacing redisContext->funcs with MUX-aware versions.
 */

#include "hiredis.h"
#include "async.h"
#include "net.h"
#include "hiredis_mux.h"
#include "async_private.h"

#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>

#ifndef _WIN32
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#endif

#include "sds.h"
#include "alloc.h"

/* Defined in hiredis.c */
void __redisSetError(redisContext *c, int type, const char *str);

/* Forward declaration */
redisContextFuncs redisContextMuxFuncs;

/* ========================== MUX Private Context ========================== */

typedef struct redisMux {
    uint64_t stream_id;         /* Our stream ID */

    /* Receive buffer: raw bytes from socket, MUX frames not yet fully received */
    char *recvbuf;
    size_t recvbuf_len;         /* Data length in recvbuf */
    size_t recvbuf_alloc;       /* Allocated size of recvbuf */

    /* Decoded payload buffer: RESP data extracted from MUX frames,
     * ready to be fed to redisReader */
    char *respbuf;
    size_t respbuf_len;
    size_t respbuf_alloc;

    /* Send framing buffer: holds MUX-framed data to be sent */
    char *sendbuf;
    size_t sendbuf_len;
    size_t sendbuf_alloc;
    size_t sendbuf_pos;         /* How much of sendbuf has been sent */
} redisMux;

/* ========================== Helper Functions ========================== */

/* Ensure buffer has room for `needed` more bytes */
static int muxBufGrow(char **buf, size_t *alloc, size_t len, size_t needed) {
    size_t required = len + needed;
    if (required <= *alloc) return 0;

    size_t newalloc = *alloc ? *alloc : 1024;
    while (newalloc < required) newalloc *= 2;

    char *newbuf = hi_realloc(*buf, newalloc);
    if (!newbuf) return -1;

    *buf = newbuf;
    *alloc = newalloc;
    return 0;
}

/* Encode a MUX frame header into buf (must be at least 10 bytes) */
static void muxEncodeHeader(unsigned char *buf, uint8_t flags,
                            uint64_t stream_id, uint32_t payload_len) {
    buf[0] = HIREDIS_MUX_FRAME_MAGIC;
    buf[1] = flags;
    buf[2] = (stream_id >> 56) & 0xFF;
    buf[3] = (stream_id >> 48) & 0xFF;
    buf[4] = (stream_id >> 40) & 0xFF;
    buf[5] = (stream_id >> 32) & 0xFF;
    buf[6] = (stream_id >> 24) & 0xFF;
    buf[7] = (stream_id >> 16) & 0xFF;
    buf[8] = (stream_id >> 8)  & 0xFF;
    buf[9] = stream_id & 0xFF;
    buf[10] = (payload_len >> 24) & 0xFF;
    buf[11] = (payload_len >> 16) & 0xFF;
    buf[12] = (payload_len >> 8)  & 0xFF;
    buf[13] = payload_len & 0xFF;
}

/* Decode a MUX frame header from buf. Returns 0 on success, -1 on error. */
static int muxDecodeHeader(const unsigned char *buf, uint8_t *flags,
                           uint64_t *stream_id, uint32_t *payload_len) {
    if (buf[0] != HIREDIS_MUX_FRAME_MAGIC) return -1;

    *flags = buf[1];
    *stream_id = ((uint64_t)buf[2] << 56) | ((uint64_t)buf[3] << 48) |
                 ((uint64_t)buf[4] << 40) | ((uint64_t)buf[5] << 32) |
                 ((uint64_t)buf[6] << 24) | ((uint64_t)buf[7] << 16) |
                 ((uint64_t)buf[8] << 8)  | (uint64_t)buf[9];
    *payload_len = ((uint32_t)buf[10] << 24) | ((uint32_t)buf[11] << 16) |
                   ((uint32_t)buf[12] << 8)  | (uint32_t)buf[13];
    return 0;
}

/* Parse complete MUX frames from recvbuf into respbuf.
 * Returns number of RESP bytes extracted, or -1 on error. */
static int muxParseFrames(redisMux *mux) {
    size_t pos = 0;
    int extracted = 0;

    while (pos + HIREDIS_MUX_FRAME_HEADER_SIZE <= mux->recvbuf_len) {
        uint8_t flags;
        uint64_t stream_id;
        uint32_t payload_len;

        if (muxDecodeHeader((unsigned char *)mux->recvbuf + pos,
                           &flags, &stream_id, &payload_len) != 0) {
            return -1; /* Bad magic / corrupt stream */
        }

        /* Check if we have the full frame */
        if (pos + HIREDIS_MUX_FRAME_HEADER_SIZE + payload_len > mux->recvbuf_len) {
            break; /* Incomplete frame, wait for more data */
        }

        uint8_t frame_type = flags & HIREDIS_MUX_FRAME_TYPE_MASK;

        if (frame_type == HIREDIS_MUX_FRAME_DATA) {
            /* Extract RESP payload into respbuf */
            if (payload_len > 0) {
                if (muxBufGrow(&mux->respbuf, &mux->respbuf_alloc,
                              mux->respbuf_len, payload_len) != 0) {
                    return -1;
                }
                memcpy(mux->respbuf + mux->respbuf_len,
                       mux->recvbuf + pos + HIREDIS_MUX_FRAME_HEADER_SIZE,
                       payload_len);
                mux->respbuf_len += payload_len;
                extracted += payload_len;
            }
        }
        /* Skip PING/PONG/GOAWAY/etc. silently for now */

        pos += HIREDIS_MUX_FRAME_HEADER_SIZE + payload_len;
    }

    /* Remove consumed data from recvbuf */
    if (pos > 0) {
        mux->recvbuf_len -= pos;
        if (mux->recvbuf_len > 0) {
            memmove(mux->recvbuf, mux->recvbuf + pos, mux->recvbuf_len);
        }
    }

    return extracted;
}

/* ========================== MUX funcs Implementation ========================== */

static void redisMuxFree(void *privctx) {
    redisMux *mux = privctx;
    if (!mux) return;

    hi_free(mux->recvbuf);
    hi_free(mux->respbuf);
    hi_free(mux->sendbuf);
    hi_free(mux);
}

/**
 * MUX read function.
 *
 * 1. Read raw bytes from socket into mux->recvbuf
 * 2. Parse MUX frames, extract RESP payload into mux->respbuf
 * 3. Copy available RESP data into caller's buf
 *
 * Returns number of RESP bytes placed in buf, 0 for EAGAIN, -1 for error.
 */
static ssize_t redisMuxRead(redisContext *c, char *buf, size_t bufcap) {
    redisMux *mux = c->privctx;

    /* If we have leftover RESP data from previous parse, serve it first */
    if (mux->respbuf_len > 0) {
        size_t tocopy = mux->respbuf_len < bufcap ? mux->respbuf_len : bufcap;
        memcpy(buf, mux->respbuf, tocopy);
        mux->respbuf_len -= tocopy;
        if (mux->respbuf_len > 0) {
            memmove(mux->respbuf, mux->respbuf + tocopy, mux->respbuf_len);
        }
        return (ssize_t)tocopy;
    }

    /* Read raw data from socket */
    char tmpbuf[1024 * 16];
    ssize_t nread = recv(c->fd, tmpbuf, sizeof(tmpbuf), 0);
    if (nread == -1) {
        if ((errno == EWOULDBLOCK && !(c->flags & REDIS_BLOCK)) || errno == EINTR) {
            return 0;
        } else if (errno == ETIMEDOUT && (c->flags & REDIS_BLOCK)) {
            __redisSetError(c, REDIS_ERR_TIMEOUT, "recv timeout");
            return -1;
        } else {
            __redisSetError(c, REDIS_ERR_IO, strerror(errno));
            return -1;
        }
    } else if (nread == 0) {
        __redisSetError(c, REDIS_ERR_EOF, "Server closed the connection");
        return -1;
    }

    /* Append to recvbuf */
    if (muxBufGrow(&mux->recvbuf, &mux->recvbuf_alloc,
                   mux->recvbuf_len, (size_t)nread) != 0) {
        __redisSetError(c, REDIS_ERR_OOM, "Out of memory");
        return -1;
    }
    memcpy(mux->recvbuf + mux->recvbuf_len, tmpbuf, nread);
    mux->recvbuf_len += nread;

    /* Parse MUX frames into RESP data */
    int parsed = muxParseFrames(mux);
    if (parsed < 0) {
        __redisSetError(c, REDIS_ERR_PROTOCOL, "MUX frame parse error");
        return -1;
    }

    /* Serve RESP data to caller */
    if (mux->respbuf_len > 0) {
        size_t tocopy = mux->respbuf_len < bufcap ? mux->respbuf_len : bufcap;
        memcpy(buf, mux->respbuf, tocopy);
        mux->respbuf_len -= tocopy;
        if (mux->respbuf_len > 0) {
            memmove(mux->respbuf, mux->respbuf + tocopy, mux->respbuf_len);
        }
        return (ssize_t)tocopy;
    }

    /* No complete frames yet, tell caller to retry */
    return 0;
}

/**
 * MUX write function.
 *
 * Takes RESP data from c->obuf, wraps it in a MUX DATA frame,
 * and sends it to the socket.
 *
 * Returns number of RESP bytes consumed from c->obuf.
 */
static ssize_t redisMuxWrite(redisContext *c) {
    redisMux *mux = c->privctx;

    /* If we have leftover framed data from a previous partial write, send it first */
    if (mux->sendbuf_len > mux->sendbuf_pos) {
        ssize_t nwritten = send(c->fd,
                                mux->sendbuf + mux->sendbuf_pos,
                                mux->sendbuf_len - mux->sendbuf_pos, 0);
        if (nwritten < 0) {
            if ((errno == EWOULDBLOCK && !(c->flags & REDIS_BLOCK)) || errno == EINTR) {
                return 0;
            }
            __redisSetError(c, REDIS_ERR_IO, strerror(errno));
            return -1;
        }
        mux->sendbuf_pos += nwritten;

        if (mux->sendbuf_pos < mux->sendbuf_len) {
            /* Still have framed data to send; report 0 bytes consumed from obuf
             * so redisBufferWrite won't trim obuf yet. But we need to return > 0
             * to indicate progress. Actually, we need to return the bytes
             * consumed from obuf. Since we're flushing old framed data, no new
             * obuf bytes were consumed. Return 0 to signal "try again". */
            return 0;
        }

        /* All old data sent; reset sendbuf */
        mux->sendbuf_len = 0;
        mux->sendbuf_pos = 0;
    }

    /* Now frame the current obuf content */
    size_t obuf_len = hi_sdslen(c->obuf);
    if (obuf_len == 0) return 0;

    /* Build MUX frame: header + payload */
    size_t frame_size = HIREDIS_MUX_FRAME_HEADER_SIZE + obuf_len;
    if (muxBufGrow(&mux->sendbuf, &mux->sendbuf_alloc, 0, frame_size) != 0) {
        __redisSetError(c, REDIS_ERR_OOM, "Out of memory");
        return -1;
    }

    muxEncodeHeader((unsigned char *)mux->sendbuf,
                    HIREDIS_MUX_FRAME_DATA, mux->stream_id, (uint32_t)obuf_len);
    memcpy(mux->sendbuf + HIREDIS_MUX_FRAME_HEADER_SIZE, c->obuf, obuf_len);
    mux->sendbuf_len = frame_size;
    mux->sendbuf_pos = 0;

    /* Try to send */
    ssize_t nwritten = send(c->fd, mux->sendbuf, mux->sendbuf_len, 0);
    if (nwritten < 0) {
        if ((errno == EWOULDBLOCK && !(c->flags & REDIS_BLOCK)) || errno == EINTR) {
            /* Framed data is in sendbuf, will be sent on next write event.
             * Return 0 to indicate no progress. */
            return 0;
        }
        __redisSetError(c, REDIS_ERR_IO, strerror(errno));
        return -1;
    }

    mux->sendbuf_pos = nwritten;

    if ((size_t)nwritten >= frame_size) {
        /* All framed data sent, report all obuf bytes consumed */
        mux->sendbuf_len = 0;
        mux->sendbuf_pos = 0;
        return (ssize_t)obuf_len;
    }

    /* Partial write of framed data. We need to report that all obuf bytes
     * are "consumed" (they've been framed into sendbuf), so that
     * redisBufferWrite clears obuf. The remaining framed bytes in sendbuf
     * will be flushed on the next write call. */
    return (ssize_t)obuf_len;
}

static void redisMuxAsyncRead(redisAsyncContext *ac) {
    redisContext *c = &ac->c;

    if (redisBufferRead(c) == REDIS_ERR) {
        __redisAsyncDisconnect(ac);
    } else {
        _EL_ADD_READ(ac);
        redisProcessCallbacks(ac);
    }
}

static void redisMuxAsyncWrite(redisAsyncContext *ac) {
    redisContext *c = &ac->c;
    int done = 0;

    if (redisBufferWrite(c, &done) == REDIS_ERR) {
        __redisAsyncDisconnect(ac);
    } else {
        if (!done)
            _EL_ADD_WRITE(ac);
        else
            _EL_DEL_WRITE(ac);

        _EL_ADD_READ(ac);
    }
}

redisContextFuncs redisContextMuxFuncs = {
    .close = redisNetClose,
    .free_privctx = redisMuxFree,
    .async_read = redisMuxAsyncRead,
    .async_write = redisMuxAsyncWrite,
    .read = redisMuxRead,
    .write = redisMuxWrite
};

/* ========================== Public API ========================== */

int redisEnableMux(redisContext *c, uint64_t stream_id) {
    if (!c) return REDIS_ERR;

    /* Must be a connected blocking context */
    if (!(c->flags & REDIS_CONNECTED)) {
        __redisSetError(c, REDIS_ERR_OTHER, "Not connected");
        return REDIS_ERR;
    }

    /* Don't enable twice */
    if (c->funcs == &redisContextMuxFuncs) {
        __redisSetError(c, REDIS_ERR_OTHER, "MUX already enabled");
        return REDIS_ERR;
    }

    if (stream_id == 0) stream_id = 1;

    /* Step 1: Send HELLO 3 MULTIPLEX and get reply */
    redisReply *reply = redisCommand(c, "HELLO 3 MULTIPLEX");
    if (!reply) {
        /* c->err is already set by redisCommand */
        return REDIS_ERR;
    }

    /* HELLO 3 returns a MAP reply in RESP3. Check it's not an error. */
    if (reply->type == REDIS_REPLY_ERROR) {
        __redisSetError(c, REDIS_ERR_OTHER, reply->str);
        freeReplyObject(reply);
        return REDIS_ERR;
    }
    freeReplyObject(reply);

    /* Step 2: Allocate MUX context */
    redisMux *mux = hi_calloc(1, sizeof(redisMux));
    if (!mux) {
        __redisSetError(c, REDIS_ERR_OOM, "Out of memory");
        return REDIS_ERR;
    }

    mux->stream_id = stream_id;

    /* Step 3: Switch to MUX funcs */
    c->privctx = mux;
    c->funcs = &redisContextMuxFuncs;

    return REDIS_OK;
}

/* Async MUX handshake callback wrapper */
typedef struct {
    redisCallbackFn *user_fn;
    void *user_privdata;
    uint64_t stream_id;
} redisMuxAsyncHandshake;

static void redisMuxAsyncHandshakeCb(redisAsyncContext *ac, void *r, void *privdata) {
    redisMuxAsyncHandshake *hs = privdata;
    redisReply *reply = r;
    redisContext *c = &ac->c;

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        /* Handshake failed, call user callback with the error reply */
        if (hs->user_fn) hs->user_fn(ac, r, hs->user_privdata);
        hi_free(hs);
        return;
    }

    /* Allocate MUX context */
    redisMux *mux = hi_calloc(1, sizeof(redisMux));
    if (!mux) {
        __redisSetError(c, REDIS_ERR_OOM, "Out of memory");
        if (hs->user_fn) hs->user_fn(ac, NULL, hs->user_privdata);
        hi_free(hs);
        return;
    }

    mux->stream_id = hs->stream_id;

    /* Switch to MUX funcs */
    c->privctx = mux;
    c->funcs = &redisContextMuxFuncs;

    /* Call user callback */
    if (hs->user_fn) hs->user_fn(ac, r, hs->user_privdata);
    hi_free(hs);
}

int redisAsyncEnableMux(redisAsyncContext *ac, uint64_t stream_id,
                        redisCallbackFn *fn, void *privdata) {
    if (!ac) return REDIS_ERR;

    if (stream_id == 0) stream_id = 1;

    redisMuxAsyncHandshake *hs = hi_malloc(sizeof(*hs));
    if (!hs) {
        __redisSetError(&ac->c, REDIS_ERR_OOM, "Out of memory");
        return REDIS_ERR;
    }

    hs->user_fn = fn;
    hs->user_privdata = privdata;
    hs->stream_id = stream_id;

    return redisAsyncCommand(ac, redisMuxAsyncHandshakeCb, hs,
                             "HELLO 3 MULTIPLEX");
}

int redisActivateMux(redisContext *c, uint64_t stream_id) {
    if (!c) return REDIS_ERR;

    /* Don't enable twice */
    if (c->funcs == &redisContextMuxFuncs) {
        __redisSetError(c, REDIS_ERR_OTHER, "MUX already enabled");
        return REDIS_ERR;
    }

    if (stream_id == 0) stream_id = 1;

    /* Allocate MUX context */
    redisMux *mux = hi_calloc(1, sizeof(redisMux));
    if (!mux) {
        __redisSetError(c, REDIS_ERR_OOM, "Out of memory");
        return REDIS_ERR;
    }

    mux->stream_id = stream_id;

    /* Drain any leftover data from hiredis reader into MUX recvbuf.
     * After the HELLO reply, the server starts sending MUX frames immediately.
     * Some of those bytes may have been read into the hiredis reader buffer
     * along with the HELLO reply by redisBufferRead(). */
    redisReader *reader = c->reader;
    size_t leftover = reader->len - reader->pos;
    if (leftover > 0) {
        mux->recvbuf = hi_malloc(leftover);
        if (mux->recvbuf) {
            memcpy(mux->recvbuf, reader->buf + reader->pos, leftover);
            mux->recvbuf_len = leftover;
            mux->recvbuf_alloc = leftover;
        }
        reader->pos = reader->len; /* Mark hiredis buffer as consumed */
    }

    /* Switch to MUX funcs */
    c->privctx = mux;
    c->funcs = &redisContextMuxFuncs;

    return REDIS_OK;
}

uint64_t redisGetMuxStreamId(redisContext *c) {
    if (!c || c->funcs != &redisContextMuxFuncs || !c->privctx) return 0;
    return ((redisMux *)c->privctx)->stream_id;
}

int redisIsMuxEnabled(redisContext *c) {
    return (c && c->funcs == &redisContextMuxFuncs) ? 1 : 0;
}
