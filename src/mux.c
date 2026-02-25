/*
 * Copyright (c) 2024-Present, Redis Ltd.
 * All rights reserved.
 *
 * Redis Stream-Multiplexed Protocol (RSMP) - Implementation
 *
 * This module implements a multiplexed protocol layer that allows
 * multiple logical streams over a single TCP connection.
 */

#include "mux.h"
#include "server.h"
#include "atomicvar.h"
#include <string.h>
#include <arpa/inet.h>

/* ========================== Dict Type for Streams ========================== */

/* Dict type for stream_id (uint64_t) -> muxStream* mapping.
 * We store stream_id as the key using a simple cast to void*. */
static uint64_t muxStreamDictHashFunction(const void *key) {
    uint64_t k = (uint64_t)(uintptr_t)key;
    return dictGenHashFunction(&k, sizeof(uint64_t));
}

static int muxStreamDictKeyCompare(dict *d, const void *key1, const void *key2) {
    UNUSED(d);
    uint64_t k1 = (uint64_t)(uintptr_t)key1;
    uint64_t k2 = (uint64_t)(uintptr_t)key2;
    /* Redis dict convention: return 0 for NOT equal, non-zero for equal. */
    return k1 == k2;
}

static dictType muxStreamDictType = {
    muxStreamDictHashFunction,  /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    muxStreamDictKeyCompare,    /* key compare */
    NULL,                       /* key destructor */
    NULL                        /* val destructor - we handle this manually */
};

/* ========================== Frame Encoding/Decoding ========================== */

/* Encode a frame header into buf (must be at least MUX_FRAME_HEADER_SIZE bytes) */
void muxEncodeFrameHeader(unsigned char *buf, uint8_t flags, uint64_t stream_id, uint32_t payload_len) {
    buf[0] = MUX_FRAME_MAGIC;
    buf[1] = flags;
    /* Stream ID in big-endian (8 bytes) */
    buf[2] = (stream_id >> 56) & 0xFF;
    buf[3] = (stream_id >> 48) & 0xFF;
    buf[4] = (stream_id >> 40) & 0xFF;
    buf[5] = (stream_id >> 32) & 0xFF;
    buf[6] = (stream_id >> 24) & 0xFF;
    buf[7] = (stream_id >> 16) & 0xFF;
    buf[8] = (stream_id >> 8) & 0xFF;
    buf[9] = stream_id & 0xFF;
    /* Payload length in big-endian */
    buf[10] = (payload_len >> 24) & 0xFF;
    buf[11] = (payload_len >> 16) & 0xFF;
    buf[12] = (payload_len >> 8) & 0xFF;
    buf[13] = payload_len & 0xFF;
}

/* Decode a frame header from buf. Returns C_OK on success, C_ERR if magic mismatch. */
int muxDecodeFrameHeader(const unsigned char *buf, uint8_t *flags, uint64_t *stream_id, uint32_t *payload_len) {
    if (buf[0] != MUX_FRAME_MAGIC) return C_ERR;

    *flags = buf[1];
    *stream_id = ((uint64_t)buf[2] << 56) |
                 ((uint64_t)buf[3] << 48) |
                 ((uint64_t)buf[4] << 40) |
                 ((uint64_t)buf[5] << 32) |
                 ((uint64_t)buf[6] << 24) |
                 ((uint64_t)buf[7] << 16) |
                 ((uint64_t)buf[8] << 8)  |
                 ((uint64_t)buf[9]);
    *payload_len = ((uint32_t)buf[10] << 24) |
                   ((uint32_t)buf[11] << 16) |
                   ((uint32_t)buf[12] << 8)  |
                   ((uint32_t)buf[13]);
    return C_OK;
}

/* ========================== Initialization ========================== */

void muxInit(void) {
    server.mux_connections_with_pending_writes = listCreate();
    server.mux_virtual_client_count = 0;
}

/* ========================== Connection Management ========================== */

muxConnection *muxConnectionCreate(client *owner) {
    muxConnection *mux = zmalloc(sizeof(muxConnection));

    mux->conn = owner->conn;
    mux->owner_client = owner;
    mux->streams = dictCreate(&muxStreamDictType);
    mux->next_server_stream_id = 2; /* Server uses even IDs */
    mux->max_client_stream_id = 0;
    mux->max_concurrent_streams = MUX_DEFAULT_MAX_STREAMS;
    mux->active_stream_count = 0;

    /* Frame parsing state */
    memset(mux->frame_header, 0, MUX_FRAME_HEADER_SIZE);
    mux->header_bytes_read = 0;
    mux->current_payload_len = 0;
    mux->frame_buf = sdsempty();
    mux->parsing_payload = 0;
    mux->current_flags = 0;
    mux->current_stream_id = 0;

    /* Write batch buffer */
    mux->write_batch_buf = sdsempty();
    mux->pending_write_frames = 0;
    mux->needs_flush = 0;

    /* Flow control */
    mux->conn_window_size = MUX_DEFAULT_CONN_WINDOW_SIZE;

    /* Initialize list node */
    listInitNode(&mux->pending_write_node, mux);

    return mux;
}

void muxConnectionFree(muxConnection *mux) {
    if (!mux) return;

    /* Free all streams and their virtual clients.
     * We iterate the dict using dictGetSafeIterator to allow modifications
     * during iteration (freeClient may trigger callbacks). We collect and
     * free streams in a safe order:
     *   1. Detach the virtual client from the stream (clear back-pointers)
     *   2. Free the virtual client
     *   3. Free the stream struct */
    dictIterator *di = dictGetSafeIterator(mux->streams);
    dictEntry *de;
    while ((de = dictNext(di))) {
        muxStream *ms = dictGetVal(de);
        if (ms->virtual_client) {
            /* Clear back-pointers BEFORE freeing to prevent
             * freeClient from re-entering mux cleanup code. */
            client *vc = ms->virtual_client;
            ms->virtual_client = NULL;
            vc->mux_data = NULL;
            vc->flags &= ~CLIENT_MUX_VIRTUAL;
            server.mux_virtual_client_count--;
            freeClient(vc);
        }
        /* Don't call muxStreamFree() here as it would dictDelete()
         * while we are iterating. Just free the struct. */
        zfree(ms);
    }
    dictReleaseIterator(di);
    mux->active_stream_count = 0;
    dictRelease(mux->streams);

    /* Remove from pending write queue if needed */
    if (mux->needs_flush) {
        listUnlinkNode(server.mux_connections_with_pending_writes,
                        &mux->pending_write_node);
    }

    sdsfree(mux->frame_buf);
    sdsfree(mux->write_batch_buf);
    zfree(mux);
}

/* ========================== Stream Management ========================== */

/* Create a virtual client for a mux stream.
 * The virtual client has conn=NULL and is marked with CLIENT_MUX_VIRTUAL. */
static client *muxCreateVirtualClient(muxStream *ms) {
    /* Create a client with no connection (conn=NULL) */
    client *vc = createClient(NULL);
    if (!vc) return NULL;

    /* Mark it as a mux virtual client */
    vc->flags |= CLIENT_MUX_VIRTUAL;
    vc->mux_data = ms;

    /* Inherit some properties from the owner client */
    client *owner = ms->mux_conn->owner_client;
    if (owner->user) vc->user = owner->user;
    vc->authenticated = owner->authenticated;

    /* Select the same DB as the owner */
    selectDb(vc, owner->db->id);

    /* Link the virtual client to server.clients and clients_index.
     * This ensures that features like CLIENT TRACKING, CLIENT LIST,
     * CLIENT KILL, lookupClientByID(), etc. work correctly for virtual clients.
     * clientsCron() will skip virtual clients via the CLIENT_MUX_VIRTUAL flag. */
    linkClient(vc);
    server.mux_virtual_client_count++;

    return vc;
}

muxStream *muxStreamCreate(muxConnection *mux, uint64_t stream_id) {
    /* Check concurrent stream limit */
    if (mux->active_stream_count >= mux->max_concurrent_streams) {
        serverLog(LL_WARNING, "MUX: max concurrent streams reached (%u) for client %llu",
                  mux->max_concurrent_streams, (unsigned long long)mux->owner_client->id);
        return NULL;
    }

    /* Reject duplicate stream IDs. For client-initiated (odd) streams,
     * IDs must be strictly increasing. If we see a stream_id that already
     * exists in the dict, or is <= max_client_stream_id and is odd, it
     * means the ID was reused (possibly after uint32 wraparound). */
    if (stream_id % 2 == 1 && stream_id <= mux->max_client_stream_id) {
        serverLog(LL_WARNING, "MUX: stream ID %llu is not greater than max seen %llu "
                  "(possible ID reuse/wraparound) for client %llu",
                  (unsigned long long)stream_id, (unsigned long long)mux->max_client_stream_id,
                  (unsigned long long)mux->owner_client->id);
        return NULL;
    }

    muxStream *ms = zmalloc(sizeof(muxStream));
    ms->stream_id = stream_id;
    ms->mux_conn = mux;
    ms->state = MUX_STREAM_OPEN;
    ms->stream_window_size = MUX_DEFAULT_STREAM_WINDOW_SIZE;
    ms->priority = 4; /* Default middle priority */

    /* Create the virtual client */
    ms->virtual_client = muxCreateVirtualClient(ms);
    if (!ms->virtual_client) {
        zfree(ms);
        return NULL;
    }

    /* Add to the streams dict */
    dictAdd(mux->streams, (void *)(uintptr_t)stream_id, ms);
    mux->active_stream_count++;

    /* Track the max client stream ID */
    if (stream_id > mux->max_client_stream_id) {
        mux->max_client_stream_id = stream_id;
    }

    return ms;
}

void muxStreamFree(muxStream *ms) {
    if (!ms) return;

    muxConnection *mux = ms->mux_conn;
    if (mux) {
        dictDelete(mux->streams, (void *)(uintptr_t)ms->stream_id);
        if (mux->active_stream_count > 0) mux->active_stream_count--;
    }

    zfree(ms);
}

muxStream *muxStreamLookup(muxConnection *mux, uint64_t stream_id) {
    dictEntry *de = dictFind(mux->streams, (void *)(uintptr_t)stream_id);
    return de ? dictGetVal(de) : NULL;
}

/* ========================== Read Path: Frame Processing ========================== */

/* Process a single complete frame that has been fully received. */
static int muxProcessFrame(muxConnection *mux, uint8_t flags, uint64_t stream_id,
                           const char *payload, uint32_t payload_len)
{
    uint8_t frame_type = flags & MUX_FRAME_TYPE_MASK;

    switch (frame_type) {
    case MUX_FRAME_PING:
        /* Respond with PONG on stream 0 */
        muxSendPong(mux);
        return C_OK;

    case MUX_FRAME_PONG:
        /* Received a pong, nothing to do */
        return C_OK;

    case MUX_FRAME_GOAWAY:
        /* Client wants to gracefully close the connection */
        serverLog(LL_NOTICE, "MUX: received GOAWAY from client %llu",
                  (unsigned long long)mux->owner_client->id);
        mux->owner_client->flags |= CLIENT_CLOSE_AFTER_REPLY;
        return C_OK;

    case MUX_FRAME_STREAM_CLOSE: {
        muxStream *ms = muxStreamLookup(mux, stream_id);
        if (ms) {
            ms->state = MUX_STREAM_CLOSED;
            /* Free the virtual client and stream */
            if (ms->virtual_client) {
                ms->virtual_client->mux_data = NULL;
                ms->virtual_client->flags &= ~CLIENT_MUX_VIRTUAL;
                server.mux_virtual_client_count--;
                freeClient(ms->virtual_client);
                ms->virtual_client = NULL;
            }
            muxStreamFree(ms);
        }
        return C_OK;
    }

    case MUX_FRAME_DATA: {
        /* Look up or create the stream */
        muxStream *ms = muxStreamLookup(mux, stream_id);
        if (!ms) {
            /* Auto-create stream for client-initiated streams (odd IDs) */
            if (stream_id % 2 == 1) {
                ms = muxStreamCreate(mux, stream_id);
                if (!ms) {
                    muxSendStreamError(mux, stream_id, "ERR max streams exceeded or invalid stream ID");
                    return C_OK; /* Don't kill the whole connection */
                }
            } else {
                /* Even IDs are server-initiated, client should not create them */
                muxSendStreamError(mux, stream_id, "ERR invalid stream ID");
                return C_OK;
            }
        }

        if (ms->state == MUX_STREAM_CLOSED) {
            muxSendStreamError(mux, stream_id, "ERR stream is closed");
            return C_OK;
        }

        /* Append the RESP payload to the virtual client's query buffer */
        client *vc = ms->virtual_client;
        if (!vc) {
            /* Virtual client already freed (should not happen, but be safe) */
            muxSendStreamError(mux, stream_id, "ERR stream has no client");
            muxStreamFree(ms);
            return C_OK;
        }
        vc->querybuf = sdscatlen(vc->querybuf, payload, payload_len);
        vc->lastinteraction = server.unixtime;

        /* Process the virtual client's input buffer (standard RESP parsing).
         * NOTE: processInputBuffer() may call freeClient(vc) internally on
         * protocol error, which sets ms->virtual_client->mux_data = NULL
         * via the freeClient() mux cleanup code. After that, vc is a dangling
         * pointer, so we must detect this and clean up the stream. */
        if (processInputBuffer(vc) == C_ERR) {
            /* Virtual client had a protocol error and was freed.
             * The freeClient path already cleared vc->mux_data.
             * We need to clean up the stream (without double-freeing vc). */
            ms->virtual_client = NULL;
            muxStreamFree(ms);
            return C_OK; /* Don't kill the mux connection */
        }

        /* If FIN flag is set, close the stream after processing */
        if (flags & MUX_FLAG_FIN) {
            ms->state = MUX_STREAM_HALF_CLOSED;
        }
        return C_OK;
    }

    case MUX_FRAME_BATCH: {
        /* A batch frame contains multiple sub-frames concatenated.
         * Parse them sequentially. */
        uint32_t offset = 0;
        while (offset + MUX_FRAME_HEADER_SIZE <= payload_len) {
            uint8_t sub_flags;
            uint64_t sub_stream_id;
            uint32_t sub_payload_len;

            if (muxDecodeFrameHeader((const unsigned char *)payload + offset,
                                     &sub_flags, &sub_stream_id, &sub_payload_len) == C_ERR) {
                serverLog(LL_WARNING, "MUX: invalid sub-frame in batch");
                break;
            }
            offset += MUX_FRAME_HEADER_SIZE;

            if (offset + sub_payload_len > payload_len) {
                serverLog(LL_WARNING, "MUX: truncated sub-frame in batch");
                break;
            }

            /* Recursively process the sub-frame */
            muxProcessFrame(mux, sub_flags, sub_stream_id,
                           payload + offset, sub_payload_len);
            offset += sub_payload_len;
        }
        return C_OK;
    }

    case MUX_FRAME_WINDOW: {
        /* Flow control window update - for future use */
        return C_OK;
    }

    case MUX_FRAME_ERROR: {
        /* Stream-level error from client - log and close stream */
        serverLog(LL_WARNING, "MUX: received error frame on stream %u from client %llu",
                  stream_id, (unsigned long long)mux->owner_client->id);
        muxStream *ms = muxStreamLookup(mux, stream_id);
        if (ms) {
            ms->state = MUX_STREAM_CLOSED;
            if (ms->virtual_client) {
                ms->virtual_client->mux_data = NULL;
                ms->virtual_client->flags &= ~CLIENT_MUX_VIRTUAL;
                server.mux_virtual_client_count--;
                freeClient(ms->virtual_client);
                ms->virtual_client = NULL;
            }
            muxStreamFree(ms);
        }
        return C_OK;
    }

    default:
        serverLog(LL_WARNING, "MUX: unknown frame type 0x%02x", frame_type);
        return C_OK;
    }
}

/* Main read-path entry point.
 * Called from readQueryFromClient() when the owner client is in mux mode.
 * Parses frames from owner_client->querybuf and dispatches to virtual clients. */
int muxProcessInputBuffer(client *c) {
    muxConnection *mux = (muxConnection *)c->mux_data;

    while (sdslen(c->querybuf) - c->qb_pos > 0) {
        size_t avail = sdslen(c->querybuf) - c->qb_pos;

        if (!mux->parsing_payload) {
            /* Reading frame header */
            int needed = MUX_FRAME_HEADER_SIZE - mux->header_bytes_read;
            int tocopy = (int)avail < needed ? (int)avail : needed;

            memcpy(mux->frame_header + mux->header_bytes_read,
                   c->querybuf + c->qb_pos, tocopy);
            mux->header_bytes_read += tocopy;
            c->qb_pos += tocopy;

            if (mux->header_bytes_read < MUX_FRAME_HEADER_SIZE) {
                break; /* Need more data for header */
            }

            /* Full header received, decode it */
            if (muxDecodeFrameHeader(mux->frame_header, &mux->current_flags,
                                     &mux->current_stream_id,
                                     &mux->current_payload_len) == C_ERR) {
                serverLog(LL_WARNING, "MUX: invalid frame magic from client %llu",
                          (unsigned long long)c->id);
                return C_ERR; /* Protocol error, kill the connection */
            }

            /* Validate payload length */
            if (mux->current_payload_len > MUX_MAX_PAYLOAD_SIZE) {
                serverLog(LL_WARNING, "MUX: payload too large (%u) from client %llu",
                          mux->current_payload_len, (unsigned long long)c->id);
                return C_ERR;
            }

            /* Reset header state for next frame */
            mux->header_bytes_read = 0;

            if (mux->current_payload_len == 0) {
                /* Zero-length payload frame (e.g., PING/PONG/STREAM_CLOSE) */
                muxProcessFrame(mux, mux->current_flags,
                               mux->current_stream_id, NULL, 0);
                continue;
            }

            /* Prepare to read payload */
            sdsclear(mux->frame_buf);
            mux->parsing_payload = 1;
        }

        /* Reading frame payload */
        avail = sdslen(c->querybuf) - c->qb_pos;
        uint32_t remaining = mux->current_payload_len - (uint32_t)sdslen(mux->frame_buf);
        uint32_t tocopy = avail < remaining ? (uint32_t)avail : remaining;

        mux->frame_buf = sdscatlen(mux->frame_buf, c->querybuf + c->qb_pos, tocopy);
        c->qb_pos += tocopy;

        if (sdslen(mux->frame_buf) < mux->current_payload_len) {
            break; /* Need more data for payload */
        }

        /* Full frame received, process it */
        mux->parsing_payload = 0;
        muxProcessFrame(mux, mux->current_flags, mux->current_stream_id,
                        mux->frame_buf, mux->current_payload_len);
    }

    /* Trim consumed data from querybuf */
    if (c->qb_pos > 0) {
        sdsrange(c->querybuf, c->qb_pos, -1);
        c->qb_pos = 0;
    }

    return C_OK;
}

/* ========================== Write Path: Frame Building ========================== */

/* Put a mux connection into the pending write queue */
void muxPutInPendingWriteQueue(muxConnection *mux) {
    if (!mux->needs_flush) {
        mux->needs_flush = 1;
        listLinkNodeTail(server.mux_connections_with_pending_writes,
                         &mux->pending_write_node);
    }
}

/* Called from prepareClientToWrite() for virtual mux clients.
 * Instead of scheduling the virtual client for direct socket write,
 * we schedule the mux connection's owner client. */
int muxPrepareVirtualClientToWrite(client *c) {
    muxStream *ms = (muxStream *)c->mux_data;
    if (!ms || !ms->mux_conn) return C_ERR;

    /* Mark the mux connection as needing flush */
    muxPutInPendingWriteQueue(ms->mux_conn);
    return C_OK;
}

/* Frame the pending reply data of a virtual client and append
 * to the mux connection's write batch buffer.
 *
 * Optimization: merge ALL pending reply data (buf + reply list) into a
 * SINGLE MUX DATA frame to minimize per-frame header overhead.  For small
 * replies such as +OK\r\n (5 bytes), this reduces the overhead from
 * 10-byte-header-per-reply-block to one 10-byte header for the whole batch. */
void muxFrameReply(muxConnection *mux, muxStream *ms, client *vc) {
    unsigned char header[MUX_FRAME_HEADER_SIZE];

    /* Calculate total payload size across buf + reply list */
    uint32_t total_payload = (uint32_t)vc->bufpos;
    listIter li;
    listNode *ln;
    listRewind(vc->reply, &li);
    while ((ln = listNext(&li))) {
        clientReplyBlock *block = listNodeValue(ln);
        total_payload += (uint32_t)block->used;
    }

    if (total_payload == 0) return;

    /* Write a single frame header for all the data */
    muxEncodeFrameHeader(header, MUX_FRAME_DATA, ms->stream_id, total_payload);
    mux->write_batch_buf = sdscatlen(mux->write_batch_buf, header, MUX_FRAME_HEADER_SIZE);

    /* Append the fixed reply buffer */
    if (vc->bufpos > 0) {
        mux->write_batch_buf = sdscatlen(mux->write_batch_buf, vc->buf, vc->bufpos);
        vc->bufpos = 0;
    }

    /* Append the reply linked list blocks */
    while (listLength(vc->reply) > 0) {
        ln = listFirst(vc->reply);
        clientReplyBlock *block = listNodeValue(ln);
        size_t block_used = block->used;

        if (block_used > 0) {
            mux->write_batch_buf = sdscatlen(mux->write_batch_buf, block->buf, block_used);
        }

        vc->reply_bytes -= block_used;
        listDelNode(vc->reply, ln);
    }
    mux->pending_write_frames++;
}

/* Write handler callback for partial mux writes.
 * Installed when connWrite() doesn't write all data in one go. */
static void muxWriteHandler(connection *conn) {
    client *owner = connGetPrivateData(conn);
    if (!owner || !owner->mux_data) return;
    muxConnection *mux = (muxConnection *)owner->mux_data;

    if (sdslen(mux->write_batch_buf) == 0) {
        connSetWriteHandler(conn, NULL);
        return;
    }

    ssize_t nwritten = connWrite(conn, mux->write_batch_buf,
                                 sdslen(mux->write_batch_buf));
    if (nwritten <= 0) {
        if (connGetState(conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_VERBOSE, "MUX: error writing to client %llu: %s",
                      (unsigned long long)owner->id, connGetLastError(conn));
            freeClientAsync(owner);
        }
        return;
    }

    if ((size_t)nwritten < sdslen(mux->write_batch_buf)) {
        sdsrange(mux->write_batch_buf, nwritten, -1);
    } else {
        sdsclear(mux->write_batch_buf);
        connSetWriteHandler(conn, NULL);
    }
}

/* Flush all pending mux writes. Called from beforeSleep().
 * This iterates all mux connections that have pending virtual client replies,
 * frames them, and writes them out in a single batch per connection. */
void muxFlushPendingWrites(void) {
    listIter li;
    listNode *ln;

    listRewind(server.mux_connections_with_pending_writes, &li);
    while ((ln = listNext(&li))) {
        muxConnection *mux = listNodeValue(ln);
        mux->needs_flush = 0;
        listUnlinkNode(server.mux_connections_with_pending_writes, ln);

        /* Iterate all streams and frame their pending replies.
         * Use safe iterator in case stream state changes during iteration. */
        dictIterator *di = dictGetSafeIterator(mux->streams);
        dictEntry *de;
        while ((de = dictNext(di))) {
            muxStream *ms = dictGetVal(de);
            client *vc = ms->virtual_client;
            if (vc && clientHasPendingReplies(vc)) {
                muxFrameReply(mux, ms, vc);
            }
        }
        dictReleaseIterator(di);

        /* Write the batch buffer to the physical connection */
        if (sdslen(mux->write_batch_buf) > 0) {
            ssize_t nwritten = connWrite(mux->conn, mux->write_batch_buf,
                                         sdslen(mux->write_batch_buf));
            if (nwritten <= 0) {
                if (connGetState(mux->conn) != CONN_STATE_CONNECTED) {
                    serverLog(LL_VERBOSE, "MUX: error writing to client %llu: %s",
                              (unsigned long long)mux->owner_client->id,
                              connGetLastError(mux->conn));
                    freeClientAsync(mux->owner_client);
                } else {
                    /* EAGAIN: socket buffer full, install write handler to retry */
                    if (connSetWriteHandler(mux->conn, muxWriteHandler) == C_ERR) {
                        freeClientAsync(mux->owner_client);
                    }
                }
            } else if ((size_t)nwritten < sdslen(mux->write_batch_buf)) {
                /* Partial write: keep remaining data */
                sdsrange(mux->write_batch_buf, nwritten, -1);

                /* Install write handler for remaining data */
                if (connSetWriteHandler(mux->conn, muxWriteHandler) == C_ERR) {
                    freeClientAsync(mux->owner_client);
                }
            } else {
                /* All data written successfully */
                sdsclear(mux->write_batch_buf);
            }
            mux->pending_write_frames = 0;
        }
    }
}

/* ========================== Control Frames ========================== */

void muxSendPong(muxConnection *mux) {
    unsigned char header[MUX_FRAME_HEADER_SIZE];
    muxEncodeFrameHeader(header, MUX_FRAME_PONG, MUX_STREAM_ID_CONTROL, 0);
    mux->write_batch_buf = sdscatlen(mux->write_batch_buf, header, MUX_FRAME_HEADER_SIZE);
    muxPutInPendingWriteQueue(mux);
}

void muxSendGoaway(muxConnection *mux, uint64_t last_stream_id, uint32_t error_code) {
    unsigned char header[MUX_FRAME_HEADER_SIZE];
    unsigned char payload[12];

    /* Payload: last_stream_id (8 bytes) + error_code (4 bytes) */
    payload[0] = (last_stream_id >> 56) & 0xFF;
    payload[1] = (last_stream_id >> 48) & 0xFF;
    payload[2] = (last_stream_id >> 40) & 0xFF;
    payload[3] = (last_stream_id >> 32) & 0xFF;
    payload[4] = (last_stream_id >> 24) & 0xFF;
    payload[5] = (last_stream_id >> 16) & 0xFF;
    payload[6] = (last_stream_id >> 8) & 0xFF;
    payload[7] = last_stream_id & 0xFF;
    payload[8] = (error_code >> 24) & 0xFF;
    payload[9] = (error_code >> 16) & 0xFF;
    payload[10] = (error_code >> 8) & 0xFF;
    payload[11] = error_code & 0xFF;

    muxEncodeFrameHeader(header, MUX_FRAME_GOAWAY, MUX_STREAM_ID_CONTROL, 12);
    mux->write_batch_buf = sdscatlen(mux->write_batch_buf, header, MUX_FRAME_HEADER_SIZE);
    mux->write_batch_buf = sdscatlen(mux->write_batch_buf, payload, 12);
    muxPutInPendingWriteQueue(mux);
}

void muxSendStreamError(muxConnection *mux, uint64_t stream_id, const char *errmsg) {
    unsigned char header[MUX_FRAME_HEADER_SIZE];
    size_t msglen = strlen(errmsg);

    muxEncodeFrameHeader(header, MUX_FRAME_ERROR, stream_id, (uint32_t)msglen);
    mux->write_batch_buf = sdscatlen(mux->write_batch_buf, header, MUX_FRAME_HEADER_SIZE);
    mux->write_batch_buf = sdscatlen(mux->write_batch_buf, errmsg, msglen);
    muxPutInPendingWriteQueue(mux);
}
