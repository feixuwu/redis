/*
 * Copyright (c) 2024-Present, Redis Ltd.
 * All rights reserved.
 *
 * Redis Stream-Multiplexed Protocol (RSMP) - Header
 *
 * This module implements a multiplexed protocol layer that allows
 * multiple logical streams over a single TCP connection, similar to
 * QUIC/HTTP2 stream multiplexing.
 */

#ifndef __MUX_H
#define __MUX_H

#include "server.h"

/* ========================== Frame Format ==========================
 * +--------+--------+------------------+--------+--------+-----------+
 * | Magic  | Flags  |     Stream ID    |    Payload Len  | Payload   |
 * | 1 byte | 1 byte |     8 bytes      |    4 bytes      | N bytes   |
 * +--------+--------+------------------+--------+--------+-----------+
 *  Total Header: 14 bytes
 */

/* Frame header size */
#define MUX_FRAME_HEADER_SIZE 14

/* Magic byte for frame synchronization */
#define MUX_FRAME_MAGIC 0xAA

/* Frame types (lower 4 bits of flags) */
#define MUX_FRAME_DATA          0x01  /* Command request or response data */
#define MUX_FRAME_STREAM_OPEN   0x02  /* Open a new logical stream */
#define MUX_FRAME_STREAM_CLOSE  0x03  /* Close a logical stream */
#define MUX_FRAME_PING          0x04  /* Heartbeat ping */
#define MUX_FRAME_PONG          0x05  /* Heartbeat pong */
#define MUX_FRAME_GOAWAY        0x06  /* Graceful connection shutdown */
#define MUX_FRAME_BATCH         0x07  /* Batch frame (multiple commands) */
#define MUX_FRAME_WINDOW        0x08  /* Flow control window update */
#define MUX_FRAME_ERROR         0x09  /* Stream-level error */

/* Frame type mask */
#define MUX_FRAME_TYPE_MASK     0x0F

/* Control bits (upper 4 bits of flags) */
#define MUX_FLAG_FIN            0x10  /* Last data frame on this stream */
#define MUX_FLAG_PRIORITY       0x20  /* Frame carries priority info */
#define MUX_FLAG_COMPRESS       0x40  /* Payload is compressed (LZ4) */
#define MUX_FLAG_RESERVED       0x80  /* Reserved for future use */

/* Stream ID 0 is reserved for connection-level control frames */
#define MUX_STREAM_ID_CONTROL   0

/* Maximum payload size: 512MB */
#define MUX_MAX_PAYLOAD_SIZE    (512 * 1024 * 1024)

/* Default configuration values */
#define MUX_DEFAULT_MAX_STREAMS         1024
#define MUX_DEFAULT_CONN_WINDOW_SIZE    65536
#define MUX_DEFAULT_STREAM_WINDOW_SIZE  16384

/* Stream states */
typedef enum {
    MUX_STREAM_OPEN,
    MUX_STREAM_HALF_CLOSED,
    MUX_STREAM_CLOSED
} muxStreamState;

/* Forward declarations */
typedef struct muxConnection muxConnection;
typedef struct muxStream muxStream;

/* Per-stream state, maps to a virtual client */
struct muxStream {
    uint64_t stream_id;             /* Logical stream identifier */
    client *virtual_client;         /* Virtual client for this stream */
    muxConnection *mux_conn;        /* Back-pointer to parent mux connection */
    muxStreamState state;           /* Current stream state */
    int64_t stream_window_size;     /* Per-stream flow control window */
    int priority;                   /* Stream priority: 0=highest, 7=lowest */
};

/* Multiplexed connection wrapper - one per physical TCP connection in mux mode */
struct muxConnection {
    connection *conn;               /* Underlying TCP connection */
    client *owner_client;           /* The physical connection's "owner" client */
    dict *streams;                  /* Stream ID -> muxStream mapping */
    uint64_t next_server_stream_id; /* Next even stream ID for server push */
    uint64_t max_client_stream_id;  /* Highest client stream ID seen */
    uint32_t max_concurrent_streams;/* Configurable limit */
    uint32_t active_stream_count;   /* Current number of active streams */

    /* Frame parsing state machine */
    unsigned char frame_header[MUX_FRAME_HEADER_SIZE]; /* Header buffer */
    int header_bytes_read;          /* Bytes of header read so far */
    uint32_t current_payload_len;   /* Current frame's expected payload length */
    sds frame_buf;                  /* Frame payload reassembly buffer */
    int parsing_payload;            /* 1 if currently reading payload, 0 if reading header */
    uint8_t current_flags;          /* Current frame's flags */
    uint64_t current_stream_id;     /* Current frame's stream ID */

    /* Batch write buffer for response framing */
    sds write_batch_buf;            /* Accumulated framed responses for batch write */
    int pending_write_frames;       /* Number of frames in write_batch_buf */
    int needs_flush;                /* Whether this mux conn needs flushing */

    /* Flow control */
    int64_t conn_window_size;       /* Connection-level flow control window */

    /* Linked list node for server.mux_connections_with_pending_writes */
    listNode pending_write_node;
};

/* ========================== API Functions ========================== */

/* Initialization and cleanup */
void muxInit(void);

/* Connection management */
muxConnection *muxConnectionCreate(client *owner);
void muxConnectionFree(muxConnection *mux);

/* Stream management */
muxStream *muxStreamCreate(muxConnection *mux, uint64_t stream_id);
void muxStreamFree(muxStream *ms);
muxStream *muxStreamLookup(muxConnection *mux, uint64_t stream_id);

/* Frame processing (read path) */
int muxProcessInputBuffer(client *c);

/* Frame building (write path) */
void muxFrameReply(muxConnection *mux, muxStream *ms, client *vc);
void muxFlushPendingWrites(void);

/* Frame encoding/decoding helpers */
void muxEncodeFrameHeader(unsigned char *buf, uint8_t flags, uint64_t stream_id, uint32_t payload_len);
int muxDecodeFrameHeader(const unsigned char *buf, uint8_t *flags, uint64_t *stream_id, uint32_t *payload_len);

/* Control frames */
void muxSendPong(muxConnection *mux);
void muxSendGoaway(muxConnection *mux, uint64_t last_stream_id, uint32_t error_code);
void muxSendStreamError(muxConnection *mux, uint64_t stream_id, const char *errmsg);
void muxSendStreamClose(muxConnection *mux, uint64_t stream_id);

/* Put a mux connection into the pending write queue */
void muxPutInPendingWriteQueue(muxConnection *mux);

/* Client integration - called from networking.c */
int muxPrepareVirtualClientToWrite(client *c);

/* Virtual client lifecycle - close stream when virtual client is freed */
void muxCloseVirtualClientStream(client *vc);

#endif /* __MUX_H */
