/*
 * Copyright (c) 2024-Present, Redis Ltd.
 * All rights reserved.
 *
 * Hiredis MUX (Stream Multiplexing) support header.
 *
 * This module implements a transparent MUX layer for hiredis that allows
 * multiple logical streams over a single TCP connection, similar to
 * QUIC/HTTP2 stream multiplexing. It works by intercepting the read/write
 * funcs in redisContext, wrapping outgoing RESP data in MUX frames and
 * unwrapping incoming MUX frames back to RESP data.
 */

#ifndef __HIREDIS_MUX_H
#define __HIREDIS_MUX_H

#include "hiredis.h"
#include "async.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ========================== MUX Frame Format ==========================
 * +--------+--------+------------------+--------+--------+-----------+
 * | Magic  | Flags  |     Stream ID    |    Payload Len  | Payload   |
 * | 1 byte | 1 byte |     8 bytes      |    4 bytes      | N bytes   |
 * +--------+--------+------------------+--------+--------+-----------+
 *  Total Header: 14 bytes
 */

/* Frame header size */
#define HIREDIS_MUX_FRAME_HEADER_SIZE 14

/* Magic byte for frame synchronization */
#define HIREDIS_MUX_FRAME_MAGIC 0xAA

/* Frame types (lower 4 bits of flags) */
#define HIREDIS_MUX_FRAME_DATA          0x01
#define HIREDIS_MUX_FRAME_STREAM_OPEN   0x02
#define HIREDIS_MUX_FRAME_STREAM_CLOSE  0x03
#define HIREDIS_MUX_FRAME_PING          0x04
#define HIREDIS_MUX_FRAME_PONG          0x05
#define HIREDIS_MUX_FRAME_GOAWAY        0x06

/* Frame type mask */
#define HIREDIS_MUX_FRAME_TYPE_MASK     0x0F

/* Maximum payload size: 512MB */
#define HIREDIS_MUX_MAX_PAYLOAD_SIZE    (512 * 1024 * 1024)

/**
 * Enable MUX mode on a blocking redisContext.
 *
 * This function performs the HELLO 3 MULTIPLEX handshake with the server,
 * then replaces c->funcs with MUX-aware read/write functions.
 *
 * After this call, all subsequent redisCommand / redisAppendCommand / etc.
 * will transparently wrap data in MUX frames (write path) and unwrap
 * MUX frames on the read path.
 *
 * @param c         A connected, blocking redisContext.
 * @param stream_id The stream ID to use for this context (must be odd for
 *                  client-initiated streams). If 0, defaults to 1.
 * @return REDIS_OK on success, REDIS_ERR on failure (check c->err/c->errstr).
 */
int redisEnableMux(redisContext *c, uint64_t stream_id);

/**
 * Enable MUX mode on an async redisAsyncContext.
 *
 * This sends HELLO 3 MULTIPLEX as an async command. When the reply arrives,
 * the callback switches funcs to MUX mode. The user-provided callback
 * is called after MUX is activated (or on error).
 *
 * @param ac        A connected redisAsyncContext.
 * @param stream_id The stream ID to use. If 0, defaults to 1.
 * @param fn        User callback invoked after MUX handshake completes.
 * @param privdata  User privdata passed to fn.
 * @return REDIS_OK on success, REDIS_ERR on failure.
 */
int redisAsyncEnableMux(redisAsyncContext *ac, uint64_t stream_id,
                        redisCallbackFn *fn, void *privdata);

/**
 * Activate MUX mode on a redisContext without sending HELLO.
 *
 * Use this when the HELLO 3 MULTIPLEX handshake has already been performed
 * (e.g., via prefix commands in redis-benchmark). This function only
 * activates the MUX frame encoding/decoding layer and drains any leftover
 * data from the hiredis reader buffer that may contain MUX frames.
 *
 * @param c         A connected redisContext whose HELLO handshake is already done.
 * @param stream_id The stream ID to use (must be odd for client-initiated streams).
 *                  If 0, defaults to 1.
 * @return REDIS_OK on success, REDIS_ERR on failure (check c->err/c->errstr).
 */
int redisActivateMux(redisContext *c, uint64_t stream_id);

/**
 * Get the current MUX stream ID for this context.
 * Returns 0 if MUX is not enabled.
 */
uint64_t redisGetMuxStreamId(redisContext *c);

/**
 * Check if MUX mode is enabled on this context.
 * Returns 1 if enabled, 0 otherwise.
 */
int redisIsMuxEnabled(redisContext *c);

#ifdef __cplusplus
}
#endif

#endif /* __HIREDIS_MUX_H */
