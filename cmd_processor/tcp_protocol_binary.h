#ifndef TCP_PROTOCOL_BINARY_H
#define TCP_PROTOCOL_BINARY_H

#include "cmd_processor.h"

#include <stddef.h>
#include <stdint.h>

#define TCP_PROTOCOL_REQUEST_MAGIC 0x31505451u
#define TCP_PROTOCOL_RESPONSE_MAGIC 0x31505452u
#define TCP_PROTOCOL_VERSION 1u

typedef enum {
    TCP_BINARY_OP_INVALID = 0,
    TCP_BINARY_OP_PING = 1,
    TCP_BINARY_OP_SQL = 2,
    TCP_BINARY_OP_CLOSE = 3
} TCPBinaryOp;

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t op;
    uint32_t flags;
    uint16_t request_id_len;
    uint16_t reserved;
    uint32_t payload_len;
    uint32_t reserved2;
} TCPBinaryRequestHeader;

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t status;
    uint32_t flags;
    uint16_t request_id_len;
    uint16_t body_format;
    uint32_t body_len;
    uint32_t error_len;
    uint32_t row_count;
    uint32_t affected_count;
} TCPBinaryResponseHeader;

typedef struct {
    TCPBinaryRequestHeader header;
    const char *request_id;
    const char *payload;
} TCPBinaryDecodedRequest;

typedef struct {
    TCPBinaryResponseHeader header;
    const char *request_id;
    const char *body;
    const char *error;
} TCPBinaryDecodedResponse;

size_t tcp_binary_request_header_size(void);
size_t tcp_binary_response_header_size(void);

void tcp_binary_encode_request_header(unsigned char *dst,
                                      const TCPBinaryRequestHeader *header);
void tcp_binary_encode_response_header(unsigned char *dst,
                                       const TCPBinaryResponseHeader *header);

int tcp_binary_decode_request_header(const unsigned char *src,
                                     size_t src_len,
                                     TCPBinaryRequestHeader *out_header);
int tcp_binary_decode_response_header(const unsigned char *src,
                                      size_t src_len,
                                      TCPBinaryResponseHeader *out_header);

int tcp_binary_validate_request_header(const TCPBinaryRequestHeader *header);
int tcp_binary_validate_response_header(const TCPBinaryResponseHeader *header);

size_t tcp_binary_request_frame_size(const TCPBinaryRequestHeader *header);
size_t tcp_binary_response_frame_size(const TCPBinaryResponseHeader *header);

int tcp_binary_decode_request_frame(const unsigned char *src,
                                    size_t src_len,
                                    TCPBinaryDecodedRequest *out_request);
int tcp_binary_decode_response_frame(const unsigned char *src,
                                     size_t src_len,
                                     TCPBinaryDecodedResponse *out_response);

void tcp_binary_init_request_header(TCPBinaryRequestHeader *header,
                                    TCPBinaryOp op,
                                    size_t request_id_len,
                                    size_t payload_len);
void tcp_binary_init_response_header(TCPBinaryResponseHeader *header,
                                     CmdStatusCode status,
                                     int ok,
                                     CmdBodyFormat body_format,
                                     size_t request_id_len,
                                     size_t body_len,
                                     size_t error_len,
                                     int row_count,
                                     int affected_count);

#endif
