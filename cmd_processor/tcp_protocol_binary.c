#include "tcp_protocol_binary.h"

#include <limits.h>
#include <string.h>

static uint16_t read_u16_le(const unsigned char *src) {
    return (uint16_t)((uint16_t)src[0] | ((uint16_t)src[1] << 8));
}

static uint32_t read_u32_le(const unsigned char *src) {
    return (uint32_t)((uint32_t)src[0] |
                      ((uint32_t)src[1] << 8) |
                      ((uint32_t)src[2] << 16) |
                      ((uint32_t)src[3] << 24));
}

static void write_u16_le(unsigned char *dst, uint16_t value) {
    dst[0] = (unsigned char)(value & 0xffu);
    dst[1] = (unsigned char)((value >> 8) & 0xffu);
}

static void write_u32_le(unsigned char *dst, uint32_t value) {
    dst[0] = (unsigned char)(value & 0xffu);
    dst[1] = (unsigned char)((value >> 8) & 0xffu);
    dst[2] = (unsigned char)((value >> 16) & 0xffu);
    dst[3] = (unsigned char)((value >> 24) & 0xffu);
}

static int size_fits_u16(size_t value) {
    return value <= 0xffffu;
}

static int size_fits_u32(size_t value) {
    return value <= 0xffffffffu;
}

size_t tcp_binary_request_header_size(void) {
    return 20u;
}

size_t tcp_binary_response_header_size(void) {
    return 32u;
}

void tcp_binary_encode_request_header(unsigned char *dst,
                                      const TCPBinaryRequestHeader *header) {
    if (!dst || !header) return;
    write_u32_le(dst + 0, header->magic);
    write_u16_le(dst + 4, header->version);
    write_u16_le(dst + 6, header->op);
    write_u32_le(dst + 8, header->flags);
    write_u16_le(dst + 12, header->request_id_len);
    write_u16_le(dst + 14, header->reserved);
    write_u32_le(dst + 16, header->payload_len);
}

void tcp_binary_encode_response_header(unsigned char *dst,
                                       const TCPBinaryResponseHeader *header) {
    if (!dst || !header) return;
    write_u32_le(dst + 0, header->magic);
    write_u16_le(dst + 4, header->version);
    write_u16_le(dst + 6, header->status);
    write_u32_le(dst + 8, header->flags);
    write_u16_le(dst + 12, header->request_id_len);
    write_u16_le(dst + 14, header->body_format);
    write_u32_le(dst + 16, header->body_len);
    write_u32_le(dst + 20, header->error_len);
    write_u32_le(dst + 24, header->row_count);
    write_u32_le(dst + 28, header->affected_count);
}

int tcp_binary_decode_request_header(const unsigned char *src,
                                     size_t src_len,
                                     TCPBinaryRequestHeader *out_header) {
    if (!src || !out_header || src_len < tcp_binary_request_header_size()) return 0;
    memset(out_header, 0, sizeof(*out_header));
    out_header->magic = read_u32_le(src + 0);
    out_header->version = read_u16_le(src + 4);
    out_header->op = read_u16_le(src + 6);
    out_header->flags = read_u32_le(src + 8);
    out_header->request_id_len = read_u16_le(src + 12);
    out_header->reserved = read_u16_le(src + 14);
    out_header->payload_len = read_u32_le(src + 16);
    out_header->reserved2 = 0;
    return 1;
}

int tcp_binary_decode_response_header(const unsigned char *src,
                                      size_t src_len,
                                      TCPBinaryResponseHeader *out_header) {
    if (!src || !out_header || src_len < tcp_binary_response_header_size()) return 0;
    memset(out_header, 0, sizeof(*out_header));
    out_header->magic = read_u32_le(src + 0);
    out_header->version = read_u16_le(src + 4);
    out_header->status = read_u16_le(src + 6);
    out_header->flags = read_u32_le(src + 8);
    out_header->request_id_len = read_u16_le(src + 12);
    out_header->body_format = read_u16_le(src + 14);
    out_header->body_len = read_u32_le(src + 16);
    out_header->error_len = read_u32_le(src + 20);
    out_header->row_count = read_u32_le(src + 24);
    out_header->affected_count = read_u32_le(src + 28);
    return 1;
}

int tcp_binary_validate_request_header(const TCPBinaryRequestHeader *header) {
    if (!header) return 0;
    if (header->magic != TCP_PROTOCOL_REQUEST_MAGIC) return 0;
    if (header->version != TCP_PROTOCOL_VERSION) return 0;
    if (header->op < TCP_BINARY_OP_PING || header->op > TCP_BINARY_OP_CLOSE) return 0;
    if (header->reserved != 0 || header->reserved2 != 0) return 0;
    return 1;
}

int tcp_binary_validate_response_header(const TCPBinaryResponseHeader *header) {
    if (!header) return 0;
    if (header->magic != TCP_PROTOCOL_RESPONSE_MAGIC) return 0;
    if (header->version != TCP_PROTOCOL_VERSION) return 0;
    if (header->body_format > CMD_BODY_BINARY) return 0;
    return 1;
}

size_t tcp_binary_request_frame_size(const TCPBinaryRequestHeader *header) {
    if (!header) return 0;
    return tcp_binary_request_header_size() +
           (size_t)header->request_id_len +
           (size_t)header->payload_len;
}

size_t tcp_binary_response_frame_size(const TCPBinaryResponseHeader *header) {
    if (!header) return 0;
    return tcp_binary_response_header_size() +
           (size_t)header->request_id_len +
           (size_t)header->body_len +
           (size_t)header->error_len;
}

int tcp_binary_decode_request_frame(const unsigned char *src,
                                    size_t src_len,
                                    TCPBinaryDecodedRequest *out_request) {
    size_t total_size;
    const unsigned char *cursor;

    if (!src || !out_request) return 0;
    memset(out_request, 0, sizeof(*out_request));
    if (!tcp_binary_decode_request_header(src, src_len, &out_request->header)) return 0;
    if (!tcp_binary_validate_request_header(&out_request->header)) return 0;
    total_size = tcp_binary_request_frame_size(&out_request->header);
    if (src_len < total_size) return 0;
    cursor = src + tcp_binary_request_header_size();
    out_request->request_id = (const char *)cursor;
    cursor += out_request->header.request_id_len;
    out_request->payload = (const char *)cursor;
    return 1;
}

int tcp_binary_decode_response_frame(const unsigned char *src,
                                     size_t src_len,
                                     TCPBinaryDecodedResponse *out_response) {
    size_t total_size;
    const unsigned char *cursor;

    if (!src || !out_response) return 0;
    memset(out_response, 0, sizeof(*out_response));
    if (!tcp_binary_decode_response_header(src, src_len, &out_response->header)) return 0;
    if (!tcp_binary_validate_response_header(&out_response->header)) return 0;
    total_size = tcp_binary_response_frame_size(&out_response->header);
    if (src_len < total_size) return 0;
    cursor = src + tcp_binary_response_header_size();
    out_response->request_id = (const char *)cursor;
    cursor += out_response->header.request_id_len;
    out_response->body = (const char *)cursor;
    cursor += out_response->header.body_len;
    out_response->error = (const char *)cursor;
    return 1;
}

void tcp_binary_init_request_header(TCPBinaryRequestHeader *header,
                                    TCPBinaryOp op,
                                    size_t request_id_len,
                                    size_t payload_len) {
    if (!header) return;
    memset(header, 0, sizeof(*header));
    header->magic = TCP_PROTOCOL_REQUEST_MAGIC;
    header->version = TCP_PROTOCOL_VERSION;
    header->op = (uint16_t)op;
    if (size_fits_u16(request_id_len)) header->request_id_len = (uint16_t)request_id_len;
    if (size_fits_u32(payload_len)) header->payload_len = (uint32_t)payload_len;
}

void tcp_binary_init_response_header(TCPBinaryResponseHeader *header,
                                     CmdStatusCode status,
                                     int ok,
                                     CmdBodyFormat body_format,
                                     size_t request_id_len,
                                     size_t body_len,
                                     size_t error_len,
                                     int row_count,
                                     int affected_count) {
    if (!header) return;
    memset(header, 0, sizeof(*header));
    header->magic = TCP_PROTOCOL_RESPONSE_MAGIC;
    header->version = TCP_PROTOCOL_VERSION;
    header->status = (uint16_t)status;
    header->flags = ok ? 1u : 0u;
    header->body_format = (uint16_t)body_format;
    if (size_fits_u16(request_id_len)) header->request_id_len = (uint16_t)request_id_len;
    if (size_fits_u32(body_len)) header->body_len = (uint32_t)body_len;
    if (size_fits_u32(error_len)) header->error_len = (uint32_t)error_len;
    if (row_count > 0) header->row_count = (uint32_t)row_count;
    if (affected_count > 0) header->affected_count = (uint32_t)affected_count;
}
