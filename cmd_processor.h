#ifndef CMD_PROCESSOR_H
#define CMD_PROCESSOR_H

#include <stddef.h>

/**
 * @file cmd_processor.h
 * @brief 외부 진입점과 DB 구현체 사이의 공통 요청 처리 API를 정의한다.
 *
 * CmdProcessor API는 CLI, TCP, HTTP 같은 외부 진입점이 같은 요청/응답
 * 모델로 DB 구현체를 호출할 수 있게 한다. CmdRequest, CmdResponse, 그리고
 * 내부 문자열 버퍼의 소유권은 모두 CmdProcessor 구현체에 있다.
 */

/**
 * @brief CmdProcessor가 처리할 요청 종류.
 */
typedef enum {
    /** SQL 문자열 실행 요청. */
    CMD_REQUEST_SQL,
    /** 상태 확인용 ping 요청. */
    CMD_REQUEST_PING
} CmdRequestType;

/**
 * @brief 요청 처리 결과를 나타내는 안정적인 상태 코드.
 */
typedef enum {
    /** 요청이 정상 처리됨. */
    CMD_STATUS_OK,
    /** 요청 형식이 잘못되었거나 필수 값이 누락됨. */
    CMD_STATUS_BAD_REQUEST,
    /** SQL 문자열 길이가 processor 설정 한도를 초과함. */
    CMD_STATUS_SQL_TOO_LONG,
    /** SQL 파싱에 실패함. */
    CMD_STATUS_PARSE_ERROR,
    /** 요청 처리 중 일반 처리 오류가 발생함. */
    CMD_STATUS_PROCESSING_ERROR,
    /** 구현체가 현재 요청을 받을 수 없는 busy 상태임. */
    CMD_STATUS_BUSY,
    /** 구현체가 정한 시간 안에 처리를 완료하지 못함. */
    CMD_STATUS_TIMEOUT,
    /** API 사용 오류 또는 치명적 내부 오류. */
    CMD_STATUS_INTERNAL_ERROR
} CmdStatusCode;

/**
 * @brief CmdResponse.body payload의 형식.
 */
typedef enum {
    /** 응답 body가 없음. */
    CMD_BODY_NONE,
    /** 사람이 읽을 수 있는 plain text body. */
    CMD_BODY_TEXT,
    /** JSON fragment 또는 JSON value 문자열 body. */
    CMD_BODY_JSON
} CmdBodyFormat;

/**
 * @brief 요청 1개를 표현하는 processor-owned context.
 *
 * 외부 진입점은 이 구조체를 직접 생성하지 않고
 * cmd_processor_acquire_request()로 얻은 뒤 setter API로 값을 채운다.
 */
typedef struct {
    /** 외부 호출자가 부여한 요청 ID. 응답에도 같은 값이 보존된다. */
    char request_id[64];
    /** 요청 종류. */
    CmdRequestType type;
    /** processor 소유 SQL 버퍼. CMD_REQUEST_SQL에서 사용한다. */
    char *sql;
} CmdRequest;

/**
 * @brief 요청 처리 결과를 표현하는 processor-owned 응답 객체.
 *
 * body와 error_message 버퍼도 CmdProcessor 구현체가 소유한다. 호출자는
 * 사용 후 cmd_processor_release_response()로 반환해야 하며 직접 해제하지 않는다.
 */
typedef struct {
    /** 원 요청 ID. */
    char request_id[64];
    /** 요청 처리 상태 코드. */
    CmdStatusCode status;
    /** 성공이면 1, 실패이면 0. */
    int ok;
    /** 조회 결과 row 수. 구현체가 제공하지 않으면 0. */
    int row_count;
    /** INSERT/UPDATE/DELETE 등으로 영향을 받은 row 수. */
    int affected_count;
    /** body payload 형식. */
    CmdBodyFormat body_format;
    /** processor 소유 응답 payload 버퍼. body가 없으면 NULL. */
    char *body;
    /** body payload byte 길이. body가 NUL 종료 문자열이어도 이 값을 우선한다. */
    size_t body_len;
    /** 실패 시 사람이 읽을 수 있는 오류 메시지. 없으면 NULL. */
    char *error_message;
} CmdResponse;

/**
 * @brief processor 인스턴스 단위의 공용 context.
 *
 * 프로세스 전체 singleton이 아니며 processor 구현체 또는 인스턴스마다
 * 서로 다른 context를 가질 수 있다.
 */
typedef struct {
    /** 구현체 또는 인스턴스 이름. */
    const char *name;
    /** SQL 문자열 최대 byte 길이. */
    size_t max_sql_len;
    /** 구현체가 준비할 request buffer 수. */
    size_t request_buffer_count;
    /** response body buffer capacity. */
    size_t response_body_capacity;
    /** 구현체 전용 공유 상태. 외부 진입점은 해석하지 않는다. */
    void *shared_state;
} CmdProcessorContext;

/**
 * @brief CmdProcessor 구현체 vtable과 context 묶음.
 *
 * 외부 진입점은 가능하면 아래 public wrapper 함수만 호출한다. callback은
 * 구현체가 채우며, wrapper는 null check와 공통 계약을 담당한다.
 */
typedef struct CmdProcessor {
    /** processor 인스턴스 공용 context. */
    CmdProcessorContext *context;
    /**
     * @brief processor-owned CmdRequest slot을 확보한다.
     * @return 성공 시 0, 실패 시 0이 아닌 값.
     */
    int (*acquire_request)(CmdProcessorContext *context,
                           CmdRequest **out_request);
    /**
     * @brief 요청을 처리하고 processor-owned CmdResponse를 반환한다.
     * @return API 호출 자체가 수행되면 0, API 사용 오류나 치명적 오류면 0이 아닌 값.
     */
    int (*process)(CmdProcessorContext *context,
                   CmdRequest *request,
                   CmdResponse **out_response);
    /**
     * @brief frontend validation 실패 등을 processor-owned 오류 응답으로 만든다.
     * @return 성공 시 0, response slot 확보 실패 등 API 실패 시 0이 아닌 값.
     */
    int (*make_error_response)(CmdProcessorContext *context,
                               const char *request_id,
                               CmdStatusCode status,
                               const char *error_message,
                               CmdResponse **out_response);
    /**
     * @brief acquire_request()로 얻은 요청 slot을 반환한다.
     */
    void (*release_request)(CmdProcessorContext *context,
                            CmdRequest *request);
    /**
     * @brief process() 또는 make_error_response()로 얻은 응답 slot을 반환한다.
     */
    void (*release_response)(CmdProcessorContext *context,
                             CmdResponse *response);
    /**
     * @brief processor 구현체가 가진 자원을 해제한다.
     */
    void (*shutdown)(CmdProcessorContext *context);
} CmdProcessor;

/**
 * @brief 상태 코드를 wire/log에 사용할 안정적인 문자열로 변환한다.
 *
 * @param status 변환할 상태 코드.
 * @return 상태 코드 문자열. 알 수 없는 값이면 "UNKNOWN".
 */
const char *cmd_status_to_string(CmdStatusCode status);

/**
 * @brief processor-owned 요청 객체를 확보한다.
 *
 * @param processor 사용할 processor.
 * @param out_request 성공 시 요청 객체 포인터를 받는다.
 * @return 성공 시 0, 실패 시 -1.
 */
int cmd_processor_acquire_request(CmdProcessor *processor,
                                  CmdRequest **out_request);

/**
 * @brief SQL 요청 내용을 processor-owned request buffer에 복사한다.
 *
 * @param processor 사용할 processor.
 * @param request cmd_processor_acquire_request()로 얻은 요청 객체.
 * @param request_id 응답에 보존할 요청 ID. NULL이면 빈 문자열로 설정된다.
 * @param sql NUL 종료 SQL 문자열.
 * @return 성공 시 CMD_STATUS_OK, 검증 실패 시 해당 CmdStatusCode.
 */
CmdStatusCode cmd_processor_set_sql_request(CmdProcessor *processor,
                                            CmdRequest *request,
                                            const char *request_id,
                                            const char *sql);

/**
 * @brief ping 요청 내용을 processor-owned request에 설정한다.
 *
 * @param processor 사용할 processor.
 * @param request cmd_processor_acquire_request()로 얻은 요청 객체.
 * @param request_id 응답에 보존할 요청 ID. NULL이면 빈 문자열로 설정된다.
 * @return 성공 시 CMD_STATUS_OK, 검증 실패 시 해당 CmdStatusCode.
 */
CmdStatusCode cmd_processor_set_ping_request(CmdProcessor *processor,
                                             CmdRequest *request,
                                             const char *request_id);

/**
 * @brief 요청을 처리하고 processor-owned 응답 객체를 받는다.
 *
 * @param processor 사용할 processor.
 * @param request 처리할 요청 객체.
 * @param out_response 성공 시 응답 객체 포인터를 받는다.
 * @return API 호출 자체가 수행되면 0, API 실패 시 -1.
 */
int cmd_processor_process(CmdProcessor *processor,
                          CmdRequest *request,
                          CmdResponse **out_response);

/**
 * @brief frontend validation 오류를 processor-owned 응답 객체로 만든다.
 *
 * @param processor 사용할 processor.
 * @param request_id 응답에 넣을 요청 ID. NULL이면 빈 문자열로 설정된다.
 * @param status 오류 상태 코드.
 * @param error_message 사람이 읽을 수 있는 오류 메시지.
 * @param out_response 성공 시 응답 객체 포인터를 받는다.
 * @return 성공 시 0, 실패 시 -1.
 */
int cmd_processor_make_error_response(CmdProcessor *processor,
                                      const char *request_id,
                                      CmdStatusCode status,
                                      const char *error_message,
                                      CmdResponse **out_response);

/**
 * @brief 요청 객체를 processor에 반환한다.
 */
void cmd_processor_release_request(CmdProcessor *processor,
                                   CmdRequest *request);

/**
 * @brief 응답 객체를 processor에 반환한다.
 */
void cmd_processor_release_response(CmdProcessor *processor,
                                    CmdResponse *response);

/**
 * @brief processor 구현체 자원을 해제한다.
 *
 * 호출 후 processor 포인터와 그 context는 더 이상 사용하지 않는다.
 */
void cmd_processor_shutdown(CmdProcessor *processor);

#endif
