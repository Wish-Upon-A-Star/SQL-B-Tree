#ifndef MOCK_CMD_PROCESSOR_H
#define MOCK_CMD_PROCESSOR_H

#include "cmd_processor.h"

/**
 * @file mock_cmd_processor.h
 * @brief DB 구현 연결 전 외부 진입점과 CmdProcessor 계약을 검증하는 mock 구현체.
 */

/**
 * @brief 고정 pool 기반 mock CmdProcessor 인스턴스를 생성한다.
 *
 * base_context의 name, max_sql_len, request_buffer_count,
 * response_body_capacity 값이 0 또는 NULL이 아니면 mock 기본값 대신 사용한다.
 * 생성된 processor와 내부 context는 mock 구현체가 소유하며, 사용 후
 * cmd_processor_shutdown()으로 해제해야 한다.
 *
 * @param base_context 선택적 기본 설정. NULL이면 mock 기본값을 사용한다.
 * @param out_processor 성공 시 생성된 processor 포인터를 받는다.
 * @return 성공 시 0, 메모리 할당 실패 등 생성 실패 시 -1.
 */
int mock_cmd_processor_create(const CmdProcessorContext *base_context,
                              CmdProcessor **out_processor);

#endif
