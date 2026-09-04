# Redis 6.0.8 전달 패키지

## 적용 범위

이 디렉터리의 Worker만 Redis 6.0.8 대응판이다. 기존 프로젝트의 `src/worker`,
`src/ex_agent`, Executor 서버는 이 변경으로 업데이트되지 않는다.
Executor의 pending 회수와 Stream retention은 별도로 보완해야 한다.

수령자의 `build_handlers`, `handle_executor_event`, State, graph 연결,
`EventContext`, binding 등록 방식은 바뀌지 않는다. DB 스키마도 그대로다.

## 기존 패키지에서 바뀐 것

| 항목 | 변경 |
|---|---|
| Pending 회수 | XAUTOCLAIM 대신 XPENDING + XCLAIM을 bounded Lua로 실행 |
| 조회 범위 | IDLE 옵션·exclusive ID 문법을 사용하지 않음 |
| 원자성 | 한 페이지 조회·idle 확인·claim·삭제된 원문 확인을 한 script로 수행 |
| 페이지 제한 | 호출당 최대 500개, 기본 EW_BATCH_SIZE=100 |
| 신규 이벤트 | 회수 페이지 사이에 신규 메시지도 읽어 기아 방지 |
| 원문이 삭제된 pending | 원문 부재를 원자적으로 확인한 항목만 XACK |
| Consumer 자동 삭제 | 시작 시 idle 값만 보고 다른 consumer를 삭제하지 않음 |
| Readiness | 모든 소비 슬롯에서 Redis 처리가 시작되고 오류가 없어야 정상 |
| Redis 명령/권한 오류 | 소비 루프의 ResponseError는 실패 종료; 무한 정상 재시도 금지 |
| 연결 오류 | backoff 재시도 중 unhealthy, 정상 처리 재개 시 복원 |
| lag | 미지원 값은 -1, lag_known=0; unread 존재 여부 별도 표시 |
| Python client | Redis 6.0을 지원 범위에 포함하는 redis-py 5.3.1로 lock |
| 이미지 | uv.lock 기반 non-editable 설치, 기본 CMD는 Worker 그대로 |

회수 script는 O(전체 pending)이 아니라 한 페이지에만 비례하는 작업을 한다.
ID는 Lua 부동소수점으로 계산하지 않는다. Python 정수로 uint64 두 구성요소의
successor를 계산하므로 정밀도 손실과 sequence overflow를 처리한다.
새 페이지가 있어도 `BLOCK 0`을 사용하지 않는다. 이는 Redis에서 무한 대기를
뜻하기 때문이다. 회수 중에는 비차단 신규 읽기, 한 바퀴를 마치면 유한 BLOCK이다.

consumer idle은 6.0에서 '마지막 성공한 작업 이후 시간'이다. 프로세스가 살아서
대기 중이어도 커질 수 있으므로 자동 GC를 끈다. 오래된 consumer 메타데이터는
남을 수 있으며, 별도 운영 정리는 replica 종료 확인 및 pending 보호가 필요하다.

## 재전달 방법

새로 전달한다면 이 디렉터리 전체를 전달한다.
이미 Agent 개발자가 커스터마이징했다면 다음만 교체/병합한다.

- `src/worker/` 전체: 신규 `redis_streams.py` 포함.
- `pyproject.toml`의 Redis 의존성: `redis>=5.3,<6`.
- 독립 프로젝트라면 `uv.lock`, Dockerfile도 함께 갱신.
  다른 저장소로 이식했다면 그 저장소의 uv.lock을 다시 해결한다.
- 선택적으로 `tests/`, `compose.test.yaml`, 이 문서.

`src/agent`와 `src/agent_worker`에 수령자가 구현한 내용을 덮어쓰지 않는다.
API의 ApiWorkerBridge와 Worker는 같은 Redis 클라이언트 의존성을 사용한다.
설정변수, namespace, group 이름을 변경할 필요는 없다. 기존 PEL과 DB를 지우거나
consumer group을 재생성하지 않는다. 중복 전달은 기존 receipt/멱등 계약으로 처리한다.

## 검증 실행

2026-09-04 검증 결과: redis-py 5.3.1, PostgreSQL 17.10,
Python 3.12의 설치된 전달 패키지를 사용했다.

| Redis 서버 | 패키지 테스트 |
|---|---|
| 6.0.8 | 54 passed |
| 7.4.10 | 54 passed |

Ruff lint/format 및 ty 검사도 통과했다. 테스트에는 페이지 회수, 동시 회수 경쟁,
heartbeat 보호, 삭제된 원문, 신규 메시지 기아 방지, 실제 ACL 거부, 연결 오류
주입 후 회복, health HTTP, Inbox/Outbox 원자성·중복·순서·재시도·런타임 교체 복구가
포함된다. LangGraph 최소 연결 테스트도 유지했다. 실제 운영 K8s/Executor/LLM,
Sentinel/Cluster failover와 장기 부하를 검증한 것은 아니다.

전달 패키지 디렉터리에서 실행한다. host 포트나 기존 Redis/DB 볼륨을 사용하지
않는 독립 Compose 프로젝트다. DB는 tmpfs이며 재시작 후 테스트 마이그레이션한다.

```bash
docker compose -f compose.test.yaml up --build \
  --abort-on-container-exit --exit-code-from test
docker compose -f compose.test.yaml down

TEST_REDIS_IMAGE=redis:7.4-alpine docker compose -f compose.test.yaml up \
  --build --abort-on-container-exit --exit-code-from test
docker compose -f compose.test.yaml down
```

소스 경로를 PYTHONPATH로 주입하지 않고 설치된 distribution으로 검사한다.
기본 Worker 배포 이미지는 다음처럼 빌드한다.

```bash
docker build --target runtime -t executor-event-worker:redis608 .
```

기본 마지막 target도 runtime이다. test target의 CMD가 운영 CMD를 바꾸지 않는다.

## 지표와 운영 조건

`ew_stream{kind="ingress|dispatch", metric="..."}`:

- pending: 전달됐지만 아직 ACK되지 않은 개수.
- lag: 서버가 제공하는 값. 없는 경우 -1; 0으로 간주하지 않는다.
- lag_known: lag를 서버에서 받았으면 1, 없으면 0.
- has_unread: 아직 읽지 않은 Stream 항목이 있으면 1, 없으면 0.
  lag 미지원 시 XRANGE COUNT 1로 확인하므로 전체 대기량 계산은 하지 않는다.

Redis 6.0.8 경보는 lag > 0만 보지 말고 has_unread, pending, ew_backlog 및
처리 성공/실패 지표를 함께 본다. 지표는 관찰 시점 값이지 작업 완료 증명은 아니다.
Readiness는 애플리케이션 수준 신호이며 네트워크 요청 timeout 전까지는 장애
감지가 지연될 수 있다. 소비가 성공해도 모든 업무 handler의 성공을 보장하지 않는다.

현재 연결 대상은 단일 writable Redis endpoint다. Redis Cluster용 client,
hash-slot 분배, Sentinel discovery를 추가한 것은 아니다.
Streams(XADD/XREADGROUP/XPENDING/XCLAIM/XACK/XINFO/XGROUP/XRANGE), EVAL,
GET/SET/DEL/EXPIRE/INCR, MULTI/EXEC 및 key 접근 권한이 필요하다.
스크립트와 트랜잭션은 실행 중 오류 발생 시 일반적인 DB rollback을 제공하지 않으므로
운영 ACL은 실제 사용하는 모든 명령에 대해 사전 검증해야 한다.

순수 6.0.8은 오래된 서버 버전이다. 이 패키지는 명령 호환성을 다루며 서버 보안
패치, TLS/ACL 운영, AOF/복제/백업, eviction 안전성을 보장하지 않는다.
운영 공급사의 보안 백포트와 지원 계약 확인이 필요하다.

## 이 패키지 밖의 후속 작업

1. Executor의 XAUTOCLAIM과 XTRIM MINID 대응.
2. 원본 서비스 Consumer·Stream 정리·DLQ 커서 조회·cutover 검사 대응.
3. 운영 ACL·인증·TLS·실제 failover 토폴로지로 배포 검증.
4. Consumer 메타데이터 및 Stream 장기 보존/정리 정책.

이 패키지에는 기존에도 Stream retention/DLQ 운영 조회 API가 없었다.
해당 API를 새로 추가하거나 전체 서비스 호환 완료라고 주장하지 않는다.

## 근거

- [XPENDING 옵션 버전](https://redis.io/docs/latest/commands/xpending/)
- [XRANGE 구버전 cursor 처리](https://redis.io/docs/latest/commands/xrange/)
- [XINFO GROUPS lag](https://redis.io/docs/latest/commands/xinfo-groups/)
- [XINFO CONSUMERS idle 의미](https://redis.io/docs/latest/commands/xinfo-consumers/)
- [redis-py 5.3.1 지원 범위](https://github.com/redis/redis-py/tree/v5.3.1#supported-redis-versions)
