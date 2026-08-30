# Readiness and Alerting

## Endpoint 계약

API와 Worker는 liveness와 readiness를 분리한다.

| Component | Liveness | Readiness | Port |
|---|---|---|---:|
| API | `/healthz` | `/readyz` | 8010 |
| Worker | `/healthz` | `/readyz` | 8011 |

`/healthz`는 프로세스가 HTTP 요청을 받을 수 있는지만 확인하며 항상 의존성과
분리한다. `/readyz`는 작업을 durable하게 수락하고 처리하는 데 반드시 필요한
PostgreSQL과 Redis를 확인한다.

- API는 요청마다 두 의존성을 비동기로 동시에 검사한다.
- Worker는 기본 10초마다 검사한 snapshot을 제공한다.
- Worker snapshot이 기본 30초보다 오래되면 `503`과 `stale=true`를 반환한다.
- 각 probe는 기본 2초 timeout을 독립적으로 적용한다.
- 오류 응답은 예외 class만 공개하며 접속 URL이나 credential을 노출하지 않는다.

정상 응답은 `200`, 하나라도 실패하거나 snapshot이 오래되면 `503`이다.

```json
{
  "status": "ready",
  "ready": true,
  "stale": false,
  "checks": {
    "postgres": {"ready": true, "latency_seconds": 0.003, "error": null},
    "redis": {"ready": true, "latency_seconds": 0.001, "error": null}
  }
}
```

Model과 Executor는 readiness 의존성에 포함하지 않는다. 일시적인 외부 서비스
장애나 장기 작업 부하로 모든 Worker가 제거되는 것을 막고, 이미 수락한 durable
작업을 재시도·복구할 실행 주체를 남기기 위해서다. 이 장애는 Worker retry와 작업
실패 지표로 별도 경보한다.

backlog, Redis pending, consumer lag도 readiness에서 제외한다. 과부하 상태에서
Pod를 endpoint에서 제거하면 가용 처리량이 더 줄기 때문에 capacity 경보와
수평 확장 판단에 사용한다.

## Kubernetes probe 권장값

API와 Worker 모두 다음 시작값을 사용한다. Worker의 `initialDelaySeconds`는 Skill
registry와 LangGraph checkpoint 초기화를 고려해 API보다 길게 설정한다.

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8010
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /readyz
    port: 8010
  periodSeconds: 10
  timeoutSeconds: 3
  failureThreshold: 3
```

Worker는 port만 `8011`로 바꾸고 `startupProbe` 또는 최소 20초의 초기 유예를
둔다. Compose에도 같은 `/readyz` healthcheck가 설정되어 있다.

## Worker 종료 계약

Worker는 `SIGTERM` 또는 `SIGINT`를 받으면 즉시 readiness를 `503`과
`error=stopping`으로 전환해 새 traffic에서 제외한다. 이후 새 Redis Stream
message claim을 중단하고 진행 중 handler를 bounded drain한다. outbox와 metrics
loop도 새 iteration을 시작하지 않는다.

```yaml
spec:
  terminationGracePeriodSeconds: 30
  containers:
    - name: agent-worker
      env:
        - name: WORKER_SHUTDOWN_GRACE_SECONDS
          value: "25"
```

`terminationGracePeriodSeconds`는 `WORKER_SHUTDOWN_GRACE_SECONDS`보다 크게 두어
checkpoint pool, Redis, HTTP client와 metrics server를 닫을 시간을 남긴다. grace
초과 시 runtime task를 취소하지만 메시지를 ACK하지 않으므로 claim idle 이후 다른
Worker가 복구한다. liveness는 프로세스가 실제 종료될 때까지 유지된다.

설정 가능한 환경 변수:

| 환경 변수 | 기본값 | 의미 |
|---|---:|---|
| `READINESS_PROBE_TIMEOUT_SECONDS` | 2 | 개별 DB/Redis probe timeout |
| `WORKER_METRICS_REFRESH_SECONDS` | 10 | Worker snapshot 갱신 주기 |
| `WORKER_READINESS_STALE_SECONDS` | 30 | Worker snapshot 최대 허용 나이 |
| `WORKER_SHUTDOWN_GRACE_SECONDS` | 25 | 종료 시 in-flight drain 상한 |
| `COMMAND_MAX_RETRY_ATTEMPTS` | 5 | command handler retry 상한 |
| `EXECUTOR_EVENT_MAX_RETRY_ATTEMPTS` | 100 | Executor event retry 상한 |
| `STREAM_RETRY_STATE_TTL_SECONDS` | 604800 | retry counter 보존기간 |
| `DLQ_ACTION_MARKER_TTL_SECONDS` | 7776000 | DLQ action 멱등성 보존기간 |
| `STREAM_RETENTION_SECONDS` | 604800 | safe trim 최소 시간 보존기간 |
| `STREAM_MINIMUM_RETAINED_ENTRIES` | 1000 | Stream별 최소 최근 entry 수 |

stale 기준은 갱신 주기보다 반드시 커야 한다.

## Prometheus 경보 기준

실제 rule은
`deploy/prometheus/ex-agent-alerts.yml`에 있다. 운영 Prometheus의 rule file로
mount하고 Alertmanager routing은 플랫폼의 서비스·환경 label 정책에 맞춘다.

| 경보 | Warning | Critical |
|---|---:|---:|
| 필수 dependency unavailable | - | 전체 replica 1분 |
| dependency probe stale | - | 45초 초과 상태가 2분 |
| durable delivery backlog | 100 초과 10분 | 1,000 초과 5분 |
| Redis pending | 50 초과 10분 | 500 초과 5분 |
| Redis consumer lag | 100 초과 10분 | 1,000 초과 5분 |
| Worker transport retry rate | 0.1/s 초과 10분 | 1/s 초과 5분 |
| API/Worker metric 부재 | - | 2분 |

여러 Worker가 같은 Redis/DB 수치를 노출하므로 backlog, pending, lag rule은
replica별 값을 합산하지 않고 `max`를 사용한다. Dependency 장애도 일부 Pod의
일시적인 rolling restart로 paging하지 않도록 모든 관측 replica가 실패할 때만
발생한다. Warning 범위는 critical 기준 이하로 제한해 같은 원인으로 두 severity가
동시에 발생하지 않는다.

이 값은 V1 초기 운영 기준이다. 운영 traffic에서 정상 p95와 incident 시점을
최소 2주 수집한 뒤 경보 정확도와 처리 SLA에 맞춰 조정한다.
