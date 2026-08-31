# Kubernetes: API+Agent와 Worker를 같은 Pod에 배포

[api-agent-worker.yaml.example](api-agent-worker.yaml.example)은
**인수 서비스의 어댑터/entrypoint를 구현한 뒤 치환하는 배포 템플릿**이다.
현재 ex-agent 서비스를 그대로 배포하거나 예제 Agent 서버를 즉시 실행하는
manifest가 아니다. 클러스터에는 적용하지 않았다.

```text
Deployment: handoff-agent
└─ Pod
   ├─ api-agent container ─ uvicorn your_agent.api:app
   └─ worker container    ─ python -m your_agent.worker_main

두 컨테이너: 동일 image version, PG/Redis, checkpoint 규칙, 공유 PVC
Service: API 포트만 노출
```

같은 Pod의 컨테이너는 함께 배치되고 네트워크를 공유하지만 Python 메모리를
공유하지 않는다. 실행 상태는 DB에 둔다.
[Kubernetes Pod 문서](https://kubernetes.io/docs/concepts/workloads/pods/)

## 1. 이미지 하나에 실행 코드 둘

인수 서비스 Dockerfile은 자신의 `src/your_agent/`에 API, Worker, 공통 graph와
이식한 consumer/adapter를 모두 포함한다. `uv sync --frozen --no-dev
--no-editable`로 이미지 빌드 시 설치하고 런타임에는 의존성을 설치하지 않는다.
저장소의 루트 Dockerfile을 이식 서비스의 빌드 구성에 맞춰 참고한다.

Dockerfile의 기본 실행은 API만 지정하면 된다.

```dockerfile
# Dependencies and the host package are already installed above this block.
ENV PATH="/app/.venv/bin:${PATH}"
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uvicorn", "your_agent.api:app", "--host", "0.0.0.0", "--port", "8010"]
```

Worker 컨테이너는 동일 이미지의 **args만** 바꾼다.
Kubernetes `command`를 지정하면 이미지 ENTRYPOINT를 대체하므로 tini가
빠질 수 있다. `args`는 CMD를 대체하고 ENTRYPOINT는 유지한다.
[Kubernetes 실행 명령 문서][container-args]

[container-args]: https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/

한 컨테이너의 shell에서 API와 Worker를 `&`로 동시에 띄우지 않는다.
각 컨테이너에 실행 프로세스 하나를 두어 로그·종료·resource limit을 나눈다.
Worker용 HTTP 서버가 있다면 상태/지표 확인용이며 Agent resume API가 아니다.

현재 루트 Dockerfile의 runtime에는 `examples/`가 복사되지 않는다.
`ex-agent-api`는 여전히 START를 Worker에 맡기는 구현이다.
따라서 원본 이미지의 명령만 바꾸면 이번 대상 구조가 완성되는 것은 아니다.
`your_agent.api:app` / `your_agent.worker_main`은 **인수자가 구현할 모듈명**이다.

## 2. 배포 전 연결할 항목

| 항목 | 연결 기준 |
|---|---|
| image | 두 컨테이너에 같은 검증된 태그/digest |
| ConfigMap | Executor URL, Stream/Group, Worker 설정 |
| Secret | DB/Checkpoint/Redis URL, 모델 인증 정보 등 |
| PVC | Executor와 동일한 파일을 볼 수 있는 공유 저장소 |
| Health | 두 entrypoint가 `/health/live`, `/health/ready` 구현 |
| Migration | 앱 배포 전 호스트 migration Job/파이프라인에서 수행 |

Secret `handoff-agent-secrets`는 따로 준비한다. 비밀 값을 저장소에 넣지 않는다.
필수 연결 키 예시는 `AGENT_DATABASE_URL`, `AGENT_CHECKPOINT_DATABASE_URL`,
`AGENT_REDIS_URL`이다. 호스트 Settings 이름이 다르면 전체 어댑터와 함께 맞춘다.
모델/Skill 설정 등 호스트 Agent가 요구하는 값도 추가해야 한다.

API와 Worker는 같은 Agent DB와 checkpoint schema에 연결한다.
SQLAlchemy URL과 psycopg saver URL은 각 client가 요구하는 형식으로 구분한다.
Redis 서버는 Executor와 공유할 수 있지만 Agent Command Stream, Group, DLQ,
run-lock key namespace는 다른 서비스와 충돌하지 않도록 분리한다.
독립 서비스는 Executor Stream을 같은 Group으로 소비하면 이벤트를 나눠 받으므로
서비스별 Group을 쓰고, 같은 서비스의 Pod들은 같은 Group을 쓴다.
소비자 이름에는 Pod UID와 슬롯 구분을 넣는다.

다른 서비스의 execution 이벤트도 들어오는 공유 Stream이라면 자신의 실행 연결을
판단하는 정책이 필요하다. 미연결 이벤트를 모두 영원히 재시도하지 않도록
기존 이벤트 bridge의 식별/복구 계약을 함께 검토한다.

Kubernetes에서는 `host.docker.internal` 대신 실제 Service DNS/접속 주소를 쓴다.
예시의 `http://executor-api:8000/api/v1`도 실제 namespace/Service로 치환한다.
모델 호스트는 클러스터 DNS가 해석할 수 있어야 한다. 로컬 `/etc/hosts` 설정이
Pod로 자동 전달된다고 가정하지 않는다.

PATH 제출 방식은 API와 Worker가 생성한 파일을 Executor에서도 읽을 수 있어야
한다. 예시의 PVC 이름을 실제 공유 저장소로 치환하고 양쪽의 **상대경로 기준**을
맞춘다. 같은 namespace의 PVC 참조가 가능한지도 확인한다.
Executor와 Agent가 다른 노드에 배치되거나 replicas를 늘릴 경우 그 배치를
지원하는 저장소/access mode가 필요하다. 일반 `emptyDir`은 이 용도가 아니다.
예시 UID/GID 10001이 공유 볼륨에서 쓸 수 있는지도 확인한다.

## 3. 생명주기와 상태 검사

- 시작: 연결 pool과 공통 graph를 만들고, Worker는 두 consumer와 relay를 시작한다.
- Readiness: API 의존성 및 Worker 소비 루프의 준비 상태를 확인한다.
  Worker readiness 실패는 같은 Pod의 API Service 유입에도 영향을 준다.
- Liveness: 프로세스 자체가 회복 불가능하게 멈췄는지를 확인한다.
  일시적인 DB 장애나 정상적인 장기 실행을 무조건 재시작 사유로 쓰지 않는다.
- 종료: SIGTERM에서 새 입력/소비를 중단하고 처리 중 호출을 drain한다.
  예시 앱 종료 유예는 25초, Pod 유예는 40초다. 실제 처리/lease 정책에 맞춘다.
- 유예 초과: 체크포인트/입력 원장/PEL에서 복구한다. 종료 시간이 충분하다는
  가정만으로 내구성을 대신하지 않는다.

두 일반 컨테이너의 시작/종료 순서를 가정하지 않는다. API가 내려가도 Worker가
DB로 재개할 수 있어야 하고, Worker가 내려가도 입력/이벤트 기록이 남아야 한다.
실행 중인 며칠짜리 코드는 Executor에 있으므로 API/Worker Pod 종료와 별개다.

리소스 값은 출발점 예시일 뿐 부하 검증 결과가 아니다.
replicas를 늘리면 API와 Worker가 함께 증가한다. 체크포인트 호환성, 공통
RunGuard, Task별 순서, 고유 consumer 이름을 다중 Pod에서도 유지한다.
롤링 배포 중 구/신 버전 공존을 지원하지 못하면 별도의 drain/전환 계획이 필요하다.

## 4. 적용 전 확인

템플릿은 치환 전 실행하지 않는다. 인수 서비스에서 다음을 확인한다.

1. 직접 invoke API와 이벤트 resume Worker entrypoint가 실제 이미지에 있다.
2. 최초 요청/승인이 기존 START/RESUME 큐에 중복 발행되지 않는다.
3. Secret/PVC/Service DNS와 두 health endpoint를 준비했다.
4. DB migration과 checkpoint setup이 완료되었다.
5. 별도 프로세스의 동시 실행/재시작 및 lease 상실 테스트를 통과했다.
6. 리포트·실패·취소까지 세션 잠금/화면 이력 복원 정책을 연결했다.

이 저장소에서는 템플릿의 YAML/구조를 검사한다. 인수 서비스 이미지 빌드,
클러스터 schema/admission 검증 및 실제 배포 검증은 별도로 수행해야 한다.
