# Kubernetes Worker restart E2E

이 디렉터리는 운영 배포 manifest가 아니라, 동일 버전 Worker의 정상 rolling
restart와 강제 Pod 종료 복구를 실제 Executor/Jupyter에 연결해 검증하는 전용
환경이다. 기존 `kind-kind` 클러스터와 Agent DB/Stream은 건드리지 않는다.

## 실행

Executor Compose와 모델이 실행 중인 상태에서 저장소 루트에서 실행한다.

```bash
UV_CACHE_DIR=/tmp/ex-agent-uv-cache \
uv run --no-sync python scripts/live_k8s_worker_restart_e2e.py \
  --executor-shared-directory \
  /Users/a10054/SKAX_PROJECT/executor/shared_dir \
  --output /tmp/ex-agent-k8s-rolling-e2e.json
```

스크립트는 다음을 자동으로 수행한다.

1. `/workspace/shared` extra mount와 API용 `18010` port mapping이 있는
   `ex-agent-rolling-e2e` kind 클러스터를 생성한다.
2. 현재 Git 작업 트리를 `ex-agent:rolling-e2e` 이미지로 빌드해 kind에 넣는다.
3. Executor PostgreSQL에 고유한 Agent 테스트 DB를 만들고 migration Job을
   완료한다.
4. Redis stream과 consumer group을 실행별 이름으로 격리한다. 공유
   `executor.events` group은 현재 시점의 `$`에서 시작하므로 기존 이벤트를
   다시 소비하지 않는다.
5. 계획 생성 중 `rollout restart`를 수행해 SIGTERM drain과 새 Pod 인계를
   확인한다.
6. Executor 실행 중 유일한 Worker Pod를 `--grace-period=0 --force`로 삭제해
   새 UID의 Pod가 PostgreSQL checkpoint와 inbox/outbox 상태로 같은 Task를
   완료하는지 확인한다.

성공 결과에는 재시작 전후 Pod UID, 동일 `task_id`와 `execution_id`, Session
잠금 상태, 후속 Task 성공 상태가 기록된다. 이어서 Redis pending 0건, Agent와
Worker binding, 완료 event 1건, `session_id` 기반 LangGraph checkpoint, 비어 있는
Session lock, 완전히 drain된 Worker command/outbox를 DB에서 재검증한다. 스크립트는
장애 분석을 위해 전용 클러스터와 DB를 자동 삭제하지 않는다.

같은 클러스터를 재사용할 때는 mount가 동일한지 확인한 후
`--reuse-cluster`를 추가한다. `run_id`는 매번 자동 생성되므로 Namespace, DB와
consumer group은 새로 만들어진다.

중단된 같은 실행 환경을 이어서 검증할 때만 기존 출력의 `run_id`와
`--reuse-cluster --reuse-run`을 함께 사용한다. 이 옵션은 해당 전용 DB와 Redis
group이 이미 생성됐다고 보고 재사용하며, migration Job은 정확한 Namespace
안에서 다시 만든다.

## 정리

결과를 보존한 뒤 출력 JSON의 정확한 Namespace와 DB 이름을 사용한다.

```bash
kubectl --context kind-ex-agent-rolling-e2e delete namespace \
  ex-agent-rolling-e2e-<run-id>
docker exec executor-postgres-1 psql -U executor -d executor \
  -c 'DROP DATABASE "agent_roll_e2e_<run-id>" WITH (FORCE)'
docker exec executor-redis-1 redis-cli XGROUP DESTROY executor.events \
  agent-roll-e2e-<run-id>
kind delete cluster --name ex-agent-rolling-e2e
```

정리 명령은 `<run-id>`를 실제 출력값으로 치환한 뒤 실행한다. 다른 DB,
Namespace, consumer group을 포괄하는 wildcard 정리는 사용하지 않는다.
