# Kubernetes legacy-to-integrated Worker cutover E2E

이 환경은 운영 manifest가 아니라 실제 Git의 레거시 Worker와 현재 통합 Worker를
서로 다른 이미지로 빌드해 **동시 실행 없이** 전환하는 리허설 전용이다.

기본 레거시 기준은 통합 직전 snapshot `391a818`이다. 스크립트는 다음 순서를
자동화한다.

1. 격리 kind cluster, Namespace, Agent DB, Executor consumer group 생성
2. `391a818` archive로 `ex-agent:cutover-legacy` 이미지 빌드
3. 현재 작업 트리로 `ex-agent:cutover-target` 이미지 빌드
4. 레거시 migration/API/Worker 기동 및 Q&A smoke Task 성공
5. BFF 신규 START 차단을 가정하고 preflight가 두 번 안정적인 drain을 확인
6. 레거시 API/Worker를 0개로 축소하고 preflight 재확인
7. 새 migration 후 통합 Worker와 API를 기동
8. 실제 Executor 코드 실행 smoke Task 성공 확인

Deployment strategy는 의도적으로 `Recreate`, 초기 replica는 0이다. 스크립트만이
migration과 preflight 경계에 맞춰 replica를 올린다. RollingUpdate로 바꾸면 이
리허설의 안전 조건이 훼손된다.

## 실행

Executor Compose와 모델이 실행 중인 상태에서 저장소 루트에서 실행한다.

```bash
UV_CACHE_DIR=/tmp/ex-agent-uv-cache \
uv run --no-sync python -m scripts.live_k8s_worker_cutover_e2e \
  --executor-shared-directory \
  /Users/a10054/SKAX_PROJECT/executor/shared_dir \
  --output /tmp/ex-agent-k8s-cutover-e2e.json
```

기존 `ex-agent-rolling-e2e` cluster가 남아 있어도 충돌하지 않도록 별도 cluster와
host port `18011`, NodePort `30011`을 사용한다. 테스트 환경은 장애 분석을 위해
자동 삭제하지 않는다.

## 한계

이 리허설은 `--unsafe-accept-operator-freeze-assertion`으로 실제 BFF 조회를
의도적으로 생략한다. 리허설 중 신규 요청을 만들지 않음으로써 차단된 상태를
재현한다. 운영에서는 BFF의 상관된 freeze receipt를 preflight가 직접 확인해야
한다. 이 테스트는 활성 레거시 Task의 checkpoint를 새 graph로 변환하지 않는다.
모든 레거시 Task가 terminal 상태가 된 뒤에만 전환한다.

## 실제 검증 기록

2026-09-02에 `391a818`과 현재 통합 이미지를 사용한 kind 리허설을 두 번
수행했다. 두 번째 강화 검증의 식별자는 다음과 같다.

- Namespace: `ex-agent-cutover-e2e-6a0c59b6`
- 레거시 Task: `3a9e07b6-962a-4e39-b158-02fb69e53f54`
- 통합 Task: `5464c880-5d47-4a8d-a2ec-cf6a28d9de45`
- Execution: `771a8565-b48d-4eae-88ca-99511771a455`

레거시 Q&A Task가 성공한 뒤 preflight의 모든 DB backlog, Session lock,
두 consumer group pending/lag가 두 표본에서 0이었다. 레거시 API/Worker Pod가
실제로 0개가 된 뒤 같은 검사를 다시 통과했으며, 그 후에만 새 migration과 통합
Pod 기동을 수행했다. 통합 코드 실행은 성공했고 Session checkpoint 25개,
Agent binding 1개, Worker binding sequence 6, 완료 event 1개를 확인했다.
최종 pending, 미완료 Worker command, 미전송 outbox, Session lock은 모두 0이었다.

결과 원본은 로컬 `/tmp/ex-agent-k8s-cutover-e2e-2.json`에 기록했다. 리허설
Namespace와 DB/group은 장애 분석을 위해 자동 정리하지 않았다.

## 정리

결과 JSON의 정확한 식별자를 사용한다.

```bash
kubectl --context kind-ex-agent-cutover-e2e delete namespace \
  ex-agent-cutover-e2e-<run-id>
docker exec executor-postgres-1 psql -U executor -d executor \
  -c 'DROP DATABASE "agent_cutover_e2e_<run-id>" WITH (FORCE)'
docker exec executor-redis-1 redis-cli XGROUP DESTROY executor.events \
  agent-cutover-e2e-<run-id>
kind delete cluster --name ex-agent-cutover-e2e
```

`<run-id>`를 실제 값으로 치환한다. wildcard로 다른 리소스까지 삭제하지 않는다.
