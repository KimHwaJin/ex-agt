# Kubernetes 운영 배포

이 디렉터리가 현재 ex-agent의 정식 Kubernetes 실행 계약이다. 루트
`Dockerfile`로 빌드한 동일 image digest를 Migration Job과 같은 Pod의 API·Worker
컨테이너에 사용한다. 파일은 환경별 이름을 치환해야 하는 `.example`이며 저장소에서
클러스터에 직접 적용하지 않는다.

```text
Migration Job ─ ex-agent-migrate (rollout 전에 완료)

Deployment: ex-agent
└─ Pod
   ├─ api-agent ─ ex-agent-api    ─ 8010
   └─ worker    ─ ex-agent-worker ─ 8011 health/metrics only

Service: ex-agent-api:8010 → api-agent
```

## 이미지 계약

```bash
docker build --target runtime \
  --tag registry.example.com/ex-agent:replace-with-version .
docker push registry.example.com/ex-agent:replace-with-version
```

이미지의 ENTRYPOINT는 `tini`이고 기본 CMD는 `ex-agent-api`다. Deployment는
`command`를 설정하지 않고 `args`만 `ex-agent-api` 또는 `ex-agent-worker`로
지정한다. 그래야 두 컨테이너 모두 동일한 signal 전달·zombie reap 계약을 사용한다.

## 환경별 필수 치환

다음 값을 실제 환경에 맞춘다.

1. 세 manifest의 `registry.example.com/ex-agent:replace-with-version`을 동일한
   검증 image digest 또는 immutable tag로 교체한다.
2. `EXECUTOR_BASE_URL`을 Kubernetes Service DNS로 교체한다.
3. `replace-with-executor-shared-pvc`를 Executor와 같은 파일을 볼 수 있는 PVC로
   교체한다. `PATH` 제출이므로 두 컨테이너와 Executor의 상대경로 기준이 같아야
   한다.
4. Migration Job 이름의 `replace-with-release`를 매 배포마다 고유한 소문자
   release ID로 교체한다.
5. `model.frodo.com`이 cluster DNS에서 해석되는지 확인한다. Dockerfile의
   `/etc/hosts`를 수정하는 방식에 의존하지 않는다.

`ex-agent-runtime` Secret에는 다음 key가 필요하다.

| key | 형식 |
|---|---|
| `AGENT_DATABASE_URL` | SQLAlchemy psycopg URL |
| `AGENT_CHECKPOINT_DATABASE_URL` | psycopg URL |
| `AGENT_REDIS_URL` | Redis URL |
| `AGENT_MODEL_API_KEY` | 모델 API key, 내부 vLLM은 `EMPTY` 가능 |
| `AGENT_EMBEDDING_API_KEY` | embedding API key, dummy는 `EMPTY` 가능 |

`ex-agent-bff-auth` Secret의 `keys-json`에는 key ID에서 padding 없는 base64url
32-byte 이상 secret으로 가는 JSON 객체를 넣는다. 이 Secret은 API 컨테이너에만
주입한다. 값은 Git이나 ConfigMap에 저장하지 않는다.

## 배포 순서

1. ConfigMap과 두 Secret을 준비한다.
2. release별 Migration Job을 적용한다.
3. Job이 `Complete`인지 확인한다. 실패하면 Deployment를 갱신하지 않는다.
4. Deployment와 Service를 적용한다.
5. API와 Worker 두 컨테이너가 모두 Ready인지 확인한다.

```bash
kubectl apply -f deploy/k8s/configmap.yaml.example
kubectl apply -f deploy/k8s/migrate-job.yaml.example
kubectl wait --for=condition=complete \
  job/ex-agent-migrate-replace-with-release --timeout=10m
kubectl apply -f deploy/k8s/deployment.yaml.example
kubectl apply -f deploy/k8s/service.yaml.example
```

Migration은 Agent Alembic, `ew_*` Worker Alembic과 LangGraph checkpoint setup을
모두 수행한다. 애플리케이션 시작 시 자동 DDL을 실행하지 않는다.

## 변경할 수 없는 Deployment에 적용할 때

플랫폼 manifest와 이 계약을 비교해 다음 항목이 이미 충족되는지 확인한다.

- 한 Pod에 컨테이너 두 개를 둘 수 있고 두 번째 컨테이너 args가
  `ex-agent-worker`인지
- API/Worker가 같은 Secret, ConfigMap, PVC와 image digest를 사용하는지
- API만 BFF HMAC Secret을 받는지
- API 8010과 Worker 8011 probe를 각각 설정할 수 있는지
- Pod 종료 유예가 Worker drain 25초보다 긴지
- rollout 전에 별도 Migration Job이나 동등한 release hook을 실행할 수 있는지

이미지 한 개와 컨테이너 한 개만 허용하면 이 manifest로는 현재 구조를 실행할 수
없다. 그 환경은 API lifespan에 Worker 전체를 포함하는 별도 통합 실행 모드가
필요하다.
