# 소스가 정식 패키지로 이동했습니다

공통 워커의 유일한 원본은 이제 [`src/worker`](../src/worker/)입니다.
이 디렉터리에는 별도 실행 코드·의존성·테스트 복사본을 유지하지 않습니다.
기존 로컬 .venv/캐시가 남아 있더라도 현재 실행 환경으로 사용하지 마세요.

- [현재 워커 사용·검증 안내](../docs/worker/README.md)
- [전체 전환 계획과 진행 상태](../docs/worker-centered-refactor.md)
- [Agent 연결 가이드](../docs/worker/agent-integration.md)

루트 uv 환경과 `tests/worker`, `worker_migrations`, `deploy/worker`를 사용합니다.
기존 독립 폴더 상태는 Git 보존 커밋 `391a818`에서 확인할 수 있습니다.
