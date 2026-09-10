#!/bin/sh
set -u

# 이식 대상 프로젝트 루트에 복사해서 사용하는 테스트용 스크립트다.
# API 실행 경로는 수령자의 실제 모듈 경로로 바꾼다.
# 장애 감시, 자동 재시작, 종료 대기 제한은 제공하지 않는다.

# 이 스크립트가 있는 폴더를 프로젝트 루트로 사용
PROJECT_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || exit 1

# 작업 폴더가 달라져도 동일한 Python을 사용
PYTHON_BIN=$(python -c 'import sys; print(sys.executable)') || exit 1

API_PID=""
WORKER_PID=""

# 종료 요청 시 두 프로세스에 SIGTERM 전달 후 대기
stop_apps() {
    trap '' TERM INT

    for pid in "$API_PID" "$WORKER_PID"; do
        if [ -n "$pid" ]; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done

    for pid in "$API_PID" "$WORKER_PID"; do
        if [ -n "$pid" ]; then
            wait "$pid" 2>/dev/null || true
        fi
    done

    exit 0
}

trap stop_apps TERM INT

# API: 프로젝트 루트에서 실행
(
    cd "$PROJECT_ROOT" || exit 1

    # 현재 정상 동작하는 API 모듈 경로로 변경
    exec "$PYTHON_BIN" -m uvicorn src.app.main:app \
        --host 0.0.0.0 \
        --port 8010
) &
API_PID=$!

# Worker: 프로젝트의 src 폴더에서 실행
(
    cd "$PROJECT_ROOT/src" || exit 1

    exec "$PYTHON_BIN" -m app.agent_worker.worker_main
) &
WORKER_PID=$!

echo "API PID: $API_PID"
echo "Worker PID: $WORKER_PID"

# 테스트용: 두 프로세스가 종료될 때까지 대기
wait "$API_PID"
wait "$WORKER_PID"
