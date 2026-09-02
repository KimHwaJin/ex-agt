# BFF → Agent 요청 서명 계약

로컬 개발과 LangChain Agent Chat UI는 `BFF_AUTH_MODE=trusted_header`에서 기존
`X-User-ID`만 사용할 수 있다. `APP_ENV=production`은 `BFF_AUTH_MODE=hmac`이
아니면 시작하지 않는다. HMAC 모드에서는 다음 헤더가 모두 필요하다.

| 헤더 | 값 |
|---|---|
| `X-User-ID` | BFF가 인증한 최종 사용자 ID |
| `X-BFF-Signature-Version` | `v1` |
| `X-BFF-Key-ID` | 서명에 사용한 키 ID |
| `X-BFF-Timestamp` | Unix seconds |
| `X-BFF-Nonce` | HTTP 시도마다 새로 생성한 16~128자 값 |
| `X-BFF-Signature` | SHA-256 HMAC의 padding 없는 base64url |

## Canonical request

아래 8개 값을 LF 한 글자로 연결한 UTF-8 bytes가 HMAC 입력이다. 마지막 줄 뒤에는
LF가 없다.

```text
v1
{UPPERCASE_METHOD}
{RAW_PERCENT_ENCODED_PATH}
{RAW_QUERY_STRING}
{X-User-ID}
{X-BFF-Timestamp}
{X-BFF-Nonce}
{SHA256_HEX_OF_EXACT_BODY_BYTES}
```

query parameter의 순서와 percent encoding을 정규화하지 않는다. BFF가 서명한
path/query와 Agent가 실제로 받은 값이 정확히 같아야 한다. JSON은 먼저 bytes로
직렬화하고 그 동일한 bytes를 서명과 HTTP body에 함께 사용한다.

```python
import json
import time
from uuid import uuid4

from ex_agent.api.identity import sign_bff_request

body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
timestamp = int(time.time())
nonce = str(uuid4())
signature = sign_bff_request(
    secret,
    method="POST",
    raw_path="/api/v1/projects/p1/sessions/s1/tasks",
    raw_query="",
    user_id=user_id,
    timestamp=timestamp,
    nonce=nonce,
    body=body,
)
headers = {
    "Content-Type": "application/json",
    "X-User-ID": user_id,
    "X-BFF-Signature-Version": "v1",
    "X-BFF-Key-ID": key_id,
    "X-BFF-Timestamp": str(timestamp),
    "X-BFF-Nonce": nonce,
    "X-BFF-Signature": signature,
}
```

## 재시도와 replay 방지

Agent는 허용 시각 범위 안의 `(key ID, nonce)`를 Redis `SET NX EX`로 한 번만
수락한다. Redis가 응답하지 않으면 `503`으로 fail closed한다. 동일 HTTP 요청을
재시도할 때 업무 `idempotency_key`는 그대로 유지하고 timestamp, nonce, signature만
새로 만든다. body나 path를 바꾸면 새 signature가 필요하다.

기본 허용 오차는 ±300초다. BFF와 Agent node는 NTP를 사용해야 한다. nonce Redis
TTL은 허용 오차의 두 배보다 길다.

## 키 생성과 회전

`BFF_AUTH_HMAC_KEYS_JSON`은 key ID에서 padding 없는 base64url secret으로 가는 JSON
객체다. secret은 최소 32 bytes다. Kubernetes에서는 ConfigMap이 아니라 전용
`handoff-agent-bff-auth` Secret으로 API 컨테이너에만 주입한다. Worker는 서명을
검증하지 않으므로 이 Secret을 받을 필요가 없다. HMAC 모드의 API는 키 설정이
비었거나 잘못되면 시작 단계에서 실패한다.

1. Agent 설정에 기존 키와 신규 키를 함께 배포한다.
2. 모든 Agent API Pod가 두 키를 수락하는지 확인한다.
3. BFF의 서명 키 ID를 신규 키로 변경한다.
4. 최대 clock skew, HTTP retry와 rolling 배포 시간을 모두 지난 뒤 기존 키를
   Agent 설정에서 제거한다.

서명 키는 사용자 인증 토큰이 아니다. BFF가 먼저 사용자를 인증·인가한 뒤 해당
사용자 ID를 Agent 요청에 결속하는 service-to-service 자격 증명이다. 외부 ingress는
Agent API를 직접 공개하지 않고 BFF만 접근할 수 있도록 NetworkPolicy도 함께 둔다.
