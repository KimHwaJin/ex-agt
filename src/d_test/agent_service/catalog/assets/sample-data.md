# sample-data · 0.1.0

실제 데이터 레이크가 아닌 개발용 합성 매출 데이터를 준비한다.
`fetch_sample_data`는 다운로드 함수의 임시 구현이며 네트워크 요청,
SQL 실행, 실제 데이터 접근을 하지 않는다. query는 추적용 설명이다.
행은 date, category, revenue 열을 갖는 사전이고 결과는 행 목록이다.
실제 데이터가 필요한 요청에 합성 데이터를 실제 데이터라고 제시하지 않는다.

사용 Tool: fetch_sample_data@0.1.0
