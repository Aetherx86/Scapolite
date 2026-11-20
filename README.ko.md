# Scapolite: EVM 멤풀 트랜잭션 의존성 DAG 스케줄러

[English](README.md) · [라이선스](LICENSE)

Scapolite는 외부 의존성 없이 순수 Go 표준 라이브러리만으로 구현된 고성능 동시성 EVM 트랜잭션 스케줄러입니다. 대기 중인 멤풀 트랜잭션을 분석하고 상태 접근 풋프린트(컨트랙트 주소, 읽기 슬롯, 쓰기 슬롯)를 디코딩하여 방향성 비순환 그래프(DAG)를 구성한 후, 상호 충돌이 없는 병렬 실행 웨이브(Execution Wave)로 트랜잭션을 분할합니다.

`rusty-chain-kit` 및 차세대 병렬 EVM 블록 빌더를 위한 스케줄링 엔진으로 설계되었으며, 트랜잭션 실행 전에 상태 위험(RAW, WAR, WAW)을 선제적으로 해소하여 합의 분기(Consensus Divergence) 없이 3배에서 7배의 멀티코어 실행 가속을 제공합니다.

---

## 아키텍처 개요

```
대기 중인 멤풀 트랜잭션 (JSON / RPC)
        |  EVM 트랜잭션 (From, To, Nonce, ReadSlots, WriteSlots)
        v
+-----------------------------------------------------------+
| Scapolite 의존성 분석 파이프라인                          |
|                                                           |
| 1. 논스(Nonce) 시퀀스 정렬                                |
|    - 송신자 계정별 그룹화: Nonce(i) -> Nonce(i+1)         |
|                                                           |
| 2. 상태 해저드(Hazard) 감지 엔진                          |
|    - Fast 모드: 컨트랙트 및 수신자 주소 레벨 충돌 판별    |
|    - Deep 모드: 세부 32바이트 SSTORE / SLOAD 슬롯 추적    |
|    - RAW, WAR, WAW 메모리 의존성 엣지 생성                |
|                                                           |
| 3. 위상 정렬 웨이브 분할기 (Kahn's Algorithm)             |
|    - 진입 차수(In-degree) 0인 트랜잭션 즉시 병렬 디스패치 |
|    - 반복적 의존성 소거 (1,000건당 400µs 미만 처리)       |
|    - 순환 의존성(Cycle) 부재 정형 검증                    |
+-----------------------------------------------------------+
        |
        v
병렬 실행 스케줄 (Wave 1 || Wave 2 || ... || Wave N)
```

---

## 듀얼 운영 프로시저

### 1. 주소 레벨 고속 스케줄링 모드 (`--mode=fast`)
- **실행**: `scapolite -mode=fast -f mempool.json`
- **동작**: 컨트랙트 주소와 수신자 계정 전체를 단일 잠금으로 취급합니다. 두 트랜잭션이 동일한 컨트랙트와 상호작용하면 순차 웨이브로 배치됩니다.
- **지연시간**: 25개 트랜잭션 기준 26 마이크로초(µs) 미만.
- **적용 대상**: 스케줄링 오버헤드를 극도로 최소화해야 하는 고속 L2 시퀀서 및 단순 ERC-20 전송.

### 2. 스토리지 슬롯 정밀 스케줄링 모드 (`--mode=deep`)
- **실행**: `scapolite -mode=deep -f mempool.json`
- **동작**: 명시적인 32바이트 스토리지 슬롯(`SSTORE` 쓰기 및 `SLOAD` 읽기)을 추적합니다. 동일한 Uniswap 풀 컨트랙트를 호출하더라도 서로 다른 틱 범위나 분리된 잔액 슬롯을 건드리는 트랜잭션은 단일 웨이브에서 완전히 병렬 실행됩니다.
- **성능**: 순차 EVM 대비 최대 7.1배의 병렬 가속 달성.
- **적용 대상**: MEV 블록 빌더, 고빈도 DeFi 실행 엔진 및 멀티코어 밸리데이터 노드.

---

## 설치 및 빌드

```bash
git clone https://github.com/Aetherx86/Scapolite.git
cd Scapolite
go build -o bin/scapolite ./cmd/scapolite
```

## CLI 사용법

```bash
# 기본 딥 모드로 합성 멤풀 스케줄링 데모 실행
./bin/scapolite -mode=deep

# 500개 합성 멤풀 생성 및 JSON 스케줄링 출력
./bin/scapolite -gen 500 -out mempool_500.json
./bin/scapolite -f mempool_500.json -mode=deep -json
```

## 공급망 보안 검증

```bash
python3 .github/scripts/repo_guard.py . --strict
go test -v -race ./...
```

## 0.2.0 변경 사항

- `pkg/evm`: ERC-20 `transfer`/`transferFrom`/`approve`, 네이티브 전송, 컨트랙트 생성, EIP-2930 액세스 리스트, AMM/WETH 셀렉터 기반 상태 접근 추론
- 스토리지 슬롯, 잔액, 논스, 생성 키에 대한 충돌 DAG (RAW/WAR/WAW, 초과 인출 검사)
- 사이클 탐지, 가스 가중 임계 경로, 워커 수 기반 효율 추정
- 샌드위치, 우선 수수료 프런트러닝, 백러닝, 경합 핫스팟 MEV 탐지
- CLI: `-gen`, `-out`, `-bench`, `-seed`, `-json`

## 라이선스

MIT License. Copyright (c) 2026 Jeong Hoon Kim (Aether Systems & Research Lab).
