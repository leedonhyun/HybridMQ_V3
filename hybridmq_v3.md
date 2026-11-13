# HybridMQ V3

**HybridMQ V3**는 .NET 기반의 모듈형 고성능 분산 메시지 브로커입니다. 동일 프로세스 내 임베디드 실행부터 원격 TCP 기반 다중 노드 클러스터링, 다중 스토리지 백엔드(File, PostgreSQL, MongoDB), 그리고 Work Queue·Pub/Sub·Stream 메시징 패턴을 단일 아키텍처에서 지원합니다.

---

## 1. 솔루션 및 프로젝트 아키텍처

클린 아키텍처(Clean Architecture) 계층 구조로 설계되어 있으며, 도메인 로직과 인프라(스토리지·전송·클러스터)가 명확히 분리되어 있습니다.

```text
HybridMQ_V3/
├── src/
│   ├── 1.Domain/
│   │   └── HybridMQ.Domain/              # 핵심 도메인 모델, 포트 인터페이스, RPC 프로토콜(BrokerProtocol)
│   ├── 2.Application/
│   │   └── HybridMQ.Engine/              # 브로커 코어 엔진(BrokerEngine), 메모리 예산 관리, 파이프라인
│   ├── 3.Infrastructure/
│   │   ├── HybridMQ.Cluster/             # 클러스터 멤버십, 파티션 리더 리스, 투명 RPC 포워딩 라우터
│   │   ├── HybridMQ.Storage.File/        # 로컬/공유 파일 기반 세그먼트 로그 스토리지
│   │   ├── HybridMQ.Storage.PostgreSql/  # PostgreSQL 스토리지 백엔드
│   │   ├── HybridMQ.Storage.MongoDb/     # MongoDB 스토리지 백엔드
│   │   ├── HybridMQ.Transport.Local/     # In-Memory 및 Named Pipe 로컬 IPC 전송 계층
│   │   └── HybridMQ.Transport.Tcp/       # TCP 소켓 서버 및 원격 클라이언트 채널
│   └── 4.Presentation/
│       ├── HybridMQ.Client/              # Producer, Consumer, Admin 클라이언트 SDK
│       ├── HybridMQ.Hosting/             # 런타임 빌더 및 팩토리(HybridMqFactory, HybridMqOptions)
│       └── HybridMQ.Server/              # 독립 실행형 및 클러스터 브로커 서버 호스트 (CLI)
├── samples/
│   └── HybridMQ.Samples.ClusterServer/   # 클러스터 구성 샘플 프로젝트
└── tests/
    └── HybridMQ.Engine.Tests/            # 엔진 및 클러스터 장애 전환(Failover) 통합 테스트
```

---

## 2. 핵심 기능 및 클러스터 동작 원리

### 2.1 클러스터 코디네이션 (`HybridMQ.Cluster`)
* **멤버십 및 장애 감지 (`ClusterMembershipManager`)**
  * 시드 노드(`--seeds`)를 통해 클러스터에 자동 합류하며, 백그라운드 루프가 1초 주기로 `BrokerCommandType.Ping` 기반 Heartbeat를 교환해 피어 목록과 엔드포인트를 동기화합니다.
  * `HeartbeatTimeout`(기본 5초) 동안 응답이 없는 피어 노드는 `Dead` 상태로 전환되며 `OnPeerDied` 이벤트를 발생시킵니다.
* **파티션 리더 선출 및 Fencing (`PartitionLeaseCoordinator`)**
  * 스토리지 백엔드(`TryAcquireLeaseAsync`)를 통해 각 토픽·파티션의 리더 권한(Lease)과 `FencingToken`을 획득·갱신하여 Split-Brain을 방지합니다.
  * 피어 장애(`OnPeerDied`)가 감지되면 살아남은 노드가 즉시 `RebalancePartitionsAsync()`를 실행해 죽은 노드의 파티션 리더십을 인수하고 손상된 테일 로그를 복구합니다.
* **투명한 요청 포워딩 (`ClusterRpcRouter`)**
  * 클라이언트가 팔로워 노드에 `Publish`, `PollWorkQueue`, `Ack`, `Nack` 요청을 보내더라도, `ClusterRpcRouter`가 해당 파티션의 리더 노드(`lease.Owner`)를 조회해 내부 `TcpTransportChannel`로 요청을 투명하게 포워딩합니다.

### 2.2 다중 전송(Transport) 및 스토리지(Storage)
* **Transport 백엔드**: `EmbeddedDirect` (인프로세스 직접 호출), `LocalInMemory`, `LocalNamedPipe`, `Tcp`
* **Storage 백엔드**: `File` (고속 세그먼트 로그), `PostgreSql`, `MongoDb`
* **메시징 신뢰성**: `Direct`/`Topic`/`Fanout` Exchange 라우팅, 파티션 키(Key) 순서 보장(`OrderedByKey`), Visibility Timeout 및 재시도(`MaxAttempts`) 기반 Work Queue 처리.

---

## 3. 빌드 및 테스트

### 빌드
```powershell
dotnet build HybridMQ_V3.sln -c Release
```

### 전체 단위/통합 테스트 실행
```powershell
dotnet test HybridMQ_V3.sln
```

---

## 4. 서버 실행 가이드 (CLI)

`HybridMQ.Server.exe`는 단일(Standalone) 모드와 다중 노드 클러스터 모드를 모두 지원합니다.

### CLI 옵션 명세

| 옵션 | 설명 | 기본값 | 예시 |
| :--- | :--- | :--- | :--- |
| `--node` | 브로커 노드의 고유 식별자 (`NodeId`) | `broker-1` | `--node broker-1` |
| `--port` | TCP 리스너 수신 포트 번호 | `5679` | `--port 5679` |
| `--seeds` | 클러스터 합류를 위한 시드 노드 주소 (쉼표 구분) | 없음 (단독 기동) | `--seeds 127.0.0.1:5679` |
| `--storage` | 스토리지 백엔드 (`file`, `postgres`, `mongo`) | `file` | `--storage postgres` |
| `--target` | 파일 저장 디렉터리 또는 DB 연결 문자열 | `./data/{nodeId}` | `--target "Host=localhost;..."` |

---

### 시나리오 A: 단일(Standalone) 서버 실행
```powershell
.\HybridMQ.Server.exe --node broker-1 --port 5679
```

### 시나리오 B: 로컬 3노드 클러스터 실행 (File 백엔드)
로컬 머신에서 테스트할 때는 포트(`--port`)와 노드 ID(`--node`)를 반드시 다르게 지정해야 합니다.

```powershell
# 터미널 1: 시드 노드 1 기동
.\HybridMQ.Server.exe --node broker-1 --port 5679

# 터미널 2: 노드 2 기동 (노드 1을 시드로 합류)
.\HybridMQ.Server.exe --node broker-2 --port 5680 --seeds 127.0.0.1:5679

# 터미널 3: 노드 3 기동 (노드 1을 시드로 합류)
.\HybridMQ.Server.exe --node broker-3 --port 5681 --seeds 127.0.0.1:5679
```

정상 연결 시 콘솔에 멤버십 이벤트가 출력됩니다:
```text
Cluster Node [broker-2] Listening on 0.0.0.0:5680. Press Ctrl+C to stop.
[CLUSTER] Peer Joined: broker-1
```

### 시나리오 C: 공용 DB 기반 고가용성 클러스터 실행 (PostgreSQL / MongoDB)
여러 브로커 노드가 동일한 메시지 데이터를 공유하고 장애 시 인플라이트 메시지를 이어받으려면 공용 DB 백엔드를 사용합니다.

```powershell
# 노드 1
.\HybridMQ.Server.exe --node broker-1 --port 5679 --storage postgres --target "Host=localhost;Database=hybridmq;Username=postgres;Password=your_password"

# 노드 2
.\HybridMQ.Server.exe --node broker-2 --port 5680 --seeds 127.0.0.1:5679 --storage postgres --target "Host=localhost;Database=hybridmq;Username=postgres;Password=your_password"
```

---

## 5. SDK 사용 예제

### 5.1 임베디드 모드 (In-Process 실행)
별도 서버 프로세스 없이 애플리케이션 내부에 브로커를 내장하여 실행할 때 사용합니다.

```csharp
using System.Text.Json.Serialization;
using HybridMQ.Client;
using HybridMQ.Client.Abstractions;
using HybridMQ.Hosting;

await using var mq = await HybridMqFactory.CreateEmbeddedAsync(opts =>
{
    opts.StorageType = StorageBackendType.File;
    opts.DataDirectory = "./data/embedded";
    opts.TransportType = TransportBackendType.EmbeddedDirect;
});

await mq.Admin.CreateTopicAsync("events", partitions: 2);

await using var sub = await mq.Consumer.SubscribeAsync(
    "events",
    "default-group",
    new SubscribeOptions { PrefetchCount = 10 },
    async (ctx, msg) =>
    {
        Console.WriteLine($"[Consumed] {msg.Id}: {msg.Payload}");
        await ValueTask.CompletedTask;
    },
    AppJsonContext.Default.SampleMessage);

await mq.Producer.PublishAsync(
    "events",
    new SampleMessage("MSG-1", "Hello HybridMQ"),
    AppJsonContext.Default.SampleMessage);

public sealed record SampleMessage(string Id, string Payload);

[JsonSerializable(typeof(SampleMessage))]
internal partial class AppJsonContext : JsonSerializerContext;
```

### 5.2 원격 TCP 클러스터 접속 및 Failover
클러스터 노드의 엔드포인트 목록을 순회하며 활성화된 브로커 노드에 연결합니다. 팔로워 노드에 연결되더라도 `ClusterRpcRouter`가 파티션 리더로 자동 라우팅합니다.

```csharp
using System.Net;
using System.Text.Json.Serialization;
using HybridMQ.Client;
using HybridMQ.Client.Abstractions;
using HybridMQ.Domain.Primitives;
using HybridMQ.Hosting;

var clusterNodes = new[]
{
    (EndPoint: new IPEndPoint(IPAddress.Loopback, 5679), NodeId: new NodeId("broker-1")),
    (EndPoint: new IPEndPoint(IPAddress.Loopback, 5680), NodeId: new NodeId("broker-2")),
    (EndPoint: new IPEndPoint(IPAddress.Loopback, 5681), NodeId: new NodeId("broker-3"))
};

await using var client = await ConnectClusterAsync(clusterNodes);

// 1. 토픽 생성
const string topicName = "cluster-orders";
await client.Admin.CreateTopicAsync(topicName, partitions: 3);

// 2. 컨슈머 구독
await using var subscription = await client.Consumer.SubscribeAsync(
    topicName,
    "order-workers",
    new SubscribeOptions
    {
        PrefetchCount = 5,
        AckTimeout = TimeSpan.FromSeconds(10),
        MaxAttempts = 3
    },
    async (ctx, order) =>
    {
        Console.WriteLine($"[Consumed] Partition={ctx.Partition}, Offset={ctx.Offset}, OrderId={order.OrderId}");
        await ValueTask.CompletedTask;
    },
    AppJsonContext.Default.OrderCreatedEvent);

// 3. 메시지 발행 (Key 기준 파티션 라우팅)
await client.Producer.PublishAsync(
    topicName,
    new OrderCreatedEvent("ORD-2026-001", 25000m),
    AppJsonContext.Default.OrderCreatedEvent,
    key: "customer-1");

static async Task<IHybridMqInstance> ConnectClusterAsync(
    IEnumerable<(IPEndPoint EndPoint, NodeId NodeId)> nodes)
{
    foreach (var (ep, nodeId) in nodes)
    {
        try
        {
            var instance = HybridMqFactory.ConnectRemoteTcp(ep, nodeId);
            await instance.Admin.CreateTopicAsync("__healthcheck", partitions: 1);
            return instance;
        }
        catch
        {
            // 연결 실패 시 다음 브로커 노드로 전환
        }
    }
    throw new InvalidOperationException("클러스터에 연결 가능한 브로커 노드가 없습니다.");
}

public sealed record OrderCreatedEvent(string OrderId, decimal Amount);

[JsonSerializable(typeof(OrderCreatedEvent))]
internal partial class AppJsonContext : JsonSerializerContext;
```

---

## 6. 운영 및 트러블슈팅 가이드

* **포트 충돌 (`SocketException 10048`)**: 동일 호스트에서 다중 노드를 실행할 때는 반드시 `--port` 값을 노드마다 다르게 지정하세요.
* **리소스 정리 및 멱등성 (`ObjectDisposedException` 방지)**: `HybridMqRuntimeHost`, `ClusterMembershipManager`, `TcpBrokerServer`의 `DisposeAsync()`는 멱등(Idempotent)하게 설계되어 있으므로, 명시적 종료 후 `await using` 스코프 종료가 겹치더라도 안전하게 정리됩니다.
* **컨테이너 및 다중 서버 배포**: Docker 또는 별도 물리 서버 간 클러스터를 구성할 때는 `HybridMqOptions.AdvertisedEndpoint`에 `127.0.0.1` 대신 다른 노드가 접근 가능한 실제 IP 또는 서비스 호스트명(예: `10.0.1.10:5679`)을 지정해야 합니다.
* **CLI 인자 전달 규칙**: 컴파일된 `.exe` 파일을 직접 실행할 때는 `--` 구분자 없이 `--node broker-1` 형태로 인자를 전달합니다. (`dotnet run`을 사용할 때만 `dotnet run -- --node broker-1` 형식을 사용합니다.)