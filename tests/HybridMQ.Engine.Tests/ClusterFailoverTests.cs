namespace HybridMQ.Engine.Tests;

using FluentAssertions;

using HybridMQ.Cluster;
using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Primitives;
using HybridMQ.Engine.Memory;
using HybridMQ.Engine.Pipelines;
using HybridMQ.Hosting;
using HybridMQ.Storage.File;

using System.Net;

using Xunit;

public sealed class ClusterFailoverTests
{
    [Fact]
    public async Task ClusterMembership_Detects_DeadPeer_And_Reclaims_InFlight_Messages()
    {
        string tempDir = Path.Combine(Path.GetTempPath(), $"hmq_cluster_{Guid.NewGuid():N}");
        try
        {
            var clock = new FakeTimeProvider(new DateTimeOffset(2026, 11, 13, 12, 0, 0, TimeSpan.Zero));
            var nodeA = new NodeId("broker-node-a");
            var nodeB = new NodeId("broker-node-b");

            await using var membershipA = new ClusterMembershipManager(nodeA, "127.0.0.1:5679", TimeSpan.FromSeconds(5), clock);
            await using var storage = new FileStorageProvider(tempDir);
            await using var engineA = new BrokerEngine(nodeA, storage, new MemoryBudgetManager(ResourceProfile.Low), membershipA, timeProvider: clock);

            await engineA.StartAsync();
            membershipA.RecordHeartbeat(nodeB, "127.0.0.1:5680");

            // --- 여기부터 수정 ---
            var deadSignal = new TaskCompletionSource<NodeId>(TaskCreationOptions.RunContinuationsAsynchronously);
            membershipA.OnPeerDied += n => deadSignal.TrySetResult(n);

            // Advance clock past heartbeat timeout (5s)
            clock.Advance(TimeSpan.FromSeconds(6));
            membershipA.EvaluatePeerHealth();

            var deadNode = await deadSignal.Task.WaitAsync(TimeSpan.FromSeconds(2));
            deadNode.Should().Be(nodeB);
            // --- 여기까지 수정 ---
        }
        finally
        {
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, recursive: true);
        }
    }
    [Fact]
    public async Task PartitionLeaseCoordinator_Acquires_And_Releases_Partition_Leadership()
    {
        string tempDir = Path.Combine(Path.GetTempPath(), $"hmq_lease_{Guid.NewGuid():N}");
        try
        {
            var nodeA = new NodeId("broker-node-a");
            await using var storage = new FileStorageProvider(tempDir);
            await using var engine = new BrokerEngine(nodeA, storage, new MemoryBudgetManager(ResourceProfile.Low));
            await engine.StartAsync();
            await engine.CreateTopicAsync("clustered-orders", partitions: 2);

            await using var coordinator = new PartitionLeaseCoordinator(nodeA, storage, engine, TimeSpan.FromSeconds(10));
            await coordinator.RebalancePartitionsAsync();

            engine.IsPartitionLeader(new PartitionId(new TopicName("clustered-orders"), 0)).Should().BeTrue();
            engine.IsPartitionLeader(new PartitionId(new TopicName("clustered-orders"), 1)).Should().BeTrue();
        }
        finally
        {
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, recursive: true);
        }
    }
    [Fact]
    public async Task MultiNode_Cluster_Continues_Operating_When_One_Node_Fails()
    {
        string baseDir = Path.Combine(Path.GetTempPath(), $"hmq_cluster_{Guid.NewGuid():N}");
        try
        {
            // 공유 스토리지 경로 (공용 DB 대신 로컬 파일로 멀티 노드 리스/리더십 테스트 시 동일 경로 사용)
            string sharedDataDir = Path.Combine(baseDir, "shared-cluster-data");

            var node2JoinedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var node1DiedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            // 1. 노드 1 기동 (짧은 HeartbeatTimeout / LeaseTtl 설정으로 빠른 장애 전환 검증)
            await using var node1 = await HybridMqFactory.StartFullRuntimeAsync(opts =>
            {
                opts.NodeId = new NodeId("node-1");
                opts.StorageType = StorageBackendType.File;
                opts.DataDirectory = sharedDataDir;
                opts.TransportType = TransportBackendType.Tcp;
                opts.TcpListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
                opts.HeartbeatTimeout = TimeSpan.FromMilliseconds(900);
                opts.PartitionLeaseTtl = TimeSpan.FromSeconds(1);
            });

            int port1 = node1.TcpServer!.BoundPort;

            // 2. 노드 2 기동 (노드 1을 시드로 클러스터 합류)
            await using var node2 = await HybridMqFactory.StartFullRuntimeAsync(opts =>
            {
                opts.NodeId = new NodeId("node-2");
                opts.StorageType = StorageBackendType.File;
                opts.DataDirectory = sharedDataDir;
                opts.TransportType = TransportBackendType.Tcp;
                opts.TcpListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
                opts.SeedPeers = [$"127.0.0.1:{port1}"];
                opts.HeartbeatTimeout = TimeSpan.FromMilliseconds(900);
                opts.PartitionLeaseTtl = TimeSpan.FromSeconds(1);
            });

            int port2 = node2.TcpServer!.BoundPort;

            // 멤버십 이벤트 구독
            node2.Membership.OnPeerJoined += peer =>
            {
                if (peer == new NodeId("node-1")) node2JoinedTcs.TrySetResult();
            };
            node2.Membership.OnPeerDied += peer =>
            {
                if (peer == new NodeId("node-1")) node1DiedTcs.TrySetResult();
            };

            // 3. 클러스터 상호 인식(Gossip Ping) 완료 대기
            await node2JoinedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // 4. 각 노드에 클라이언트 연결 (targetNodeId를 실제 노드 ID로 지정)
            await using var client1 = HybridMqFactory.ConnectRemoteTcp(
                new IPEndPoint(IPAddress.Loopback, port1), new NodeId("node-1"));
            await using var client2 = HybridMqFactory.ConnectRemoteTcp(
                new IPEndPoint(IPAddress.Loopback, port2), new NodeId("node-2"));

            // node1을 통해 토픽 생성 -> node1이 파티션 0의 리더 리스 획득
            await client1.Admin.CreateTopicAsync("ha-topic", partitions: 1);

            // 팔로워(node2)로 발행 요청을 보내도 ClusterRpcRouter가 리더(node1)로 투명 포워딩하여 성공해야 함
            var forwardedRes = await client2.Producer.PublishAsync(
                "ha-topic",
                new OrderCreatedEvent("BEFORE-FAILOVER-1", 15000m),
                TestJsonContext.Default.OrderCreatedEvent);
            forwardedRes.Should().NotBeEmpty();

            // 5. node1 종료 (DisposeAsync 시 리스 반납 및 Heartbeat 중단)
            await node1.DisposeAsync();

            // node2가 node1의 장애(OnPeerDied)를 감지하고 리밸런싱할 때까지 대기
            await node1DiedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // 6. node2가 리더십을 인수한 후 단독으로 메시지 발행 및 소비가 정상 동작해야 함
            var failoverRes = await client2.Producer.PublishAsync(
                "ha-topic",
                new OrderCreatedEvent("AFTER-FAILOVER-1", 30000m),
                TestJsonContext.Default.OrderCreatedEvent);

            failoverRes.Should().NotBeEmpty();
        }
        finally
        {
            if (Directory.Exists(baseDir))
                Directory.Delete(baseDir, recursive: true);
        }
    }
}
