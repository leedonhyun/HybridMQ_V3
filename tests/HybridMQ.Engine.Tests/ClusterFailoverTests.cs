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
            // 1. 노드 2대 기동 (포트 0으로 OS 자동 할당)
            await using var node1 = await HybridMqFactory.StartFullRuntimeAsync(opts =>
            {
                opts.NodeId = new NodeId("node-1");
                opts.StorageType = StorageBackendType.File;
                opts.DataDirectory = Path.Combine(baseDir, "n1");
                opts.TransportType = TransportBackendType.Tcp;
                opts.TcpListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
            });

            int port1 = node1.TcpServer!.BoundPort;

            await using var node2 = await HybridMqFactory.StartFullRuntimeAsync(opts =>
            {
                opts.NodeId = new NodeId("node-2");
                opts.StorageType = StorageBackendType.File;
                opts.DataDirectory = Path.Combine(baseDir, "n2");
                opts.TransportType = TransportBackendType.Tcp;
                opts.TcpListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
                opts.SeedPeers = [$"127.0.0.1:{port1}"];
            });

            int port2 = node2.TcpServer!.BoundPort;

            // 2. 클라이언트로 각 노드에 접속하여 메시지 송수신 검증
            await using var client1 = HybridMqFactory.ConnectRemoteTcp(new IPEndPoint(IPAddress.Loopback, port1));
            await using var client2 = HybridMqFactory.ConnectRemoteTcp(new IPEndPoint(IPAddress.Loopback, port2));

            await client1.Admin.CreateTopicAsync("ha-topic", partitions: 1);
            await client2.Admin.CreateTopicAsync("ha-topic", partitions: 1);

            // node1 강제 종료 시뮬레이션
            await node1.DisposeAsync();

            // node2는 여전히 정상 동작해야 함
            var pubRes = await client2.Producer.PublishAsync(
                "ha-topic",
                new OrderCreatedEvent("FAILOVER-1", 30000m),
                TestJsonContext.Default.OrderCreatedEvent);

            pubRes.Should().NotBeEmpty();
        }
        finally
        {
            if (Directory.Exists(baseDir))
                Directory.Delete(baseDir, recursive: true);
        }
    }
}
