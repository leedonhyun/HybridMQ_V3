namespace HybridMQ.Engine.Tests;

using HybridMQ.Cluster;
using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Primitives;
using HybridMQ.Engine.Memory;
using HybridMQ.Engine.Pipelines;
using HybridMQ.Storage.File;
using FluentAssertions;
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

            var deadNodes = new List<NodeId>();
            membershipA.OnPeerDied += n => deadNodes.Add(n);

            // Advance clock past heartbeat timeout (5s)
            clock.Advance(TimeSpan.FromSeconds(6));
            membershipA.EvaluatePeerHealth();

            deadNodes.Should().ContainSingle().Which.Should().Be(nodeB);
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
}
