namespace HybridMQ.Chaos.Tests;

using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Messaging;
using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Subscriptions;
using HybridMQ.Domain.Topics;
using HybridMQ.Engine.Memory;
using HybridMQ.Engine.Pipelines;
using HybridMQ.Storage.File;
using FluentAssertions;
using Xunit;

public sealed class TornWriteAndConcurrencyTests
{
    [Fact]
    public async Task Corrupt_Tail_Bytes_Should_Be_Truncated_Without_Losing_Confirmed_Records()
    {
        string tempDir = Path.Combine(Path.GetTempPath(), $"hmq_chaos_{Guid.NewGuid():N}");
        try
        {
            await using var storage = new FileStorageProvider(tempDir);
            await storage.InitializeAsync();
            await storage.CreateTopicAsync(new TopicMetadata(new TopicName("events"), 1, new RetentionPolicy(), 0));

            var pid = new PartitionId(new TopicName("events"), 0);
            var rec = new Record(Offset.Invalid, 1000, 1, 1, RecordFlags.None, ReadOnlyMemory<byte>.Empty, Array.Empty<MessageHeader>(), new byte[] { 1, 2, 3, 4 });
            await storage.AppendAsync(pid, new[] { rec }, Durability.DiskSync);

            string segPath = Path.Combine(tempDir, "topics", "events", "0", "00000000000000000000.log");
            await using (var fs = new FileStream(segPath, FileMode.Append, FileAccess.Write))
            {
                await fs.WriteAsync(new byte[] { 0xFF, 0x00, 0xAB, 0xCD, 0x99 });
            }

            int truncated = await storage.RecoverAndTruncateCorruptTailAsync(pid);
            truncated.Should().Be(5);

            var recovered = new List<Record>();
            await foreach (var r in storage.ReadAsync(pid, Offset.Earliest, 10))
                recovered.Add(r);

            recovered.Should().HaveCount(1);
            recovered[0].Payload.ToArray().Should().BeEquivalentTo(new byte[] { 1, 2, 3, 4 });
        }
        finally
        {
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task State_Persistence_Survives_Broker_Restart()
    {
        string tempDir = Path.Combine(Path.GetTempPath(), $"hmq_restart_{Guid.NewGuid():N}");
        try
        {
            var subId = new SubscriptionId("persist-group", new TopicName("orders"), 0);

            // Session 1: Publish 3, Poll 2, Ack 1
            await using (var storage1 = new FileStorageProvider(tempDir))
            await using (var engine1 = new BrokerEngine(NodeId.NewId(), storage1, new MemoryBudgetManager(ResourceProfile.Low)))
            {
                await engine1.StartAsync();
                await engine1.CreateTopicAsync("orders", 1);

                for (int i = 0; i < 3; i++)
                {
                    await engine1.PublishAsync(new PublishEnvelope(new TopicName("orders"), null, null, ReadOnlyMemory<byte>.Empty, new byte[] { (byte)i }));
                }

                var polled = await engine1.PollWorkQueueAsync(subId, 2, TimeSpan.FromMinutes(5));
                polled.Should().HaveCount(2);
                await engine1.AckAsync(subId, polled[0].Offset); // Ack offset 0, leave offset 1 in-flight
            }

            // Session 2: Restart engine, verify commit offset = 1 and state log replay
            await using (var storage2 = new FileStorageProvider(tempDir))
            await using (var engine2 = new BrokerEngine(NodeId.NewId(), storage2, new MemoryBudgetManager(ResourceProfile.Low)))
            {
                await engine2.StartAsync();
                var reloadedState = await engine2.GetOrLoadSubscriptionAsync(subId);
                reloadedState.CommitOffset.Value.Should().Be(1);
                reloadedState.InFlight.Should().ContainKey(1);
            }
        }
        finally
        {
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task Concurrent_Publishers_Maintain_Contiguous_Offsets_Without_DataLoss()
    {
        string tempDir = Path.Combine(Path.GetTempPath(), $"hmq_conc_{Guid.NewGuid():N}");
        try
        {
            await using var storage = new FileStorageProvider(tempDir);
            await using var engine = new BrokerEngine(NodeId.NewId(), storage, new MemoryBudgetManager(ResourceProfile.Standard));
            await engine.StartAsync();
            await engine.CreateTopicAsync("high-throughput", 1);

            const int publisherCount = 8;
            const int messagesPerPublisher = 25;

            var tasks = Enumerable.Range(0, publisherCount).Select(p => Task.Run(async () =>
            {
                for (int m = 0; m < messagesPerPublisher; m++)
                {
                    await engine.PublishAsync(new PublishEnvelope(
                        new TopicName("high-throughput"),
                        null,
                        null,
                        ReadOnlyMemory<byte>.Empty,
                        new byte[] { (byte)p, (byte)m },
                        Durability: Durability.Disk));
                }
            }));

            await Task.WhenAll(tasks);

            var pid = new PartitionId(new TopicName("high-throughput"), 0);
            var hwm = await storage.GetHighWatermarkAsync(pid);
            hwm.Value.Should().Be(publisherCount * messagesPerPublisher);

            var all = new List<Record>();
            await foreach (var rec in storage.ReadAsync(pid, Offset.Earliest, 500))
                all.Add(rec);

            all.Select(r => r.Offset.Value).Should().BeEquivalentTo(Enumerable.Range(0, 200).Select(i => (long)i));
        }
        finally
        {
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, recursive: true);
        }
    }
}
