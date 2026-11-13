namespace HybridMQ.Engine.Tests;

using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Primitives;
using HybridMQ.Hosting;
using HybridMQ.Storage.File;
using FluentAssertions;
using Xunit;

public sealed class ToolsCliTests
{
    [Fact]
    public async Task Corrupt_Segment_Tail_Can_Be_Recovered_And_Truncated_Cleanly()
    {
        string tempDir = Path.Combine(Path.GetTempPath(), $"hmq_tools_test_{Guid.NewGuid():N}");
        try
        {
            await using (var mq = await HybridMqFactory.CreateEmbeddedAsync(o =>
            {
                o.DataDirectory = tempDir;
                o.Profile = ResourceProfile.Low;
            }))
            {
                await mq.Admin.CreateTopicAsync("wal-check", partitions: 1);
                await mq.Producer.PublishAsync("wal-check", "valid-message-1"u8.ToArray());
                await mq.Producer.PublishAsync("wal-check", "valid-message-2"u8.ToArray());
            }

            string segFile = Directory.GetFiles(tempDir, "*.log", SearchOption.AllDirectories).Single();
            long cleanLen = new FileInfo(segFile).Length;
            await File.AppendAllBytesAsync(segFile, new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02 });

            await using var storage = new FileStorageProvider(tempDir);
            await storage.InitializeAsync();
            int truncated = await storage.RecoverAndTruncateCorruptTailAsync(new PartitionId(new TopicName("wal-check"), 0));

            truncated.Should().Be(6);
            new FileInfo(segFile).Length.Should().Be(cleanLen);
        }
        finally
        {
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, recursive: true);
        }
    }
}
