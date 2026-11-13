namespace HybridMQ.Engine.Tests;

using System.Net;
using HybridMQ.Client.Abstractions;
using HybridMQ.Domain.Cluster;
using HybridMQ.Domain.Primitives;
using HybridMQ.Hosting;
using FluentAssertions;
using Xunit;

public sealed class TransportEndToEndTests
{
    [Theory]
    [InlineData(TransportBackendType.LocalInMemory)]
    [InlineData(TransportBackendType.LocalNamedPipe)]
    [InlineData(TransportBackendType.Tcp)]
    public async Task All_Transport_Modes_Support_Full_Publish_And_Consume_Workflow(TransportBackendType transportType)
    {
        string tempDir = Path.Combine(Path.GetTempPath(), $"hmq_trans_{transportType}_{Guid.NewGuid():N}");
        try
        {
            await using var host = await HybridMqFactory.StartFullRuntimeAsync(opts =>
            {
                opts.StorageType = StorageBackendType.File;
                opts.DataDirectory = tempDir;
                opts.TransportType = transportType;
                opts.LocalPipePrefix = $"hmq_test_{Guid.NewGuid():N}";
                opts.TcpListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
                opts.Profile = ResourceProfile.Low;
            });

            await host.Client.Admin.CreateTopicAsync("remote-orders", partitions: 1);

            var received = new List<OrderCreatedEvent>();
            var tcs = new TaskCompletionSource();

            await using var sub = await host.Client.Consumer.SubscribeAsync(
                "remote-orders",
                "remote-group",
                new SubscribeOptions { PrefetchCount = 5 },
                async (ctx, evt) =>
                {
                    received.Add(evt);
                    if (received.Count == 2) tcs.TrySetResult();
                    await ValueTask.CompletedTask;
                },
                TestJsonContext.Default.OrderCreatedEvent);

            // If TCP mode, also test connecting an independent external TCP client
            if (transportType == TransportBackendType.Tcp)
            {
                await using var externalClient = HybridMqFactory.ConnectRemoteTcp(
                    new IPEndPoint(IPAddress.Loopback, host.TcpServer!.BoundPort));

                await externalClient.Producer.PublishAsync("remote-orders", new OrderCreatedEvent("TCP-1", 5000m), TestJsonContext.Default.OrderCreatedEvent);
                await externalClient.Producer.PublishAsync("remote-orders", new OrderCreatedEvent("TCP-2", 9000m), TestJsonContext.Default.OrderCreatedEvent);
            }
            else
            {
                await host.Client.Producer.PublishAsync("remote-orders", new OrderCreatedEvent("LOC-1", 5000m), TestJsonContext.Default.OrderCreatedEvent);
                await host.Client.Producer.PublishAsync("remote-orders", new OrderCreatedEvent("LOC-2", 9000m), TestJsonContext.Default.OrderCreatedEvent);
            }

            await tcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
            received.Should().HaveCount(2);
            received[1].Amount.Should().Be(9000m);
        }
        finally
        {
            if (Directory.Exists(tempDir)) Directory.Delete(tempDir, recursive: true);
        }
    }
}
