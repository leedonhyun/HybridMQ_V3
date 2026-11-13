namespace HybridMQ.Domain.Tests;

using HybridMQ.Domain.Reliability;
using HybridMQ.Domain.Routing;
using FluentAssertions;
using Xunit;

public class RoutingAndReliabilityTests
{
    [Theory]
    [InlineData(ExchangeType.Direct, "order.created", "order.created", true)]
    [InlineData(ExchangeType.Direct, "order.created", "order.paid", false)]
    [InlineData(ExchangeType.Fanout, "ignored", "any.key", true)]
    [InlineData(ExchangeType.Topic, "order.*.kr", "order.paid.kr", true)]
    [InlineData(ExchangeType.Topic, "order.*.kr", "order.paid.us", false)]
    [InlineData(ExchangeType.Topic, "order.#", "order", true)]
    [InlineData(ExchangeType.Topic, "order.#", "order.paid.kr.v2", true)]
    [InlineData(ExchangeType.Topic, "#.error", "app.db.timeout.error", true)]
    [InlineData(ExchangeType.Topic, "#.error", "app.db.timeout.info", false)]
    public void RoutingMatcher_Evaluates_Patterns_Accurately(ExchangeType type, string pattern, string routingKey, bool expected)
    {
        RoutingMatcher.IsMatch(type, pattern, routingKey).Should().Be(expected);
    }

    [Fact]
    public void IdempotencyWindow_Deduplicates_Within_Window_And_Evicts_After_Expiry()
    {
        var window = new IdempotencyWindow(TimeSpan.FromSeconds(10), maxEntries: 100);

        window.TryRegister("prod1:1", 1000).Should().BeTrue();
        window.TryRegister("prod1:1", 5000).Should().BeFalse(); // Duplicate within 10s
        window.TryRegister("prod1:1", 11001).Should().BeTrue(); // Expired after 10s
    }

    [Fact]
    public void RetryPolicy_Computes_Exponential_Backoff_Within_Bounds()
    {
        var policy = new RetryPolicy(
            MaxAttempts: 5,
            InitialDelay: TimeSpan.FromMilliseconds(100),
            MaxDelay: TimeSpan.FromMilliseconds(1000),
            BackoffMultiplier: 2.0,
            JitterFactor: 0.0);

        policy.ComputeDelay(1).TotalMilliseconds.Should().Be(100);
        policy.ComputeDelay(2).TotalMilliseconds.Should().Be(200);
        policy.ComputeDelay(3).TotalMilliseconds.Should().Be(400);
        policy.ComputeDelay(10).TotalMilliseconds.Should().Be(1000); // Capped at MaxDelay
    }
}
