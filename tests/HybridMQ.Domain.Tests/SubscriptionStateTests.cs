namespace HybridMQ.Domain.Tests;

using HybridMQ.Domain.Primitives;
using HybridMQ.Domain.Subscriptions;
using FluentAssertions;
using Xunit;

public class SubscriptionStateTests
{
    private readonly SubscriptionId _subId = new("group1", new TopicName("test_topic"), 0);

    [Fact]
    public void Delivered_Advances_NextFetchOffset()
    {
        var state = new SubscriptionState(_subId, Offset.Earliest);
        state.Apply(new StateChange(StateChangeType.Delivered, new Offset(0), 1000));
        
        state.NextFetchOffset.Value.Should().Be(1);
        state.InFlight.Should().ContainKey(0);
    }

    [Fact]
    public void OutOfOrder_Ack_Should_Advance_CommitOffset_Only_When_Gap_Closed()
    {
        var state = new SubscriptionState(_subId, Offset.Earliest);
        state.Apply(new StateChange(StateChangeType.Delivered, new Offset(0), 1000));
        state.Apply(new StateChange(StateChangeType.Delivered, new Offset(1), 1000));
        state.Apply(new StateChange(StateChangeType.Delivered, new Offset(2), 1000));

        state.Apply(new StateChange(StateChangeType.Acked, new Offset(2), 1050));
        state.CommitOffset.Value.Should().Be(0);

        state.Apply(new StateChange(StateChangeType.Acked, new Offset(1), 1060));
        state.CommitOffset.Value.Should().Be(0);

        state.Apply(new StateChange(StateChangeType.Acked, new Offset(0), 1100));
        state.CommitOffset.Value.Should().Be(3);
    }

    [Fact]
    public void KeyLock_Tracks_InFlight_Keys_Until_Settled()
    {
        var state = new SubscriptionState(_subId, Offset.Earliest);
        state.Apply(new StateChange(StateChangeType.Delivered, new Offset(0), 1000, KeyHash: "HASH_A"));

        state.IsKeyLocked("HASH_A").Should().BeTrue();
        state.IsKeyLocked("HASH_B").Should().BeFalse();

        state.Apply(new StateChange(StateChangeType.Acked, new Offset(0), 1100));
        state.IsKeyLocked("HASH_A").Should().BeFalse();
    }

    [Fact]
    public void Nack_Moves_To_Retry_And_Holds_CommitOffset()
    {
        var state = new SubscriptionState(_subId, Offset.Earliest);
        state.Apply(new StateChange(StateChangeType.Delivered, new Offset(0), 1000));
        
        state.Apply(new StateChange(StateChangeType.Nacked, new Offset(0), 1050, NextRetryUnixMs: 2000, Attempt: 2));
        
        state.CommitOffset.Value.Should().Be(0);
        state.InFlight.Should().BeEmpty();
        state.Retries.Should().ContainKey(0);
        state.Retries[0].Attempt.Should().Be(2);
    }

    [Fact]
    public void MoveToDlq_Should_Advance_CommitOffset()
    {
        var state = new SubscriptionState(_subId, Offset.Earliest);
        state.Apply(new StateChange(StateChangeType.Delivered, new Offset(0), 1000));
        
        state.Apply(new StateChange(StateChangeType.MovedToDlq, new Offset(0), 1050, Attempt: 5, Reason: "Max Attempts"));
        
        state.CommitOffset.Value.Should().Be(1);
        state.InFlight.Should().BeEmpty();
        state.Retries.Should().BeEmpty();
        state.DeadLetters.Should().HaveCount(1);
        state.DeadLetters[0].OriginalOffset.Value.Should().Be(0);
    }

    [Fact]
    public void ResetOffset_Clears_InFlight_And_Rewinds()
    {
        var state = new SubscriptionState(_subId, Offset.Earliest);
        state.Apply(new StateChange(StateChangeType.Delivered, new Offset(0), 1000));
        state.Apply(new StateChange(StateChangeType.Delivered, new Offset(1), 1000));
        state.Apply(new StateChange(StateChangeType.Acked, new Offset(0), 1050));

        state.Apply(new StateChange(StateChangeType.ResetOffset, new Offset(0), 1200));

        state.CommitOffset.Value.Should().Be(0);
        state.NextFetchOffset.Value.Should().Be(0);
        state.InFlight.Should().BeEmpty();
    }
}
