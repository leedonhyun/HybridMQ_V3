namespace HybridMQ.Domain.Primitives;

public readonly record struct TopicName(string Value)
{
    public override string ToString() => Value;
    public static implicit operator string(TopicName name) => name.Value;
}

public readonly record struct PartitionId(TopicName Topic, int Partition)
{
    public override string ToString() => $"{Topic.Value}-{Partition}";
}

public readonly record struct Offset(long Value) : IComparable<Offset>
{
    public static readonly Offset Earliest = new(0);
    public static readonly Offset Latest = new(-1);
    public static readonly Offset Invalid = new(-2);

    public Offset Next() => new(Value + 1);
    public int CompareTo(Offset other) => Value.CompareTo(other.Value);
    public override string ToString() => Value.ToString();
}

public readonly record struct SubscriptionId(string Group, TopicName Topic, int Partition)
{
    public override string ToString() => $"{Group}:{Topic.Value}:{Partition}";
}

public readonly record struct NodeId(string Value)
{
    public static NodeId NewId() => new(Guid.NewGuid().ToString("N"));
    public override string ToString() => Value;
}

public readonly record struct FencingToken(long Value) : IComparable<FencingToken>
{
    public FencingToken Next() => new(Value + 1);
    public int CompareTo(FencingToken other) => Value.CompareTo(other.Value);
}
