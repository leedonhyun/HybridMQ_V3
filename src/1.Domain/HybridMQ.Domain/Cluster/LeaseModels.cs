namespace HybridMQ.Domain.Cluster;

using HybridMQ.Domain.Primitives;

public sealed record LeaseResult(
    bool Acquired,
    NodeId Owner,
    FencingToken Token,
    long ExpiresAtUnixMs);

public enum DeploymentMode
{
    Embedded,
    LocalShared,
    Cluster
}

public enum ClusterStrategy
{
    None,
    SharedStore,
    ReplicatedLog
}

public enum ResourceProfile
{
    Low,
    Standard,
    High
}
