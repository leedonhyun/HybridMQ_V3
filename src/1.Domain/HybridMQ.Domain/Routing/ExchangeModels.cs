namespace HybridMQ.Domain.Routing;

using HybridMQ.Domain.Primitives;

public enum ExchangeType
{
    Direct,
    Topic,
    Fanout
}

public sealed record BindingRule(
    string ExchangeName,
    ExchangeType Type,
    string Pattern,
    TopicName TargetTopic);

public static class RoutingMatcher
{
    public static bool IsMatch(ExchangeType type, string pattern, string? routingKey)
    {
        return type switch
        {
            ExchangeType.Fanout => true,
            ExchangeType.Direct => string.Equals(pattern, routingKey ?? string.Empty, StringComparison.Ordinal),
            ExchangeType.Topic => MatchTopicPattern(pattern, routingKey ?? string.Empty),
            _ => false
        };
    }

    private static bool MatchTopicPattern(string pattern, string routingKey)
    {
        var pTokens = pattern.Split('.');
        var kTokens = routingKey.Split('.');
        return MatchTokens(pTokens, 0, kTokens, 0);
    }

    private static bool MatchTokens(string[] p, int pi, string[] k, int ki)
    {
        if (pi == p.Length && ki == k.Length) return true;
        if (pi == p.Length) return false;

        if (p[pi] == "#")
        {
            if (pi == p.Length - 1) return true;
            for (int nextKi = ki; nextKi <= k.Length; nextKi++)
            {
                if (MatchTokens(p, pi + 1, k, nextKi)) return true;
            }
            return false;
        }

        if (ki == k.Length) return false;
        if (p[pi] == "*" || string.Equals(p[pi], k[ki], StringComparison.Ordinal))
        {
            return MatchTokens(p, pi + 1, k, ki + 1);
        }

        return false;
    }
}
