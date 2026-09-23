using System.Collections.Concurrent;

namespace ViciOne.Suite.DataPort;

/// <summary>
/// Remembers what was reported about the texts a sender wrote, so a sender that keeps writing the
/// same unreadable text is reported once instead of once per message. The texts come off the broker,
/// so at most <see cref="Capacity"/> are remembered; a full memory starts over, which reports a
/// problem that persists once more.
/// </summary>
internal sealed class DistinctReports
{
    internal const int Capacity = 256;

    private readonly ConcurrentDictionary<(string Problem, string Topic, string Text), byte> _reported = new();

    internal bool IsFirst(string problem, string topic, string text)
    {
        if (_reported.Count >= Capacity)
            _reported.Clear();

        return _reported.TryAdd((problem, topic, text), 0);
    }
}
