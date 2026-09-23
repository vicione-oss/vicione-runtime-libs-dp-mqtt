using AwesomeAssertions;
using Xunit;

namespace ViciOne.Suite.DataPort;

public class DistinctReports_IsFirst
{
    [Fact]
    public void Is_first_only_for_a_text_not_reported_yet()
    {
        DistinctReports reports = new();

        reports.IsFirst("Timestamp", "value", "the day before").Should().BeTrue();
        reports.IsFirst("Timestamp", "value", "the day before").Should().BeFalse();
    }

    [Theory]
    [InlineData("Validity", "value", "the day before")]
    [InlineData("Timestamp", "other", "the day before")]
    [InlineData("Timestamp", "value", "the day after")]
    public void Tells_the_problem_the_topic_and_the_text_apart(string problem, string topic, string text)
    {
        DistinctReports reports = new();
        reports.IsFirst("Timestamp", "value", "the day before");

        reports.IsFirst(problem, topic, text).Should().BeTrue();
    }

    /// <summary>
    /// The texts come off the broker, so the memory is bounded; once it is full it starts over and a
    /// problem that persists is reported once more.
    /// </summary>
    [Fact]
    public void Reports_a_text_again_once_the_memory_was_full()
    {
        DistinctReports reports = new();
        reports.IsFirst("Timestamp", "value", "the day before");

        for (var i = 1; i < DistinctReports.Capacity; i++)
            reports.IsFirst("Timestamp", "value", $"text {i}");

        reports.IsFirst("Timestamp", "value", "the day before").Should().BeTrue();
    }
}
