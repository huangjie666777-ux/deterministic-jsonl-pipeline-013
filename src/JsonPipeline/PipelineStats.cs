using System.Text.Json.Serialization;

namespace JsonPipeline;

public sealed record PipelineStats
{
    [JsonPropertyName("inputRecords")]
    public long InputRecords { get; init; }

    [JsonPropertyName("committedRecords")]
    public long CommittedRecords { get; init; }

    [JsonPropertyName("filteredRecords")]
    public long FilteredRecords { get; init; }

    [JsonPropertyName("outputBytes")]
    public long OutputBytes { get; init; }

    public PipelineResult ToResult() => new((int)CommittedRecords, OutputBytes);
}

public sealed record PipelineResult(int Records, long OutputBytes);
