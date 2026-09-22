using System.Text.Json;

namespace JsonPipeline;

public sealed record PipelineResult(int Records, long OutputBytes);

public sealed class PipelineEngine
{
    public static string NormalizeJson(string json)
    {
        using JsonDocument document = JsonDocument.Parse(json);
        return JsonSerializer.Serialize(document.RootElement);
    }

    public Task<PipelineResult> RunAsync(
        Stream input,
        Stream output,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        return Task.FromResult(new PipelineResult(0, 0));
    }
}
