using System.Security.Cryptography;
using System.Text.Json;
using System.Threading.Channels;

namespace JsonPipeline;

public sealed record PipelineResult(int Records, long OutputBytes);

public sealed class PipelineEngine
{
    private readonly PipelineConfig _config;
    private readonly int _workers;

    public PipelineEngine() : this(null, 2) { }

    public PipelineEngine(PipelineConfig? config, int workers = 2)
    {
        _config = config ?? new PipelineConfig();
        if (workers is < 1 or > 1024)
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig, "workers must be in 1..1024");
        }
        _workers = workers;
    }

    public static string NormalizeJson(string json)
    {
        using JsonDocument document = JsonDocument.Parse(json);
        return JsonSerializer.Serialize(document.RootElement);
    }

    public async Task<PipelineResult> RunAsync(
        Stream input,
        Stream output,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);
        PipelineOrchestrator orchestrator = new(_config, _workers);
        PipelineStats stats = await orchestrator
            .RunAsync(input, output, null, null, cancellationToken)
            .ConfigureAwait(false);
        return new PipelineResult((int)stats.CommittedRecords, stats.OutputBytes);
    }
}
