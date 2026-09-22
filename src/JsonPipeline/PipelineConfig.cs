using System.Text.Json;
using System.Text.Json.Serialization;

namespace JsonPipeline;

public sealed class PipelineLimits
{
    public int MaxLineBytes { get; set; } = 1 << 20;
    public int MaxInFlight { get; set; } = 256;
    public long MaxOutputBytes { get; set; } = 256L << 20;
    public int MaxOperations { get; set; } = 64;
    public int MaxConfigBytes { get; set; } = 1 << 20;

    internal const int AbsoluteMaxLineBytes = 16 << 20;
    internal const int AbsoluteMaxInFlight = 1 << 15;
    internal const long AbsoluteMaxOutputBytes = 8L << 30;
    internal const int AbsoluteMaxOperations = 4096;
}

public sealed class PipelineConfig
{
    [JsonPropertyName("sequenceField")]
    public string? SequenceField { get; set; }

    [JsonPropertyName("operations")]
    public List<Dictionary<string, JsonElement>> Operations { get; set; } = new();

    [JsonPropertyName("limits")]
    public PipelineLimits Limits { get; set; } = new();

    internal bool HasExplicitSequenceField { get; private set; }

    public static PipelineConfig Parse(ReadOnlySpan<byte> utf8Json)
    {
        PipelineConfig? config;
        try
        {
            config = JsonSerializer.Deserialize<PipelineConfig>(
                utf8Json,
                new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    PropertyNameCaseInsensitive = false,
                });
        }
        catch (JsonException ex)
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig, $"config is not valid JSON: {ex.Message}", ex);
        }

        if (config is null)
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig, "config must be a JSON object");
        }

        config.HasExplicitSequenceField = config.SequenceField is not null;
        config.SequenceField ??= "seq";
        if (string.IsNullOrWhiteSpace(config.SequenceField))
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig, "sequenceField must be a non-empty string");
        }

        PipelineLimits limits = config.Limits;
        if (limits.MaxLineBytes is <= 0 or > PipelineLimits.AbsoluteMaxLineBytes)
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig,
                $"limits.maxLineBytes must be in 1..{PipelineLimits.AbsoluteMaxLineBytes}");
        }
        if (limits.MaxInFlight is <= 0 or > PipelineLimits.AbsoluteMaxInFlight)
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig,
                $"limits.maxInFlight must be in 1..{PipelineLimits.AbsoluteMaxInFlight}");
        }
        if (limits.MaxOutputBytes <= 0 || limits.MaxOutputBytes > PipelineLimits.AbsoluteMaxOutputBytes)
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig,
                $"limits.maxOutputBytes must be in 1..{PipelineLimits.AbsoluteMaxOutputBytes}");
        }
        if (limits.MaxOperations is <= 0 or > PipelineLimits.AbsoluteMaxOperations)
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig,
                $"limits.maxOperations must be in 1..{PipelineLimits.AbsoluteMaxOperations}");
        }
        if (limits.MaxConfigBytes is <= 0)
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig, "limits.maxConfigBytes must be positive");
        }

        foreach (Dictionary<string, JsonElement> operation in config.Operations)
        {
            OperationDefinition.Validate(operation);
        }

        if (config.Operations.Count > limits.MaxOperations)
        {
            throw new PipelineException(
                PipelineErrorCode.InvalidConfig,
                $"operation count {config.Operations.Count} exceeds maxOperations {limits.MaxOperations}");
        }

        return config;
    }
}

internal static class OperationDefinition
{
    private static readonly HashSet<string> KnownTypes = new(StringComparer.Ordinal)
    {
        "validate", "project", "rename", "filter",
    };

    public static void Validate(Dictionary<string, JsonElement> operation)
    {
        if (!operation.TryGetValue("op", out JsonElement opElement) || opElement.ValueKind != JsonValueKind.String)
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig, "each operation requires a string 'op'");
        }

        string op = opElement.GetString()!;
        if (!KnownTypes.Contains(op))
        {
            throw new PipelineException(PipelineErrorCode.UnknownOperation, $"unknown operation '{op}'");
        }

        switch (op)
        {
            case "validate":
                RequireField(operation, "field");
                if (operation.TryGetValue("type", out JsonElement typeElement))
                {
                    string? type = typeElement.GetString();
                    if (type is not ("string" or "number" or "integer" or "boolean" or "object" or "array"))
                    {
                        throw new PipelineException(PipelineErrorCode.InvalidConfig,
                            $"validate.type '{type}' is not supported");
                    }
                }
                break;
            case "project":
                RequireStringArray(operation, "fields");
                break;
            case "rename":
                RequireField(operation, "from");
                RequireField(operation, "to");
                if (string.Equals(
                        operation["from"].GetString(),
                        operation["to"].GetString(),
                        StringComparison.Ordinal))
                {
                    throw new PipelineException(PipelineErrorCode.InvalidConfig, "rename 'from' and 'to' must differ");
                }
                break;
            case "filter":
                RequireField(operation, "field");
                if (!operation.TryGetValue("operator", out JsonElement filterOp) || filterOp.ValueKind != JsonValueKind.String)
                {
                    throw new PipelineException(PipelineErrorCode.InvalidConfig, "filter requires a string 'operator'");
                }
                string? filterOperator = filterOp.GetString();
                if (filterOperator is not ("eq" or "ne" or "gt" or "ge" or "lt" or "le"))
                {
                    throw new PipelineException(PipelineErrorCode.InvalidConfig,
                        $"filter.operator '{filterOperator}' is not supported");
                }
                if (!operation.ContainsKey("value"))
                {
                    throw new PipelineException(PipelineErrorCode.InvalidConfig, "filter requires a 'value'");
                }
                break;
        }
    }

    private static void RequireField(Dictionary<string, JsonElement> operation, string key)
    {
        if (!operation.TryGetValue(key, out JsonElement element) || element.ValueKind != JsonValueKind.String
            || string.IsNullOrEmpty(element.GetString()))
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig,
                $"operation '{operation.GetValueOrDefault("op").GetString()}' requires a non-empty string '{key}'");
        }
    }

    private static void RequireStringArray(Dictionary<string, JsonElement> operation, string key)
    {
        if (!operation.TryGetValue(key, out JsonElement element) || element.ValueKind != JsonValueKind.Array)
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig, $"project requires an array '{key}'");
        }

        HashSet<string> seen = new(StringComparer.Ordinal);
        foreach (JsonElement item in element.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String || string.IsNullOrEmpty(item.GetString()))
            {
                throw new PipelineException(PipelineErrorCode.InvalidConfig,
                    "project.fields entries must be non-empty strings");
            }
            if (!seen.Add(item.GetString()!))
            {
                throw new PipelineException(PipelineErrorCode.InvalidConfig,
                    $"project.fields contains duplicate field '{item.GetString()}'");
            }
        }
    }
}
