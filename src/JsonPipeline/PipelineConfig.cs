using System.Text.Json;
using System.Text.Json.Serialization;

namespace JsonPipeline;

public sealed class PipelineConfig
{
    [JsonPropertyName("sequenceField")]
    public string SequenceField { get; set; } = "id";

    [JsonPropertyName("maxInputLineBytes")]
    public int MaxInputLineBytes { get; set; } = 1 << 20;

    [JsonPropertyName("maxInFlight")]
    public int MaxInFlight { get; set; } = 256;

    [JsonPropertyName("maxOutputBytes")]
    public long MaxOutputBytes { get; set; } = 256L << 20;

    [JsonPropertyName("maxOperations")]
    public int MaxOperations { get; set; } = 1024;

    [JsonPropertyName("operations")]
    public List<OperationConfig> Operations { get; set; } = new();

    public const int MaxConfigBytes = 1 << 20;
    public const int HardLineCap = 64 << 20;
    public const int HardInFlightCap = 100_000;
    public const long HardOutputCap = 8L << 40;
    public const int HardOperationsCap = 10_000;

    public static PipelineConfig Parse(ReadOnlySpan<byte> utf8Json)
    {
        if (utf8Json.Length == 0)
        {
            throw new PipelineException(PipelineErrorCodes.InvalidConfig, "config is empty");
        }

        PipelineConfig? config;
        try
        {
            config = JsonSerializer.Deserialize<PipelineConfig>(utf8Json, ConfigJsonOptions);
        }
        catch (JsonException ex)
        {
            throw new PipelineException(PipelineErrorCodes.InvalidConfig, ex.Message);
        }

        if (config is null)
        {
            throw new PipelineException(PipelineErrorCodes.InvalidConfig, "config deserialized to null");
        }

        config.Validate();
        return config;
    }

    public static PipelineConfig Load(string path)
    {
        byte[] bytes = File.ReadAllBytes(path);
        if (bytes.Length > MaxConfigBytes)
        {
            throw new PipelineException(
                PipelineErrorCodes.ConfigLimitExceeded,
                $"config size {bytes.Length} exceeds limit {MaxConfigBytes} bytes");
        }

        return Parse(bytes);
    }

    internal static readonly JsonSerializerOptions ConfigJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        AllowTrailingCommas = false,
    };

    private void Validate()
    {
        if (string.IsNullOrWhiteSpace(SequenceField))
        {
            throw new PipelineException(PipelineErrorCodes.InvalidConfig, "sequenceField must be a non-empty string");
        }

        if (MaxInputLineBytes <= 0 || MaxInputLineBytes > HardLineCap)
        {
            throw new PipelineException(
                PipelineErrorCodes.ConfigLimitExceeded,
                $"maxInputLineBytes must be in 1..{HardLineCap}");
        }

        if (MaxInFlight <= 0 || MaxInFlight > HardInFlightCap)
        {
            throw new PipelineException(
                PipelineErrorCodes.ConfigLimitExceeded,
                $"maxInFlight must be in 1..{HardInFlightCap}");
        }

        if (MaxOutputBytes <= 0 || MaxOutputBytes > HardOutputCap)
        {
            throw new PipelineException(
                PipelineErrorCodes.ConfigLimitExceeded,
                $"maxOutputBytes must be in 1..{HardOutputCap}");
        }

        if (Operations.Count > HardOperationsCap)
        {
            throw new PipelineException(
                PipelineErrorCodes.ConfigLimitExceeded,
                $"operations count {Operations.Count} exceeds limit {HardOperationsCap}");
        }

        if (Operations.Count > MaxOperations)
        {
            throw new PipelineException(
                PipelineErrorCodes.ConfigLimitExceeded,
                $"operations count {Operations.Count} exceeds configured maxOperations {MaxOperations}");
        }

        if (MaxOperations <= 0 || MaxOperations > HardOperationsCap)
        {
            throw new PipelineException(
                PipelineErrorCodes.ConfigLimitExceeded,
                $"maxOperations must be in 1..{HardOperationsCap}");
        }

        HashSet<string> projectionFields = new(StringComparer.Ordinal);
        int seenIndex = 0;
        foreach (OperationConfig operation in Operations)
        {
            operation.Validate(seenIndex, this, projectionFields);
            seenIndex++;
        }
    }
}

public sealed class OperationConfig
{
    [JsonPropertyName("op")]
    public string Op { get; set; } = "";

    [JsonPropertyName("field")]
    public string? Field { get; set; }

    [JsonPropertyName("to")]
    public string? To { get; set; }

    [JsonPropertyName("fields")]
    public List<string>? Fields { get; set; }

    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("equals")]
    public JsonElement? EqualValue { get; set; }

    [JsonPropertyName("min")]
    public double? Min { get; set; }

    [JsonPropertyName("max")]
    public double? Max { get; set; }

    [JsonPropertyName("negate")]
    public bool Negate { get; set; }

    public const string ValidateOp = "validate";
    public const string ProjectOp = "project";
    public const string RenameOp = "rename";
    public const string FilterOp = "filter";

    public static readonly string[] AllowedTypes = { "string", "number", "boolean", "object", "array" };

    internal void Validate(int index, PipelineConfig config, HashSet<string> projectionFields)
    {
        string where = $"operations[{index}]";
        if (string.IsNullOrWhiteSpace(Op))
        {
            throw new PipelineException(PipelineErrorCodes.InvalidConfig, $"{where}.op is required");
        }

        switch (Op)
        {
            case ValidateOp:
                RequireField(where);
                if (string.IsNullOrWhiteSpace(Type) || !AllowedTypes.Contains(Type))
                {
                    throw new PipelineException(
                        PipelineErrorCodes.InvalidConfig,
                        $"{where}.type must be one of {string.Join("|", AllowedTypes)}");
                }

                if (Min.HasValue || Max.HasValue)
                {
                    if (Type != "number")
                    {
                        throw new PipelineException(
                            PipelineErrorCodes.InvalidConfig,
                            $"{where}.min/max require type=number");
                    }

                    if (Min.HasValue && Max.HasValue && Min.Value > Max.Value)
                    {
                        throw new PipelineException(
                            PipelineErrorCodes.InvalidConfig,
                            $"{where}.min must not exceed max");
                    }
                }

                break;

            case ProjectOp:
                if (Fields is null || Fields.Count == 0 || Fields.Any(string.IsNullOrWhiteSpace))
                {
                    throw new PipelineException(
                        PipelineErrorCodes.InvalidConfig,
                        $"{where}.fields must be a non-empty array of field names");
                }

                if (Fields.Count > config.MaxOperations)
                {
                    throw new PipelineException(
                        PipelineErrorCodes.ConfigLimitExceeded,
                        $"{where}.fields count exceeds {config.MaxOperations}");
                }

                if (!projectionFields.Any())
                {
                    foreach (string field in Fields)
                    {
                        projectionFields.Add(field);
                    }
                }

                break;

            case RenameOp:
                RequireField(where);
                if (string.IsNullOrWhiteSpace(To))
                {
                    throw new PipelineException(PipelineErrorCodes.InvalidConfig, $"{where}.to is required");
                }

                if (Field == To)
                {
                    throw new PipelineException(
                        PipelineErrorCodes.InvalidConfig,
                        $"{where}.field and to must differ");
                }

                break;

            case FilterOp:
                RequireField(where);
                bool hasPredicate = EqualValue.HasValue || Min.HasValue || Max.HasValue || !string.IsNullOrWhiteSpace(Type);
                if (!hasPredicate)
                {
                    throw new PipelineException(
                        PipelineErrorCodes.InvalidConfig,
                        $"{where} requires one of: equals, min, max, type");
                }

                if ((Min.HasValue || Max.HasValue) && Min.HasValue && Max.HasValue && Min.Value > Max.Value)
                {
                    throw new PipelineException(
                        PipelineErrorCodes.InvalidConfig,
                        $"{where}.min must not exceed max");
                }

                break;

            default:
                throw new PipelineException(
                    PipelineErrorCodes.UnknownOperation,
                    $"unknown operation '{Op}' at {where}; allowed: validate|project|rename|filter");
        }
    }

    private void RequireField(string where)
    {
        if (string.IsNullOrWhiteSpace(Field))
        {
            throw new PipelineException(PipelineErrorCodes.InvalidConfig, $"{where}.field is required");
        }
    }
}
