using System.Text.Json;

namespace JsonPipeline;

internal readonly record struct TransformOutcome(bool Written, string? CanonicalLine)
{
    public static TransformOutcome Filtered { get; } = new(false, null);
}

internal static class RecordTransformer
{
    public static TransformOutcome Apply(string rawLine, int lineNumber, PipelineConfig config)
    {
        using JsonDocument document = JsonDocument.Parse(rawLine, CanonicalJson.StrictOptions);
        JsonElement root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
        {
            throw new PipelineException(
                PipelineErrorCodes.ValidationFailed,
                $"line {lineNumber}: root value must be a JSON object");
        }

        JsonObjectBuilder builder = JsonObjectBuilder.FromElement(root);

        foreach (OperationConfig operation in config.Operations)
        {
            switch (operation.Op)
            {
                case OperationConfig.ValidateOp:
                    ApplyValidate(builder, operation, lineNumber);
                    break;

                case OperationConfig.ProjectOp:
                    builder.Project(operation.Fields!, config.SequenceField);
                    break;

                case OperationConfig.RenameOp:
                    builder.Rename(operation.Field!, operation.To!, lineNumber);
                    break;

                case OperationConfig.FilterOp:
                    if (!EvaluateFilter(builder, operation, lineNumber))
                    {
                        return TransformOutcome.Filtered;
                    }

                    break;
            }
        }

        string canonical = CanonicalJson.Serialize(builder.ToJson());
        return new TransformOutcome(true, canonical);
    }

    private static void ApplyValidate(JsonObjectBuilder builder, OperationConfig operation, int lineNumber)
    {
        if (!builder.TryGet(operation.Field!, out JsonElement value))
        {
            throw new PipelineException(
                PipelineErrorCodes.ValidationFailed,
                $"line {lineNumber}: required field '{operation.Field}' is missing");
        }

        string expected = operation.Type!;
        string actual = DescribeKind(value.ValueKind);
        if (!KindMatches(value.ValueKind, expected))
        {
            throw new PipelineException(
                PipelineErrorCodes.ValidationFailed,
                $"line {lineNumber}: field '{operation.Field}' expected {expected} but was {actual}");
        }

        if (expected == "number")
        {
            double number = value.GetDouble();
            if (operation.Min.HasValue && number < operation.Min.Value)
            {
                throw new PipelineException(
                    PipelineErrorCodes.ValidationFailed,
                    $"line {lineNumber}: field '{operation.Field}' value {number} is below min {operation.Min.Value}");
            }

            if (operation.Max.HasValue && number > operation.Max.Value)
            {
                throw new PipelineException(
                    PipelineErrorCodes.ValidationFailed,
                    $"line {lineNumber}: field '{operation.Field}' value {number} exceeds max {operation.Max.Value}");
            }
        }
    }

    private static bool EvaluateFilter(JsonObjectBuilder builder, OperationConfig operation, int lineNumber)
    {
        if (!builder.TryGet(operation.Field!, out JsonElement value))
        {
            return operation.Negate;
        }

        bool match = true;

        if (!string.IsNullOrWhiteSpace(operation.Type))
        {
            match &= KindMatches(value.ValueKind, operation.Type);
        }

        if (operation.EqualValue.HasValue)
        {
            match &= JsonEquals(value, operation.EqualValue.Value);
        }

        if (operation.Min.HasValue || operation.Max.HasValue)
        {
            if (value.ValueKind != JsonValueKind.Number)
            {
                throw new PipelineException(
                    PipelineErrorCodes.ValidationFailed,
                    $"line {lineNumber}: filter field '{operation.Field}' expected number but was {DescribeKind(value.ValueKind)}");
            }

            double number = value.GetDouble();
            if (operation.Min.HasValue && number < operation.Min.Value)
            {
                match = false;
            }

            if (operation.Max.HasValue && number > operation.Max.Value)
            {
                match = false;
            }
        }

        return operation.Negate ? !match : match;
    }

    private static bool KindMatches(JsonValueKind kind, string expected) => expected switch
    {
        "string" => kind == JsonValueKind.String,
        "number" => kind == JsonValueKind.Number,
        "boolean" => kind is JsonValueKind.True or JsonValueKind.False,
        "object" => kind == JsonValueKind.Object,
        "array" => kind == JsonValueKind.Array,
        _ => false,
    };

    private static string DescribeKind(JsonValueKind kind) => kind switch
    {
        JsonValueKind.String => "string",
        JsonValueKind.Number => "number",
        JsonValueKind.True or JsonValueKind.False => "boolean",
        JsonValueKind.Object => "object",
        JsonValueKind.Array => "array",
        JsonValueKind.Null => "null",
        _ => "undefined",
    };

    private static bool JsonEquals(JsonElement a, JsonElement b)
    {
        if (a.ValueKind != b.ValueKind)
        {
            return false;
        }

        switch (a.ValueKind)
        {
            case JsonValueKind.String:
                return string.Equals(a.GetString(), b.GetString(), StringComparison.Ordinal);
            case JsonValueKind.Number:
                return a.GetRawText() == b.GetRawText();
            case JsonValueKind.True:
            case JsonValueKind.False:
                return true;
            case JsonValueKind.Null:
                return true;
            case JsonValueKind.Array:
            {
                JsonElement.ArrayEnumerator left = a.EnumerateArray();
                JsonElement.ArrayEnumerator right = b.EnumerateArray();
                while (left.MoveNext())
                {
                    if (!right.MoveNext() || !JsonEquals(left.Current, right.Current))
                    {
                        return false;
                    }
                }

                return !right.MoveNext();
            }

            case JsonValueKind.Object:
            {
                if (CountProperties(a) != CountProperties(b))
                {
                    return false;
                }

                foreach (JsonProperty property in a.EnumerateObject())
                {
                    if (!b.TryGetProperty(property.Name, out JsonElement other) ||
                        !JsonEquals(property.Value, other))
                    {
                        return false;
                    }
                }

                return true;
            }

            default:
                return false;
        }
    }

    private static int CountProperties(JsonElement element)
    {
        int count = 0;
        foreach (JsonProperty _ in element.EnumerateObject())
        {
            count++;
        }

        return count;
    }
}
