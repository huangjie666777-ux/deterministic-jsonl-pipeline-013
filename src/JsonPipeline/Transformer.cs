using System.Text.Json;

namespace JsonPipeline;

internal sealed record RawRecord(long Ordinal, long LineNumber, long Sequence, byte[] Utf8Line);

internal readonly record struct TransformOutput(long Ordinal, long LineNumber, byte[]? Payload)
{
    public bool Dropped => Payload is null;
}

internal static class Transformer
{
    public static readonly JsonWriterOptions WriterOptions = new() { Indented = false };

    public static TransformOutput Process(RawRecord record, PipelineConfig config)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(
                record.Utf8Line,
                new JsonDocumentOptions { AllowTrailingCommas = false, CommentHandling = JsonCommentHandling.Disallow });
        }
        catch (JsonException ex)
        {
            throw new PipelineException(
                PipelineErrorCode.InvalidJson,
                $"line {record.LineNumber}: invalid JSON: {ex.Message}",
                ex,
                record.LineNumber);
        }

        using (document)
        {
            JsonElement root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                throw new PipelineException(
                    PipelineErrorCode.TypeMismatch,
                    $"line {record.LineNumber}: record must be a JSON object",
                    record.LineNumber);
            }

            foreach (Dictionary<string, JsonElement> operation in config.Operations)
            {
                root = ApplyOperation(root, operation, record.LineNumber);
            }

            using MemoryStream stream = new();
            using (Utf8JsonWriter writer = new(stream, WriterOptions))
            {
                root.WriteTo(writer);
                writer.Flush();
            }
            stream.WriteByte((byte)'\n');
            return new TransformOutput(record.Ordinal, record.LineNumber, stream.ToArray());
        }
    }

    private static JsonElement ApplyOperation(
        JsonElement root,
        Dictionary<string, JsonElement> operation,
        long lineNumber)
    {
        string op = operation["op"].GetString()!;
        return op switch
        {
            "validate" => ApplyValidate(root, operation, lineNumber),
            "project" => ApplyProject(root, operation, lineNumber),
            "rename" => ApplyRename(root, operation, lineNumber),
            "filter" => ApplyFilter(root, operation, lineNumber),
            _ => throw new PipelineException(PipelineErrorCode.UnknownOperation, $"unknown operation '{op}'", lineNumber),
        };
    }

    private static JsonElement ApplyValidate(JsonElement root, Dictionary<string, JsonElement> operation, long lineNumber)
    {
        string field = operation["field"].GetString()!;
        if (!root.TryGetProperty(field, out JsonElement value))
        {
            throw new PipelineException(
                PipelineErrorCode.ValidationFailed,
                $"line {lineNumber}: missing required field '{field}'",
                lineNumber);
        }

        if (operation.TryGetValue("type", out JsonElement typeElement))
        {
            string expected = typeElement.GetString()!;
            if (!MatchesType(value, expected))
            {
                throw new PipelineException(
                    PipelineErrorCode.TypeMismatch,
                    $"line {lineNumber}: field '{field}' expected {expected}, got {value.ValueKind.ToString().ToLowerInvariant()}",
                    lineNumber);
            }
        }

        return root;
    }

    private static bool MatchesType(JsonElement value, string expected) => expected switch
    {
        "string" => value.ValueKind == JsonValueKind.String,
        "number" => value.ValueKind == JsonValueKind.Number,
        "integer" => value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out _),
        "boolean" => value.ValueKind is JsonValueKind.True or JsonValueKind.False,
        "object" => value.ValueKind == JsonValueKind.Object,
        "array" => value.ValueKind == JsonValueKind.Array,
        _ => false,
    };

    private static JsonElement ApplyProject(JsonElement root, Dictionary<string, JsonElement> operation, long lineNumber)
    {
        using MemoryStream stream = new();
        using (Utf8JsonWriter writer = new(stream, WriterOptions))
        {
            writer.WriteStartObject();
            foreach (JsonElement fieldElement in operation["fields"].EnumerateArray())
            {
                string field = fieldElement.GetString()!;
                if (root.TryGetProperty(field, out JsonElement value))
                {
                    writer.WritePropertyName(field);
                    value.WriteTo(writer);
                }
            }
            writer.WriteEndObject();
            writer.Flush();
        }

        return ParseCloned(stream.ToArray(), lineNumber);
    }

    private static JsonElement ApplyRename(JsonElement root, Dictionary<string, JsonElement> operation, long lineNumber)
    {
        string from = operation["from"].GetString()!;
        string to = operation["to"].GetString()!;
        if (!root.TryGetProperty(from, out JsonElement value))
        {
            throw new PipelineException(
                PipelineErrorCode.ValidationFailed,
                $"line {lineNumber}: cannot rename missing field '{from}'",
                lineNumber);
        }

        using MemoryStream stream = new();
        using (Utf8JsonWriter writer = new(stream, WriterOptions))
        {
            writer.WriteStartObject();
            bool renamed = false;
            foreach (JsonProperty property in root.EnumerateObject())
            {
                writer.WritePropertyName(renamed || !property.NameEquals(from) ? property.Name : to);
                property.Value.WriteTo(writer);
                renamed |= property.NameEquals(from);
            }
            writer.WriteEndObject();
            writer.Flush();
        }

        return ParseCloned(stream.ToArray(), lineNumber);
    }

    private static JsonElement ApplyFilter(JsonElement root, Dictionary<string, JsonElement> operation, long lineNumber)
    {
        string field = operation["field"].GetString()!;
        string filterOperator = operation["operator"].GetString()!;
        JsonElement expected = operation["value"];

        if (!root.TryGetProperty(field, out JsonElement actual))
        {
            throw new PipelineException(
                PipelineErrorCode.ValidationFailed,
                $"line {lineNumber}: filter references missing field '{field}'",
                lineNumber);
        }

        bool keep = filterOperator switch
        {
            "eq" => JsonEquals(actual, expected),
            "ne" => !JsonEquals(actual, expected),
            "gt" or "ge" or "lt" or "le" => CompareNumbers(actual, expected, filterOperator, field, lineNumber),
            _ => throw new PipelineException(PipelineErrorCode.UnknownOperation, $"unknown filter operator '{filterOperator}'", lineNumber),
        };

        return keep
            ? root
            : throw new DropRecordException();
    }

    private static bool CompareNumbers(JsonElement actual, JsonElement expected, string filterOperator, string field, long lineNumber)
    {
        if (actual.ValueKind != JsonValueKind.Number || expected.ValueKind != JsonValueKind.Number)
        {
            throw new PipelineException(
                PipelineErrorCode.TypeMismatch,
                $"line {lineNumber}: filter '{field}' {filterOperator} requires numeric operands",
                lineNumber);
        }

        if (!actual.TryGetDouble(out double left) || !expected.TryGetDouble(out double right))
        {
            throw new PipelineException(
                PipelineErrorCode.TypeMismatch,
                $"line {lineNumber}: filter '{field}' operands are not finite numbers",
                lineNumber);
        }

        return filterOperator switch
        {
            "gt" => left > right,
            "ge" => left >= right,
            "lt" => left < right,
            _ => left <= right,
        };
    }

    private static bool JsonEquals(JsonElement left, JsonElement right)
    {
        if (left.ValueKind != right.ValueKind)
        {
            if (left.ValueKind == JsonValueKind.Number && right.ValueKind == JsonValueKind.Number
                && left.TryGetDecimal(out decimal a) && right.TryGetDecimal(out decimal b))
            {
                return a == b;
            }
            return false;
        }

        return left.ValueKind switch
        {
            JsonValueKind.Object => ObjectEquals(left, right),
            JsonValueKind.Array => ArrayEquals(left, right),
            JsonValueKind.String => string.Equals(left.GetString(), right.GetString(), StringComparison.Ordinal),
            JsonValueKind.Number => left.TryGetDecimal(out decimal a)
                && right.TryGetDecimal(out decimal b) ? a == b : left.GetRawText() == right.GetRawText(),
            JsonValueKind.True or JsonValueKind.False => left.GetBoolean() == right.GetBoolean(),
            JsonValueKind.Null => true,
            _ => left.GetRawText() == right.GetRawText(),
        };
    }

    private static bool ObjectEquals(JsonElement left, JsonElement right)
    {
        int count = 0;
        foreach (JsonProperty property in left.EnumerateObject())
        {
            count++;
            if (!right.TryGetProperty(property.Name, out JsonElement other)
                || !JsonEquals(property.Value, other))
            {
                return false;
            }
        }
        int otherCount = 0;
        foreach (JsonProperty _ in right.EnumerateObject())
        {
            otherCount++;
        }
        return count == otherCount;
    }

    private static bool ArrayEquals(JsonElement left, JsonElement right)
    {
        JsonElement.ArrayEnumerator leftEnumerator = left.EnumerateArray();
        JsonElement.ArrayEnumerator rightEnumerator = right.EnumerateArray();
#pragma warning disable CA1851
        while (leftEnumerator.MoveNext())
        {
            if (!rightEnumerator.MoveNext() || !JsonEquals(leftEnumerator.Current, rightEnumerator.Current))
            {
                return false;
            }
        }
#pragma warning restore CA1851
        return !rightEnumerator.MoveNext();
    }

    private static JsonElement ParseCloned(byte[] utf8Json, long lineNumber)
    {
        try
        {
            return JsonSerializer.Deserialize<JsonElement>(utf8Json);
        }
        catch (JsonException ex)
        {
            throw new PipelineException(PipelineErrorCode.InvalidJson, $"line {lineNumber}: internal rewrite failed", ex, lineNumber);
        }
    }
}

internal sealed class DropRecordException : Exception
{
    public DropRecordException() : base("record filtered out") { }
}
