using System.Buffers;
using System.Text.Json;

namespace JsonPipeline;

public static class CanonicalJson
{
    public static string Serialize(string json)
    {
        ArgumentNullException.ThrowIfNull(json);
        using JsonDocument document = JsonDocument.Parse(json, StrictOptions);
        ArrayBufferWriter<byte> buffer = new();
        using (Utf8JsonWriter writer = new(buffer, WriterOptions))
        {
            WriteElement(writer, document.RootElement);
            writer.Flush();
        }

        return System.Text.Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    internal static void WriteElement(Utf8JsonWriter writer, JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                writer.WriteStartObject();
                List<JsonProperty> properties = new();
                foreach (JsonProperty property in element.EnumerateObject())
                {
                    properties.Add(property);
                }

                properties.Sort(static (a, b) => string.CompareOrdinal(a.Name, b.Name));
                foreach (JsonProperty property in properties)
                {
                    writer.WritePropertyName(property.Name);
                    WriteElement(writer, property.Value);
                }

                writer.WriteEndObject();
                break;

            case JsonValueKind.Array:
                writer.WriteStartArray();
                foreach (JsonElement child in element.EnumerateArray())
                {
                    WriteElement(writer, child);
                }

                writer.WriteEndArray();
                break;

            case JsonValueKind.String:
                writer.WriteStringValue(element.GetString());
                break;

            case JsonValueKind.Number:
                writer.WriteRawValue(element.GetRawText(), skipInputValidation: true);
                break;

            case JsonValueKind.True:
                writer.WriteBooleanValue(true);
                break;

            case JsonValueKind.False:
                writer.WriteBooleanValue(false);
                break;

            case JsonValueKind.Null:
                writer.WriteNullValue();
                break;

            default:
                throw new PipelineException(PipelineErrorCodes.InvalidJson, $"unsupported JSON value kind {element.ValueKind}");
        }
    }

    internal static readonly JsonWriterOptions WriterOptions = new()
    {
        Indented = false,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    internal static readonly JsonDocumentOptions StrictOptions = new()
    {
        AllowTrailingCommas = false,
        CommentHandling = JsonCommentHandling.Disallow,
    };
}
