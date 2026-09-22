using System.Buffers;
using System.Text;
using System.Text.Json;

namespace JsonPipeline;

internal sealed class JsonObjectBuilder
{
    private readonly List<KeyValuePair<string, JsonElement>> _members;

    private JsonObjectBuilder(List<KeyValuePair<string, JsonElement>> members)
    {
        _members = members;
    }

    public static JsonObjectBuilder FromElement(JsonElement root)
    {
        List<KeyValuePair<string, JsonElement>> members = new();
        foreach (JsonProperty property in root.EnumerateObject())
        {
            members.Add(new KeyValuePair<string, JsonElement>(property.Name, property.Value));
        }

        return new JsonObjectBuilder(members);
    }

    public bool TryGet(string name, out JsonElement value)
    {
        foreach (KeyValuePair<string, JsonElement> member in _members)
        {
            if (member.Key == name)
            {
                value = member.Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    public void Project(IEnumerable<string> fields, string sequenceField)
    {
        HashSet<string> keep = new(fields, StringComparer.Ordinal)
        {
            sequenceField,
        };

        _members.RemoveAll(member => !keep.Contains(member.Key));
    }

    public void Rename(string from, string to, int lineNumber)
    {
        int fromIndex = -1;
        int toIndex = -1;
        for (int i = 0; i < _members.Count; i++)
        {
            if (_members[i].Key == from)
            {
                fromIndex = i;
            }
            else if (_members[i].Key == to)
            {
                toIndex = i;
            }
        }

        if (fromIndex < 0)
        {
            throw new PipelineException(
                PipelineErrorCodes.ValidationFailed,
                $"line {lineNumber}: cannot rename missing field '{from}'");
        }

        if (toIndex >= 0)
        {
            throw new PipelineException(
                PipelineErrorCodes.ValidationFailed,
                $"line {lineNumber}: rename target '{to}' already exists");
        }

        _members[fromIndex] = new KeyValuePair<string, JsonElement>(to, _members[fromIndex].Value);
    }

    public string ToJson()
    {
        ArrayBufferWriter<byte> buffer = new();
        using (Utf8JsonWriter writer = new(buffer, CanonicalJson.WriterOptions))
        {
            writer.WriteStartObject();
            foreach (KeyValuePair<string, JsonElement> member in _members)
            {
                writer.WritePropertyName(member.Key);
                CanonicalJson.WriteElement(writer, member.Value);
            }

            writer.WriteEndObject();
            writer.Flush();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}
