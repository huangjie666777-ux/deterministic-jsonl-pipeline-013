using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace JsonPipeline;

public sealed class CheckpointFile
{
    [JsonPropertyName("version")]
    public int Version { get; set; } = 1;

    [JsonPropertyName("inputSha256")]
    public string InputSha256 { get; set; } = "";

    [JsonPropertyName("configSha256")]
    public string ConfigSha256 { get; set; } = "";

    [JsonPropertyName("committedSequence")]
    public long CommittedSequence { get; set; } = -1;

    [JsonPropertyName("tempOutputPath")]
    public string TempOutputPath { get; set; } = "";

    [JsonPropertyName("tempOutputSha256")]
    public string TempOutputSha256 { get; set; } = HashOf.Empty;

    [JsonPropertyName("outputSha256")]
    public string OutputSha256 { get; set; } = "";

    [JsonPropertyName("stats")]
    public PipelineStats Stats { get; set; } = new();

    [JsonPropertyName("completed")]
    public bool Completed { get; set; }
}

internal static class HashOf
{
    public const string Empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    public static string Sha256Hex(byte[] bytes)
    {
        byte[] hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    public static string Sha256Hex(Stream stream)
    {
        using SHA256 sha = SHA256.Create();
        byte[] hash = sha.ComputeHash(stream);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    public static string Sha256HexOfFile(string path)
    {
        using FileStream stream = new(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        return Sha256Hex(stream);
    }
}

internal static class AtomicFile
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
    };

    public static void WriteAllTextAtomic(string path, string content)
    {
        string staging = path + ".write-" + ToShortHash(path);
        using (FileStream stream = new(staging, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            byte[] bytes = System.Text.Encoding.UTF8.GetBytes(content);
            stream.Write(bytes);
            stream.Flush(true);
        }

        File.Move(staging, path, overwrite: true);
    }

    public static void WriteJsonAtomic<T>(string path, T value)
    {
        string content = JsonSerializer.Serialize(value, JsonOptions);
        WriteAllTextAtomic(path, content);
    }

    public static T ReadJson<T>(string path)
    {
        try
        {
            using FileStream stream = new(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            T? value = JsonSerializer.Deserialize<T>(stream, JsonOptions);
            if (value is null)
            {
                throw new PipelineException(PipelineErrorCodes.CheckpointCorrupt, $"checkpoint '{path}' is null");
            }

            return value;
        }
        catch (Exception ex) when (ex is JsonException or IOException)
        {
            throw new PipelineException(PipelineErrorCodes.CheckpointCorrupt, $"checkpoint '{path}' cannot be read: {ex.Message}");
        }
    }

    private static string ToShortHash(string text)
    {
        byte[] hash = SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text));
        return Convert.ToHexString(hash, 0, 6).ToLowerInvariant();
    }
}
