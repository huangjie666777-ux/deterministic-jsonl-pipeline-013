using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace JsonPipeline;

public sealed class PipelineStats
{
    [JsonPropertyName("inputRecords")]
    public long InputRecords { get; set; }

    [JsonPropertyName("committedRecords")]
    public long CommittedRecords { get; set; }

    [JsonPropertyName("filteredRecords")]
    public long FilteredRecords { get; set; }

    [JsonPropertyName("outputBytes")]
    public long OutputBytes { get; set; }
}

public sealed record PipelineRunResult(
    long InputRecords,
    long CommittedRecords,
    long FilteredRecords,
    long OutputBytes,
    bool Resumed,
    bool Completed)
{
    public PipelineStats ToStats() => new()
    {
        InputRecords = InputRecords,
        CommittedRecords = CommittedRecords,
        FilteredRecords = FilteredRecords,
        OutputBytes = OutputBytes,
    };
}

public sealed record PipelineRunOptions(
    string? InputPath = null,
    string? OutputPath = null,
    string? ConfigPath = null,
    string? CheckpointPath = null,
    int Workers = 2,
    bool Resume = false)
{
    public int Workers { get; init; } = Workers;
    internal long? CrashAfterSequence { get; init; }
}

internal sealed class CheckpointState
{
    [JsonPropertyName("version")]
    public int Version { get; set; } = 1;

    [JsonPropertyName("status")]
    public string Status { get; set; } = "running";

    [JsonPropertyName("inputSha256")]
    public string InputSha256 { get; set; } = "";

    [JsonPropertyName("configSha256")]
    public string ConfigSha256 { get; set; } = "";

    [JsonPropertyName("committedSequence")]
    public long CommittedSequence { get; set; }

    [JsonPropertyName("tempOutputPath")]
    public string TempOutputPath { get; set; } = "";

    [JsonPropertyName("tempOutputSha256")]
    public string TempOutputSha256 { get; set; } = "";

    [JsonPropertyName("tempOutputBytes")]
    public long TempOutputBytes { get; set; }

    [JsonPropertyName("stats")]
    public PipelineStats Stats { get; set; } = new();
}

internal static class CheckpointStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    public static CheckpointState Read(string path)
    {
        byte[] bytes;
        try
        {
            bytes = File.ReadAllBytes(path);
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            throw new PipelineException(
                PipelineErrorCode.InvalidResumeState,
                $"checkpoint not found: {path}",
                ex);
        }
        catch (IOException ex)
        {
            throw new PipelineException(PipelineErrorCode.IoError, $"cannot read checkpoint {path}: {ex.Message}", ex);
        }

        CheckpointState? state;
        try
        {
            state = JsonSerializer.Deserialize<CheckpointState>(bytes, JsonOptions);
        }
        catch (JsonException ex)
        {
            throw new PipelineException(
                PipelineErrorCode.CheckpointCorrupt,
                $"checkpoint {path} is not valid JSON: {ex.Message}",
                ex);
        }

        if (state is null
            || state.Version != 1
            || string.IsNullOrWhiteSpace(state.InputSha256)
            || string.IsNullOrWhiteSpace(state.ConfigSha256)
            || string.IsNullOrWhiteSpace(state.TempOutputPath))
        {
            throw new PipelineException(PipelineErrorCode.CheckpointCorrupt, $"checkpoint {path} is missing required fields");
        }

        if (state.Status is not ("running" or "complete"))
        {
            throw new PipelineException(PipelineErrorCode.CheckpointCorrupt, $"checkpoint {path} has unknown status '{state.Status}'");
        }

        return state;
    }

    public static void WriteAtomic(string path, CheckpointState state)
    {
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(state, JsonOptions);
        string? directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        string tempPath = $"{path}.{Environment.CurrentManagedThreadId}.tmp";
        try
        {
            using (FileStream stream = new(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                stream.Write(bytes);
                stream.Flush(true);
            }
            File.Move(tempPath, path, overwrite: true);
        }
        catch (IOException ex)
        {
            throw new PipelineException(PipelineErrorCode.IoError, $"cannot write checkpoint {path}: {ex.Message}", ex);
        }
        finally
        {
            TryDelete(tempPath);
        }
    }

    public static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch (IOException)
        {
        }
    }

    public static string Sha256Hex(byte[] bytes)
    {
        byte[] hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    public static string ComputeStreamSha256(Stream stream)
    {
        using SHA256 sha = SHA256.Create();
        return Convert.ToHexString(sha.ComputeHash(stream)).ToLowerInvariant();
    }
}
