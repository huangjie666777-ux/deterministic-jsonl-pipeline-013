using System.Security.Cryptography;
using System.Text.Json;

namespace JsonPipeline;

internal sealed class CheckpointStore
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };
    private readonly string _path;
    private readonly CheckpointFile _checkpoint;
    private readonly object _gate = new();

    public CheckpointStore(string path, CheckpointFile checkpoint)
    {
        _path = path;
        _checkpoint = checkpoint;
    }

    public static CheckpointFile Read(string path)
    {
        try
        {
            using FileStream stream = new(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            CheckpointFile? value = JsonSerializer.Deserialize<CheckpointFile>(stream, JsonOptions);
            if (value is null)
            {
                throw new PipelineException(PipelineErrorCodes.CheckpointCorrupt, $"checkpoint '{path}' deserialized to null");
            }

            return value;
        }
        catch (Exception ex) when (ex is JsonException or IOException)
        {
            throw new PipelineException(PipelineErrorCodes.CheckpointCorrupt, $"checkpoint '{path}' cannot be read: {ex.Message}");
        }
    }

    public static void WriteAtomic(string path, CheckpointFile checkpoint)
    {
        new CheckpointStore(path, checkpoint).WriteAtomic();
    }

    public void RecordCommit(long sequence, PipelineStats stats)
    {
        lock (_gate)
        {
            _checkpoint.CommittedSequence = sequence;
            _checkpoint.Stats = stats;
        }
    }

    public void WriteAtomic()
    {
        lock (_gate)
        {
            _checkpoint.TempOutputSha256 = ComputeFileHash(_checkpoint.TempOutputPath);
            string content = JsonSerializer.Serialize(_checkpoint, JsonOptions);
            string staging = _path + ".write-" + ShortHash(_path);
            using (FileStream stagingStream = new(staging, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                byte[] bytes = System.Text.Encoding.UTF8.GetBytes(content);
                stagingStream.Write(bytes);
                stagingStream.Flush(true);
            }

            File.Move(staging, _path, overwrite: true);
        }
    }

    private static string ComputeFileHash(string path)
    {
        using FileStream stream = new(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        byte[] hash = SHA256.HashData(stream);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    private static string ShortHash(string text)
    {
        byte[] hash = SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text));
        return Convert.ToHexString(hash, 0, 6).ToLowerInvariant();
    }
}
