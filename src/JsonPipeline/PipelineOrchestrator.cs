using System.Text.Json;
using System.Threading.Channels;

namespace JsonPipeline;

internal sealed record ResumeContext(
    long CommittedSequence,
    PipelineStats Stats,
    long InputRecords);

internal readonly record struct RawLine(byte[] Bytes, int RawLength);

internal readonly record struct WorkItem(long Ordinal, long LineNumber, long Sequence, byte[]? Payload, bool Dropped, Exception? Error)
{
    public static WorkItem Success(long ordinal, long lineNumber, long sequence, byte[]? payload, bool dropped) =>
        new(ordinal, lineNumber, sequence, payload, dropped, null);
}

internal sealed class PipelineOrchestrator
{
    private readonly PipelineConfig _config;
    private readonly int _workers;

    public PipelineOrchestrator(PipelineConfig config, int workers)
    {
        if (workers is < 1 or > 1024)
        {
            throw new PipelineException(PipelineErrorCode.InvalidConfig, "workers must be in 1..1024");
        }
        _config = config;
        _workers = workers;
    }

    public async Task<PipelineStats> RunAsync(
        Stream input,
        Stream? output,
        ResumeContext? resume,
        Func<WorkItem, PipelineStats, ValueTask>? checkpointAsync,
        CancellationToken cancellationToken)
    {
        int capacity = _config.Limits.MaxInFlight;
        Channel<RawRecord> inputChannel = Channel.CreateBounded<RawRecord>(new BoundedChannelOptions(capacity)
        {
            SingleReader = false,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });
        Channel<WorkItem> resultChannel = Channel.CreateBounded<WorkItem>(new BoundedChannelOptions(capacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait,
        });

        using CancellationTokenSource failureCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        Task readerTask = Task.Run(() => ReadLoopAsync(input, inputChannel.Writer, resume, failureCts.Token));
        Task[] workerTasks = Enumerable.Range(0, _workers)
            .Select(_ => Task.Run(() => TransformLoopAsync(inputChannel.Reader, resultChannel.Writer, failureCts.Token)))
            .ToArray();
        Task completerTask = Task.Run(async () =>
        {
            try
            {
                await Task.WhenAll(workerTasks).ConfigureAwait(false);
            }
            finally
            {
                resultChannel.Writer.TryComplete();
            }
        });
        Task<PipelineStats> committerTask = Task.Run(
            () => CommitLoopAsync(output, resultChannel.Reader, resume, checkpointAsync, failureCts));

        PipelineStats stats;
        try
        {
            stats = await committerTask.ConfigureAwait(false);
        }
        catch
        {
            failureCts.Cancel();
            await CancelAndDrain(readerTask, workerTasks).ConfigureAwait(false);
            await CancelAndDrain(completerTask, Array.Empty<Task>()).ConfigureAwait(false);
            throw;
        }

        failureCts.Cancel();
        Exception? firstError = await CancelAndDrain(readerTask, workerTasks).ConfigureAwait(false);
        await CancelAndDrain(completerTask, Array.Empty<Task>()).ConfigureAwait(false);
        if (firstError is not null)
        {
            ThrowPreserving(firstError);
        }

        return stats;
    }

    private static async Task<Exception?> CancelAndDrain(Task readerTask, Task[] workerTasks)
    {
        Exception? first = null;
        foreach (Task task in new[] { readerTask }.Concat(workerTasks))
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException)
            {
            }
            catch (Exception ex)
            {
                first ??= ex;
            }
        }
        return first;
    }

    private static void ThrowPreserving(Exception ex)
    {
        if (ex is PipelineException)
        {
            throw ex;
        }
        if (ex is OperationCanceledException)
        {
            throw new OperationCanceledException(ex.Message, ex);
        }
        throw new PipelineException(PipelineErrorCode.IoError, ex.Message, ex);
    }

    private async Task ReadLoopAsync(
        Stream input,
        ChannelWriter<RawRecord> writer,
        ResumeContext? resume,
        CancellationToken cancellationToken)
    {
        long ordinal = 0;
        long lineNumber = 0;
        long lastSequence = long.MinValue;
        int maxLineBytes = _config.Limits.MaxLineBytes;
        string sequenceField = _config.SequenceField!;

        try
        {
            await using ByteLineReader reader = new(input);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                RawLine? line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
                if (line is null)
                {
                    break;
                }

                lineNumber++;
                byte[] bytes = line.Value.Bytes;
                if (bytes.Length == 0)
                {
                    throw new PipelineException(
                        PipelineErrorCode.EmptyLine,
                        $"line {lineNumber}: empty lines are not allowed",
                        lineNumber);
                }

                if (line.Value.RawLength > maxLineBytes)
                {
                    throw new PipelineException(
                        PipelineErrorCode.LineTooLong,
                        $"line {lineNumber}: {line.Value.RawLength} bytes exceeds maxLineBytes {maxLineBytes}",
                        lineNumber);
                }

                long sequence = ReadSequence(bytes, sequenceField, lineNumber);
                if (sequence <= lastSequence)
                {
                    throw new PipelineException(
                        PipelineErrorCode.DuplicateSequence,
                        $"line {lineNumber}: sequence {sequence} duplicates or precedes prior sequence {lastSequence}",
                        lineNumber);
                }
                lastSequence = sequence;

                ordinal++;
                if (resume is null || sequence > resume.CommittedSequence)
                {
                    await writer.WriteAsync(new RawRecord(ordinal, lineNumber, sequence, bytes), cancellationToken).ConfigureAwait(false);
                }
            }

            writer.Complete();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            writer.Complete(ex);
            throw;
        }
    }

    private static long ReadSequence(byte[] utf8Line, string sequenceField, long lineNumber)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(utf8Line);
        }
        catch (JsonException ex)
        {
            throw new PipelineException(
                PipelineErrorCode.InvalidJson,
                $"line {lineNumber}: invalid JSON: {ex.Message}",
                ex,
                lineNumber);
        }

        using (document)
        {
            JsonElement root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                throw new PipelineException(
                    PipelineErrorCode.TypeMismatch,
                    $"line {lineNumber}: record must be a JSON object",
                    lineNumber);
            }

            if (!root.TryGetProperty(sequenceField, out JsonElement sequenceElement))
            {
                throw new PipelineException(
                    PipelineErrorCode.InvalidSequence,
                    $"line {lineNumber}: missing sequence field '{sequenceField}'",
                    lineNumber);
            }

            if (sequenceElement.ValueKind != JsonValueKind.Number
                || !sequenceElement.TryGetInt64(out long sequence)
                || sequence < 0)
            {
                throw new PipelineException(
                    PipelineErrorCode.InvalidSequence,
                    $"line {lineNumber}: field '{sequenceField}' must be a non-negative integer",
                    lineNumber);
            }

            return sequence;
        }
    }

    private async Task TransformLoopAsync(
        ChannelReader<RawRecord> reader,
        ChannelWriter<WorkItem> writer,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (RawRecord record in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                WorkItem item;
                try
                {
                    TransformOutput transformed = Transformer.Process(record, _config);
                    item = WorkItem.Success(record.Ordinal, record.LineNumber, record.Sequence, transformed.Payload, transformed.Dropped);
                }
                catch (DropRecordException)
                {
                    item = WorkItem.Success(record.Ordinal, record.LineNumber, record.Sequence, null, true);
                }
                catch (PipelineException ex)
                {
                    item = new WorkItem(record.Ordinal, record.LineNumber, record.Sequence, null, false, ex);
                }

                await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (ChannelClosedException)
        {
        }
    }
    private async Task<PipelineStats> CommitLoopAsync(
        Stream? output,
        ChannelReader<WorkItem> reader,
        ResumeContext? resume,
        Func<WorkItem, PipelineStats, ValueTask>? checkpointAsync,
        CancellationTokenSource failureCts)
    {
        PipelineStats stats = resume?.Stats.Clone() ?? new PipelineStats();
                long nextOrdinal = resume?.InputRecords ?? 0;
                Dictionary<long, WorkItem> pending = new();
                long maxOutputBytes = _config.Limits.MaxOutputBytes;

        Exception? fatal = null;
        await foreach (WorkItem item in reader.ReadAllAsync(CancellationToken.None).ConfigureAwait(false))
        {
            if (fatal is not null)
            {
                continue;
            }

            pending[item.Ordinal] = item;
            while (pending.Remove(nextOrdinal + 1, out WorkItem ready))
            {
                nextOrdinal++;
                stats.InputRecords++;

                if (ready.Error is not null)
                {
                    failureCts.Cancel();
                    fatal = ready.Error;
                    break;
                }

                if (ready.Dropped)
                {
                    stats.FilteredRecords++;
                }
                else
                {
                    byte[] payload = ready.Payload!;
                    long bytesAfterWrite = stats.OutputBytes + payload.Length;
                    if (bytesAfterWrite > maxOutputBytes)
                    {
                        failureCts.Cancel();
                        fatal = new PipelineException(
                            PipelineErrorCode.OutputTooLarge,
                            $"output would reach {stats.OutputBytes + payload.Length} bytes, exceeding maxOutputBytes {maxOutputBytes}",
                            ready.LineNumber);
                        break;
                    }

                    if (output is not null)
                    {
                        await output.WriteAsync(payload).ConfigureAwait(false);
                    }
                    stats.OutputBytes += payload.Length;
                    stats.CommittedRecords++;
                }

                if (checkpointAsync is not null)
                {
                    try
                    {
                        await checkpointAsync(ready, stats).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        failureCts.Cancel();
                        fatal = ex;
                        break;
                    }
                }
            }
        }

        if (fatal is not null)
        {
            ThrowPreserving(fatal);
        }
        if (pending.Count > 0)
        {
            throw new PipelineException(PipelineErrorCode.InvalidResumeState, "internal ordering gap before commit");
        }

        return stats;
    }
}

internal sealed class ByteLineReader : IAsyncDisposable
{
    private readonly Stream _stream;
    private readonly byte[] _buffer = new byte[81920];
    private int _position;
    private int _length;
    private bool _ended;

    public ByteLineReader(Stream stream) => _stream = stream;

    public async Task<RawLine?> ReadLineAsync(CancellationToken cancellationToken)
    {
        using MemoryStream line = new();
        int rawLength = 0;

        while (true)
        {
            if (_position >= _length)
            {
                if (_ended)
                {
                    return line.Length == 0 && rawLength == 0 ? null : new RawLine(line.ToArray(), rawLength);
                }

                _position = 0;
                _length = await _stream.ReadAsync(_buffer.AsMemory(0), cancellationToken).ConfigureAwait(false);
                if (_length == 0)
                {
                    _ended = true;
                    return line.Length == 0 && rawLength == 0 ? null : new RawLine(line.ToArray(), rawLength);
                }
            }

            int start = _position;
            int newline = Array.IndexOf(_buffer, (byte)'\n', _position, _length - _position);
            int end = newline >= 0 ? newline : _length;
            int count = end - start;
            rawLength += count + (newline >= 0 ? 1 : 0);
            int contentLength = count;
            if (contentLength > 0 && _buffer[start + contentLength - 1] == (byte)'\r')
            {
                contentLength--;
            }
            if (contentLength > 0)
            {
                line.Write(_buffer, start, contentLength);
            }
            _position = end + (newline >= 0 ? 1 : 0);

            if (newline >= 0)
            {
                return new RawLine(line.ToArray(), rawLength);
            }
        }
    }

    public ValueTask DisposeAsync() => default;
}
