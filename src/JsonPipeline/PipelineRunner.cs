using System.Text;
using System.Threading.Channels;

namespace JsonPipeline;

internal readonly record struct WorkItem(long Sequence, int LineNumber, string Line);

internal readonly record struct TransformResult(long Sequence, bool Written, string? CanonicalLine, string? ErrorCode, string? ErrorMessage)
{
    public static TransformResult Failure(long sequence, string code, string message) =>
        new(sequence, false, null, code, message);
}

internal static class PipelineRunner
{
    public static async Task<PipelineStats> RunAsync(
        Stream input,
        PipelineConfig config,
        int workers,
        PipelineRunState state,
        CancellationToken cancellationToken)
    {
        int workerCount = Math.Min(workers, Math.Max(1, config.MaxInFlight));

        Channel<WorkItem> workChannel = Channel.CreateBounded<WorkItem>(new BoundedChannelOptions(config.MaxInFlight)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true,
        });

        Channel<TransformResult> resultChannel = Channel.CreateBounded<TransformResult>(new BoundedChannelOptions(config.MaxInFlight)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false,
        });

        Task reader = Task.Run(() => ReadLoopAsync(input, config, workChannel.Writer, state, cancellationToken), cancellationToken);

        Task[] transformers = Enumerable.Range(0, workerCount)
            .Select(_ => Task.Run(() => TransformLoopAsync(workChannel.Reader, resultChannel.Writer, config, state, cancellationToken), cancellationToken))
            .ToArray();

        Task writer = Task.Run(() => WriteLoopAsync(resultChannel.Reader, config, state, cancellationToken), cancellationToken);

        Exception? failure = null;
        try
        {
            await reader.ConfigureAwait(false);
            workChannel.Writer.Complete();
            await Task.WhenAll(transformers).ConfigureAwait(false);
            resultChannel.Writer.Complete();
            await writer.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            failure = ex;
            workChannel.Writer.TryComplete(ex);
            resultChannel.Writer.TryComplete(ex);

            Task all = Task.WhenAll(new[] { reader, writer }.Concat(transformers));
            Task finished = await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(5), cancellationToken)).ConfigureAwait(false);
            if (finished != all)
            {
                throw;
            }
        }

        if (failure is PipelineException pipelineFailure)
        {
            throw pipelineFailure.InnerException is PipelineException inner
                ? inner
                : pipelineFailure;
        }

        if (failure is not null)
        {
            throw new PipelineException(PipelineErrorCodes.InvalidJson, failure.Message, failure);
        }

        return state.BuildStats();
    }

    private static async Task ReadLoopAsync(
        Stream input,
        PipelineConfig config,
        ChannelWriter<WorkItem> writer,
        PipelineRunState state,
        CancellationToken cancellationToken)
    {
        using StreamReader streamReader = new(input, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, 1 << 16, leaveOpen: true);
        int lineNumber = 0;

        while (true)
        {
            string? line = await streamReader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (line is null)
            {
                break;
            }

            lineNumber++;
            if (line.Length == 0)
            {
                throw new PipelineException(
                    PipelineErrorCodes.EmptyLine,
                    $"line {lineNumber}: empty lines are not allowed");
            }

            int lineBytes = Encoding.UTF8.GetByteCount(line);
            if (lineBytes > config.MaxInputLineBytes)
            {
                throw new PipelineException(
                    PipelineErrorCodes.InputLineLimitExceeded,
                    $"line {lineNumber}: {lineBytes} bytes exceeds maxInputLineBytes {config.MaxInputLineBytes}");
            }

            long sequence = ReadSequence(line, lineNumber, config, state);

            await writer.WriteAsync(new WorkItem(sequence, lineNumber, line), cancellationToken).ConfigureAwait(false);
        }
    }

    private static long ReadSequence(string line, int lineNumber, PipelineConfig config, PipelineRunState state)
    {
        using System.Text.Json.JsonDocument document =
            System.Text.Json.JsonDocument.Parse(line, CanonicalJson.StrictOptions);
        if (document.RootElement.ValueKind != System.Text.Json.JsonValueKind.Object)
        {
            throw new PipelineException(
                PipelineErrorCodes.InvalidJson,
                $"line {lineNumber}: root value must be a JSON object");
        }

        if (!document.RootElement.TryGetProperty(config.SequenceField, out System.Text.Json.JsonElement idElement) ||
            idElement.ValueKind != System.Text.Json.JsonValueKind.Number ||
            !idElement.TryGetInt64(out long sequence))
        {
            throw new PipelineException(
                PipelineErrorCodes.InvalidSequence,
                $"line {lineNumber}: field '{config.SequenceField}' must be an integer sequence number");
        }

        long expected = state.ReserveNextSequence();
        if (sequence < expected)
        {
            throw new PipelineException(
                PipelineErrorCodes.DuplicateSequence,
                $"line {lineNumber}: sequence {sequence} repeats or regresses (expected {expected})");
        }

        if (sequence != expected)
        {
            throw new PipelineException(
                PipelineErrorCodes.InvalidSequence,
                $"line {lineNumber}: sequence gap; expected {expected} but found {sequence}");
        }

        return sequence;
    }

    private static async Task TransformLoopAsync(
        ChannelReader<WorkItem> reader,
        ChannelWriter<TransformResult> writer,
        PipelineConfig config,
        PipelineRunState state,
        CancellationToken cancellationToken)
    {
        await foreach (WorkItem item in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            TransformResult result;
            try
            {
                TransformOutcome outcome = RecordTransformer.Apply(item.Line, item.LineNumber, config);
                result = outcome.Written
                    ? new TransformResult(item.Sequence, true, outcome.CanonicalLine, null, null)
                    : new TransformResult(item.Sequence, false, null, null, null);
            }
            catch (PipelineException ex)
            {
                result = TransformResult.Failure(item.Sequence, ex.Code, ex.Message);
            }
            catch (System.Text.Json.JsonException ex)
            {
                result = TransformResult.Failure(
                    item.Sequence,
                    PipelineErrorCodes.InvalidJson,
                    $"line {item.LineNumber}: invalid JSON: {ex.Message}");
            }

            await writer.WriteAsync(result, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task WriteLoopAsync(
        ChannelReader<TransformResult> reader,
        PipelineConfig config,
        PipelineRunState state,
        CancellationToken cancellationToken)
    {
        Dictionary<long, TransformResult> pending = new();
        long expected = state.NextSequence;

        await foreach (TransformResult result in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            if (result.Sequence < state.NextSequence)
            {
                continue;
            }

            pending.Add(result.Sequence, result);
            while (pending.Remove(expected, out TransformResult ready))
            {
                if (ready.ErrorCode is not null)
                {
                    throw new PipelineException(ready.ErrorCode, ready.ErrorMessage ?? "record failed");
                }

                if (!ready.Written)
                {
                    state.RecordFiltered();
                    await state.Sink.UpdateCheckpointAsync(expected, state, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    byte[] payload = Encoding.UTF8.GetBytes(ready.CanonicalLine! + "\n");
                    if (state.TotalOutputBytes + payload.Length > config.MaxOutputBytes)
                    {
                        throw new PipelineException(
                            PipelineErrorCodes.OutputLimitExceeded,
                            $"output would be {state.TotalOutputBytes + payload.Length} bytes, exceeding maxOutputBytes {config.MaxOutputBytes}");
                    }

                    await state.Sink.WriteAsync(payload, expected, cancellationToken).ConfigureAwait(false);
                    state.RecordCommitted(payload.Length);
                }

                expected++;
            }
        }
    }
}
