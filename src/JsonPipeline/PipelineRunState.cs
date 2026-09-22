namespace JsonPipeline;

internal interface IRecordSink
{
    ValueTask WriteAsync(byte[] payload, long sequence, CancellationToken cancellationToken);
    ValueTask UpdateCheckpointAsync(long sequence, PipelineRunState state, CancellationToken cancellationToken);
}

internal sealed class InMemorySink : IRecordSink
{
    private readonly Stream _stream;

    public InMemorySink(Stream stream) => _stream = stream;

    public async ValueTask WriteAsync(byte[] payload, long sequence, CancellationToken cancellationToken)
    {
        await _stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask UpdateCheckpointAsync(long sequence, PipelineRunState state, CancellationToken cancellationToken)
        => ValueTask.CompletedTask;
}

internal sealed class FileSink : IRecordSink
{
    private readonly FileStream _stream;
    private readonly CheckpointStore _checkpointStore;
    private readonly PipelineRunState _state;

    public FileSink(FileStream stream, CheckpointStore checkpointStore, PipelineRunState state)
    {
        _stream = stream;
        _checkpointStore = checkpointStore;
        _state = state;
    }

    public async ValueTask WriteAsync(byte[] payload, long sequence, CancellationToken cancellationToken)
    {
        await _stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
        await _stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        _checkpointStore.RecordCommit(sequence, _state.BuildStats());
        _checkpointStore.WriteAtomic();
    }

    public ValueTask UpdateCheckpointAsync(long sequence, PipelineRunState state, CancellationToken cancellationToken)
    {
        _checkpointStore.RecordCommit(sequence, state.BuildStats());
        _checkpointStore.WriteAtomic();
        return ValueTask.CompletedTask;
    }
}

internal sealed class PipelineRunState
{
    private readonly object _gate = new();
    private long _reserveCursor;

    public PipelineRunState(
        string inputHash,
        string configHash,
        long totalInputRecords,
        IRecordSink sink,
        CheckpointFile? checkpoint)
    {
        InputHash = inputHash;
        ConfigHash = configHash;
        TotalInputRecords = totalInputRecords;
        Sink = sink;
        Checkpoint = checkpoint;
        long start = checkpoint is null || checkpoint.Completed ? 0 : checkpoint.CommittedSequence + 1;
        NextSequence = start;
        _reserveCursor = 0;
        LastCommittedSequence = start - 1;
        FilteredRecords = checkpoint?.Stats.FilteredRecords ?? 0;
        CommittedRecords = checkpoint?.Stats.CommittedRecords ?? 0;
        TotalOutputBytes = checkpoint?.Stats.OutputBytes ?? 0;
    }

    public void SetSink(IRecordSink sink)
    {
        Sink = sink;
    }

    public string InputHash { get; }
    public string ConfigHash { get; }
    public long TotalInputRecords { get; }
    public IRecordSink Sink { get; private set; }
    public CheckpointFile? Checkpoint { get; }

    public long NextSequence { get; }
    public long LastCommittedSequence { get; private set; }
    public long FilteredRecords { get; private set; }
    public long CommittedRecords { get; private set; }
    public long TotalOutputBytes { get; private set; }

    public long ReserveNextSequence()
    {
        lock (_gate)
        {
            return _reserveCursor++;
        }
    }

    public void RecordFiltered()
    {
        lock (_gate)
        {
            FilteredRecords++;
            LastCommittedSequence = NextSequence + CommittedRecords + FilteredRecords - 1;
        }
    }

    public void RecordCommitted(long bytes)
    {
        lock (_gate)
        {
            CommittedRecords++;
            TotalOutputBytes += bytes;
            LastCommittedSequence = NextSequence + CommittedRecords + FilteredRecords - 1;
        }
    }

    public PipelineStats BuildStats() => new()
    {
        InputRecords = TotalInputRecords,
        CommittedRecords = CommittedRecords,
        FilteredRecords = FilteredRecords,
        OutputBytes = TotalOutputBytes,
    };
}
