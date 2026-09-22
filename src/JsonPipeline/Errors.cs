namespace JsonPipeline;

public enum PipelineErrorCode
{
    EmptyLine,
    InvalidJson,
    InvalidSequence,
    DuplicateSequence,
    InvalidConfig,
    UnknownOperation,
    TypeMismatch,
    ValidationFailed,
    LineTooLong,
    OutputTooLarge,
    CheckpointCorrupt,
    InputHashMismatch,
    ConfigHashMismatch,
    TempOutputTampered,
    InvalidResumeState,
    IoError,
}

public sealed class PipelineException : Exception
{
    public PipelineErrorCode Code { get; }
    public long? LineNumber { get; }

    public PipelineException(PipelineErrorCode code, string message, long? lineNumber = null)
        : base(message)
    {
        Code = code;
        LineNumber = lineNumber;
    }

    public PipelineException(PipelineErrorCode code, string message, Exception inner, long? lineNumber = null)
        : base(message, inner)
    {
        Code = code;
        LineNumber = lineNumber;
    }

    public string StableCode => Code switch
    {
        PipelineErrorCode.EmptyLine => "EMPTY_LINE",
        PipelineErrorCode.InvalidJson => "INVALID_JSON",
        PipelineErrorCode.InvalidSequence => "INVALID_SEQUENCE",
        PipelineErrorCode.DuplicateSequence => "DUPLICATE_SEQUENCE",
        PipelineErrorCode.InvalidConfig => "INVALID_CONFIG",
        PipelineErrorCode.UnknownOperation => "UNKNOWN_OPERATION",
        PipelineErrorCode.TypeMismatch => "TYPE_MISMATCH",
        PipelineErrorCode.ValidationFailed => "VALIDATION_FAILED",
        PipelineErrorCode.LineTooLong => "LINE_TOO_LONG",
        PipelineErrorCode.OutputTooLarge => "OUTPUT_TOO_LARGE",
        PipelineErrorCode.CheckpointCorrupt => "CHECKPOINT_CORRUPT",
        PipelineErrorCode.InputHashMismatch => "INPUT_HASH_MISMATCH",
        PipelineErrorCode.ConfigHashMismatch => "CONFIG_HASH_MISMATCH",
        PipelineErrorCode.TempOutputTampered => "TEMP_OUTPUT_TAMPERED",
        PipelineErrorCode.InvalidResumeState => "INVALID_RESUME_STATE",
        PipelineErrorCode.IoError => "IO_ERROR",
        _ => "INTERNAL",
    };
}
