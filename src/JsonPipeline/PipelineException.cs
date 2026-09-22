namespace JsonPipeline;

public sealed class PipelineException : Exception
{
    public PipelineException(string code, string message)
        : base($"{code}: {message}")
    {
        Code = code;
    }

    public PipelineException(string code, string message, Exception innerException)
        : base($"{code}: {message}", innerException)
    {
        Code = code;
    }

    public string Code { get; }
}

public static class PipelineErrorCodes
{
    public const string EmptyLine = "EMPTY_LINE";
    public const string InvalidJson = "INVALID_JSON";
    public const string InvalidSequence = "INVALID_SEQUENCE";
    public const string DuplicateSequence = "DUPLICATE_SEQUENCE";
    public const string ValidationFailed = "VALIDATION_FAILED";
    public const string UnknownOperation = "CONFIG_UNKNOWN_OPERATION";
    public const string InvalidConfig = "INVALID_CONFIG";
    public const string ConfigLimitExceeded = "CONFIG_LIMIT_EXCEEDED";
    public const string InputLineLimitExceeded = "INPUT_LINE_LIMIT_EXCEEDED";
    public const string InFlightLimitExceeded = "INFLIGHT_LIMIT_EXCEEDED";
    public const string OutputLimitExceeded = "OUTPUT_LIMIT_EXCEEDED";
    public const string CheckpointInputMismatch = "CHECKPOINT_INPUT_MISMATCH";
    public const string CheckpointConfigMismatch = "CHECKPOINT_CONFIG_MISMATCH";
    public const string CheckpointTempTampered = "CHECKPOINT_TEMP_TAMPERED";
    public const string CheckpointCorrupt = "CHECKPOINT_CORRUPT";
    public const string CheckpointExists = "CHECKPOINT_EXISTS";
    public const string CheckpointCompleted = "CHECKPOINT_ALREADY_COMPLETED";
    public const string Usage = "USAGE";
}
