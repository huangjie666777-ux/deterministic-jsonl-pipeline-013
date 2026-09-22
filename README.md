# JsonPipeline

Deterministic, resumable JSONL processing for .NET 8 and C# 12.

The pipeline reads JSONL line by line with a bounded `System.Threading.Channels`
read/transform/write pipeline. Records carry a configured monotonic sequence field
(default `id`); worker results are reordered by sequence before a single writer
commits them, so output bytes are independent of worker count and scheduling.

Operations are configured via JSON:

- `validate` — required field with type `string|number|boolean|object|array` and optional numeric `min`/`max`.
- `project` — keep only the listed fields (the sequence field is always retained).
- `rename` — rename a field; missing source or colliding target fails the run.
- `filter` — keep records matching `equals`, numeric `min`/`max`, or `type`; supports `negate`.

Limits enforced per run: `maxInputLineBytes`, `maxInFlight`, `maxOutputBytes`,
`maxOperations`, plus a 1 MiB config cap. Empty lines, invalid JSON, duplicate or
gapped sequences, unknown operations and type errors fail with stable error codes.

Checkpoints atomically record input/config SHA-256, the last committed sequence,
the temp output path plus its SHA-256, and canonical stats. `--resume` only
continues when both hashes match and the temp output is untampered; final output
is flushed and atomically moved into place only after the completed checkpoint is
written.

CLI:

```bash
dotnet run --project src/JsonPipeline.Cli -- \
  --input data.jsonl --output out.jsonl --config pipeline.config.json \
  --checkpoint run.checkpoint.json --workers 4 [--resume]
```

See `examples/demo.md` for a worked example.
