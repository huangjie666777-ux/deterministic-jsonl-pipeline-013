# JsonPipeline

Deterministic, resumable JSONL processing for .NET 8 / C# 12.

- Bounded-concurrent read/transform/write stages via `System.Threading.Channels`
- Records carry a monotonic sequence; results commit strictly in input order
- Configurable validation, projection, field rename and filters
- Stable normalized JSONL output independent of worker count or scheduling
- Limits: input line bytes, in-flight records, total output bytes, config size
- Atomic checkpoints with input/config SHA-256, committed sequence, temp output
  path/hash/bytes and normalized stats; `--resume` verifies everything
- Final output is flushed/fsync'ed and atomically renamed after checkpointing

See `examples/demo.md` for a runnable demo.
