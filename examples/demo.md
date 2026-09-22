# Demo

The runnable demo lives in `examples/demo`:

- `examples/demo/input.jsonl` — five customer records keyed by the monotonic `id` field.
- `examples/demo/pipeline.config.json` — validates `email`/`age`, filters to `country == "UK"`,
  projects five fields, then renames `email` to `contactEmail`.
- `examples/demo/output.jsonl` — canonical JSONL produced by the CLI.
- `examples/demo/.checkpoint.json` — atomic checkpoint used for `--resume`.

Run it from the repository root:

```bash
dotnet run --project src/JsonPipeline.Cli -- --demo
```

Equivalent explicit invocation:

```bash
dotnet run --project src/JsonPipeline.Cli -- \
  --input examples/demo/input.jsonl \
  --output examples/demo/output.jsonl \
  --config examples/demo/pipeline.config.json \
  --checkpoint examples/demo/.checkpoint.json \
  --workers 4
```

Re-running after completion with `--resume` is a deterministic no-op that returns the
same stats without rewriting the output. Re-running without `--resume` fails with
`CHECKPOINT_EXISTS`.
