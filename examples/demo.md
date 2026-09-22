# Demo

A deterministic JSONL pipeline using projection, rename and filters.

Input: `examples/demo/input.jsonl` (5 customer records).

Config: `examples/demo/config.json`

- `validate`: requires string `id` and integer `age`
- `filter`: keeps adults (`age >= 18`) with `active == true`
- `project`: keeps `seq`, `id`, `name`, `age`, `city`
- `rename`: `id` -> `customerId`

Run (from the repository root):

```sh
dotnet run --project src/JsonPipeline.Cli -- \
  --input examples/demo/input.jsonl \
  --output examples/demo/output.jsonl \
  --config examples/demo/config.json \
  --checkpoint examples/demo/checkpoint.json \
  --workers 4
```

Resume an interrupted run:

```sh
dotnet run --project src/JsonPipeline.Cli -- \
  --input examples/demo/input.jsonl \
  --output examples/demo/output.jsonl \
  --config examples/demo/config.json \
  --checkpoint examples/demo/checkpoint.json \
  --workers 4 --resume
```

Expected output (`examples/demo/output.jsonl`):

```jsonl
{"seq":1,"customerId":"a1","name":"Alice","age":30,"city":"Shanghai"}
{"seq":4,"customerId":"d4","name":"Dan","age":25,"city":"Hangzhou"}
```
