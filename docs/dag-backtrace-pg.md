# PG-backed DAG Backtrace

`whowhen_eval.run_dag_pg` adapts the upstream DAG Backtrace workflow to the
Who&When Pro text data already stored in PostgreSQL. Trace content is loaded
from the root span's lossless `source.raw_json`; the canonical snapshot is used
only for offline scoring. DAG results are written separately from all-at-once
results under `results/dag_backtrace_pg/<model>/<benchmark>.jsonl`.

The adapter maps `task.query` to the task question and converts the ordered
`trajectory` into zero-based DAG steps. A `round` containing multiple turns is
expanded into one DAG step per turn; every predicted step keeps a
`trajectory_index` / optional `turn_index` crosswalk. Deterministic child span
IDs are regenerated from the source events and checked against the IDs returned
by PostgreSQL before any model request is made.

Attribution labels must not enter model input. The adapter removes
`ground_truth`, failure-label fields, and trajectory injection markers
(`is_injected`, `injection_via`, `phase`, replay flags, and related fields)
recursively. The PG query itself selects only the raw envelope and span IDs,
not `failure.*` metadata. The upstream `joint` start strategy is intentionally
not used: it accepts a `ground_truth` field, which has a different, label-bearing
meaning in Who&When Pro. This runner uses a label-blind model start.

The upstream method returns agent and step, but does not classify the error
mode. Consequently, this protocol reports actor and when scores; `mode` and
joint `all` are marked not applicable rather than scored as incorrect. Add a
separate mode-classification extension only if that additional task is
deliberately included in the experiment.

Validate PG access, sanitization, and coordinate mappings without API calls:

```bash
python -m whowhen_eval.run_dag_pg \
  --model DeepSeek-V4.1-Flash \
  --canonical-snapshot /path/to/canonical-labels.jsonl \
  --pg-dsn 'host=127.0.0.1 port=25432 dbname=whowhen user=trace' \
  --max-traces 3 --dry-run
```

To run a small real sample against an OpenAI-compatible custom endpoint, use
the LiteLLM `openai/` provider prefix and set `--api-base` / `--api-key-env`:

```bash
python -m whowhen_eval.run_dag_pg \
  --model openai/DeepSeek-V4.1-Flash \
  --api-base "$DAG_API_BASE" \
  --api-key-env DEEPSEEK_API_KEY \
  --canonical-snapshot /path/to/canonical-labels.jsonl \
  --pg-dsn 'host=127.0.0.1 port=25432 dbname=whowhen user=trace' \
  --benchmark tablebench --max-traces 3 \
  --concurrency 1 --reasoning-effort low --max-tokens 4096
```

The provider prefix matters: passing only `DeepSeek-V4.1-Flash` makes LiteLLM
reject the request before it reaches the endpoint. Also, `--max-traces` is a
per-benchmark limit; use `--benchmark` or explicit repeated `--trace-id`
arguments when the intended sample size is a strict global total.

In the October 2026 pilot, a 3-step trace completed with a 4,096-token
per-request output cap (8,500 total tokens, 59 seconds), and a 6-step trace
completed with an 8,192-token per-request cap (14,734 total tokens, 78
seconds). An 8-step trace's graph-building call still returned no visible JSON
at an 8,192-token per-request cap. The default cap of 2,048 is therefore not
reliable for real runs, and increasing it alone does not guarantee that longer
traces complete. Do not start the full text set until graph-build prompting /
reasoning-budget behavior is addressed and re-tested. Successful cases are
skipped on resume; failed cases are retried. Pass `--no-resume` to re-run all
selected cases.
