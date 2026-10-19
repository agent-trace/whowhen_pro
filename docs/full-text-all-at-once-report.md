# Text 全量 All-at-Once Benchmark 运行报告

更新日期：2026-10-19（Asia/Shanghai）

## 执行结论

本地 text 全量数据共 6,257 条，分别完成过两套 All-at-Once 运行：

1. **JSON 原始协议（2026-10-10）**：从发布版 `data/text.jsonl` 读取，使用 `DeepSeek-V4-Flash-0731`。按仓库 leaderboard 复算，Who / When / What / All 为 **45.3 / 49.9 / 13.7 / 17.2**。有 288 条 API 错误；leaderboard 会排除带 `error` 的行，因此分数只基于其余 5,969 条有效响应。
2. **PostgreSQL canonical 协议（2026-10-13）**：通过 Python `psycopg` 从 PostgreSQL 查询 trace，使用 `DeepSeek-V4.1-Flash`。最终覆盖 6,257 条，Actor / When / Error-mode macro-F1 / Joint 为 **72.1 / 67.9 / 12.7 / 21.9**。该协议的 `When` 是 canonical 目标 span ID 精确匹配。

两次运行使用了不同模型、数据读取路径、坐标协议和评分聚合方式，**不能把两组分数解释为同一设置下的模型提升或退化**。下文分别记录。

## 数据与运行环境

- 执行环境：WSL2，Ubuntu 24.04.4 LTS，`andy` 用户；模型推理经 OpenAI-compatible API 完成。
- 数据版本：Hugging Face `Leoxx/whowhen_pro` revision `0bd196c8a040841c4ae167ab33cc8151de246f1f`。
- 本地 `data/text.jsonl`：6,257 行；SHA-256 `51403bfd1b6570d0a9f8e62d25a50aa62504cb1264c33a3627ead893b7e2dd51`；taxonomy 有 17 个 mode。
- PostgreSQL：本机 Docker 容器 `postgres:18.6`（`whowhen-pg18`）；text 数据的 `events_dedup` 视图包含 6,257 条 trace、48,534 个 span。
- API 凭证只从运行环境读取；本报告及仓库不保存 API key。原始逐条结果含评分标签，未上传到 Git。

text split 的 6,257 条记录分布在 16 个 benchmark：

| Benchmark | 条数 | Benchmark | 条数 |
| --- | ---: | --- | ---: |
| ALFWorld | 311 | BigCodeBench | 195 |
| DA-Bench | 327 | DA-Code | 306 |
| DataBench | 818 | DeepSearch QA | 830 |
| GAIA | 302 | GPQA | 143 |
| HumanEval | 18 | KramaBench | 82 |
| LiveCodeBench-Pro | 129 | MATH | 448 |
| MMLU-Pro | 371 | SciBench | 555 |
| SimpleQA-Verified | 509 | TableBench | 913 |

## 运行 A：发布版 JSONL All-at-Once

| 项目 | 记录 |
| --- | --- |
| 日期 / run ID | 2026-10-10 / `20261010T_deepseek_v4_flash_text_full_c64` |
| 方法 | Who&When Pro 发布版 All-at-Once；从 JSONL 读取并渲染框架原生 trace |
| 模型 | `openai/DeepSeek-V4-Flash-0731` |
| 关键参数 | concurrency 64；temperature 0；max tokens 512；reasoning effort `none`；timeout 600 s；最多 5 次 provider retry |
| 覆盖 | 16 个 text benchmark，6,257 个唯一 trace |
| 请求记录 | 6,257；API 错误 288；解析 warning 1,720 |
| Token 用量 | input 24,910,660；output 30,821,758；合计 **55,732,418** |
| 结果时间戳跨度 | 2026-10-10 17:44 至 2026-10-11 05:32（本地时间），约 11 小时 48 分；这是结果记录的时间跨度，不是独立采集的进程 wall-clock |
| 结果目录 | `/home/andy/work/whowhen-pro-all-at-once/results/20261010T_deepseek_v4_flash_text_full_c64/openai_DeepSeek-V4-Flash-0731/` |
| 日志 / 配置 | `logs/20261010T_deepseek_v4_flash_text_full_c64.log`；`run-config.20261010T_deepseek_v4_flash_text_full_c64.yaml`（均在本机工作目录） |

用仓库官方 leaderboard 计算的 text 指标：

| Who（Agent） | When（Step） | What（Error macro-F1） | All（Joint） |
| ---: | ---: | ---: | ---: |
| 45.3% | 49.9% | 13.7% | 17.2% |

评分按框架先算再宏平均；Who 排除单 agent 框架。leaderboard 会跳过 API `error` 行，但解析 warning 行保留并按其预测参与评分。可在 harness 中复算：

```bash
python -m whowhen_eval.leaderboard \
  --results /home/andy/work/whowhen-pro-all-at-once/results/20261010T_deepseek_v4_flash_text_full_c64 \
  --modalities text
```

## 运行 B：PostgreSQL-backed canonical All-at-Once

| 项目 | 记录 |
| --- | --- |
| 日期 / run ID | 2026-10-13 / `20261013T_canonical_pg_text_full_v41_c16_16384` |
| 方法 | `canonical_db_all_at_once_v1`；Python `psycopg` 查询 `events_dedup`，组装 label-blind trace prompt；canonical snapshot 仅用于目标映射和离线评分 |
| 模型 | `openai/DeepSeek-V4.1-Flash` |
| 关键参数 | 全局调度；API concurrency 16；DB concurrency 16；temperature 0；max tokens 16,384；reasoning effort `none`；timeout 600 s；resume 开启 |
| 最终覆盖 | 16 个 text benchmark，6,257 个唯一 trace；相对 6,257 条预期 ID 全覆盖 |
| 最终记录质量 | API / PG error 字段为 0；48 条有空可见输出 / parse warning，仍保留在评分分母中并作为错误预测处理 |
| Token 用量 | input 31,959,079；output 37,136,263；合计 **69,095,342** |
| 结果时间戳跨度 | 2026-10-13 08:59 至 22:00（本地时间），约 13 小时；按结果记录时间戳估算，非独立进程计时 |
| 最终汇总目录 | `/home/andy/work/whowhen-pro-all-at-once/results/20261013T_canonical_pg_text_full_v41_final/openai_DeepSeek-V4.1-Flash/`（含逐 benchmark JSONL 与 `summary.json`） |
| 主运行日志 / 配置 | `logs/20261013T_canonical_pg_text_full_v41_c16_16384.log`；`run-config.20261013T_canonical_pg_text_full_v41_c16_16384.yaml` |

canonical 汇总指标（微平均 accuracy；Error 另为 17 类 macro-F1）：

| 指标 | 结果 | 分子 / 分母 |
| --- | ---: | ---: |
| Actor | 72.1% | 1,925 / 2,670（无 actor ground truth 的 trace 不纳入） |
| When（canonical target span） | 67.9% | 4,248 / 6,257 |
| Error mode accuracy | 26.8% | 1,675 / 6,257 |
| Error mode macro-F1 | 12.7% | 17 类 macro-F1 |
| Joint（All） | 21.9% | 1,368 / 6,257 |

运行配置等价命令如下。API 地址、key 和 PostgreSQL DSN 由本机环境变量提供；此处使用占位环境变量，不记录凭证：

```bash
cd /home/andy/work/whowhen-pro-all-at-once/harness
python -m whowhen_eval.run_pg \
  --model openai/DeepSeek-V4.1-Flash \
  --api-base "$DEEPSEEK_API_BASE" --api-key-env DEEPSEEK_API_KEY \
  --canonical-snapshot /home/andy/work/whowhen-trace-db/runtime/audit-whowhen-pro-text-phase1-final/canonical-labels.jsonl \
  --taxonomy-path /home/andy/work/whowhen-pro-all-at-once/data/taxonomy.yaml \
  --pg-dsn "$WHOWHEN_PG_DSN" --modality text --benchmark all \
  --schedule-scope global --concurrency 16 --db-concurrency 16 \
  --temperature 0 --max-tokens 16384 --reasoning-effort none \
  --out /home/andy/work/whowhen-pro-all-at-once/results/20261013T_canonical_pg_text_full_v41_c16_16384
```

复算最终 PG 汇总（原始结果仍留在本机）：

```bash
python -m whowhen_eval.summarize_canonical_pg \
  --primary /home/andy/work/whowhen-pro-all-at-once/results/20261013T_canonical_pg_text_full_v41_final/openai_DeepSeek-V4.1-Flash \
  --taxonomy-path /home/andy/work/whowhen-pro-all-at-once/data/taxonomy.yaml \
  --expected-trace-id-file /home/andy/work/whowhen-pro-all-at-once/runtime/full-text-v41/expected-trace-ids.txt \
  --out /tmp/canonical-pg-full-summary
```

## 与论文结果的对照

论文 v2 Table 4 的 DeepSeek-V4-Pro text 结果为 **52.0 / 68.7 / 16.6 / 17.5**（Agent / Step / Error / Joint）。本地 JSONL full run 的 DeepSeek-V4-Flash-0731 为 **45.3 / 49.9 / 13.7 / 17.2**；简单相减为 **-6.7 / -18.8 / -2.9 / -0.3 个百分点**。该差值仅作背景参照，不能作为模型优劣结论：模型不是同一版本，论文使用的基准发布/标签分布与本地固定数据 revision 不完全一致，本地请求还有 API 错误，且本地 taxonomy 为 17 类。

PG canonical 的 72.1 / 67.9 / 12.7 / 21.9 **不与论文表格直接比较**：其 `When` 比较 canonical span ID，不是发布版原生 step 坐标；其 Actor、When、Joint 为全体 eligible trace 上的微平均，论文按 benchmark/framework 的聚合规则计算；模型也换成了 V4.1-Flash。PG run 的主要价值是验证数据库读取与 canonical 标签映射下的端到端结果，不是复现论文 leaderboard。

论文来源：[Who&When Pro, arXiv:2607.09996 v2（2026-10-09），Table 4](https://arxiv.org/html/2607.09996v2)。

## 结果文件与复现边界

- 本报告是提交到代码仓库的公开汇总；两个 run 的逐条 JSONL、PG label snapshot、日志和配置仍保存在本机上述路径。
- 逐条 JSONL 含 ground truth、模型输出和逐条预测，属于审计/评分材料；仓库 `.gitignore` 排除了 `results/`，本次不上传这些原始记录或 API 凭证。
- 原运行日志为追加式记录，运行配置中的 `status` 字段未可靠更新；报告以最终汇总的 6,257 个唯一 trace、覆盖清单和评分结果为准。
- 报告不估算人民币费用：token 用量可审计，但没有本次实际账单或经确认的计价信息。
