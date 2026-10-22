# Who&When Pro 全量 Graph/DAG API 评测报告

**完成时间：** 2026-10-21（Asia/Shanghai）
**评测状态：** 已完成
**覆盖量：** 6,257 条轨迹，16 个 benchmark

## 摘要

本次对 Who&When Pro 全量文本轨迹执行 Graph/DAG 模式 API 评测。全量 6,257 条结果均已落盘，runner 正常结束；结果 ID 与预期轨迹集合核对通过，无重复、无缺失。仓库现有汇总器报告 0 条评测错误。

主要指标：Who/Actor 准确率 **73.18%**，When 准确率 **69.73%**，Mode 准确率 **27.39%**；Who、When、Mode 三项同时正确率 **22.69%**。这组数字显示，联合判断的主要改进空间在 Mode 分类；三项联合正确率也明显低于单项准确率。

## 评测范围与方法

- **方法：** graph；汇总记录的 protocol 为 `canonical_db_graph_all_at_once_v1`，即整条轨迹一次性构图的评测协议。
- **模型：** `DeepSeek-V4.1-Flash`。
- **样本：** full-text 全量数据，共 6,257 条；结果从既有 full API run 目录读取并由仓库汇总逻辑离线统计。
- **统计逻辑：** 使用仓库 `whowhen_eval.summarize_canonical_pg` 汇总；同时传入预期 trace ID 清单做覆盖校验。

## 覆盖情况

| Benchmark | 结果数 |
|---|---:|
| alfworld | 311 |
| bigcodebench | 195 |
| dabench | 327 |
| dacode | 306 |
| databench | 818 |
| dsqa | 830 |
| gaia | 302 |
| gpqa | 143 |
| humaneval | 18 |
| kramabench | 82 |
| livecodebench_pro | 129 |
| math | 448 |
| mmlu_pro | 371 |
| scibench | 555 |
| simpleqa_verified | 509 |
| tablebench | 913 |
| **合计** | **6,257** |

## 评测指标

| 指标 | 正确数 / 分母 | 准确率 |
|---|---:|---:|
| Who / Actor | 1,954 / 2,670 | **73.18%** |
| When | 4,363 / 6,257 | **69.73%** |
| Mode | 1,714 / 6,257 | **27.39%** |
| Who + When + Mode 全部正确 | 1,420 / 6,257 | **22.69%** |
| Error-mode macro-F1（17 类） | — | **14.06%** |

Who/Actor 的分母按仓库汇总器的适用样本口径计算，因此与全量轨迹数不同；其他列按 6,257 条全量统计。联合准确率要求同一条轨迹的 Who、When 和 Mode 全部正确。

## 与 `full-text-all-at-once-report.md` 的 PostgreSQL canonical All-at-Once 对比

这里仅对比该报告中的 **运行 B：PostgreSQL-backed canonical All-at-Once**（2026-10-13，`DeepSeek-V4.1-Flash`）。发布版 JSONL 运行使用了不同模型和评分协议，不适合作为这次 DAG run 的直接对照。

| 指标 | Canonical All-at-Once（10/13） | Graph/DAG（本次） | 差值 |
|---|---:|---:|---:|
| Actor / Who | 72.10%（1,925/2,670） | 73.18%（1,954/2,670） | **+1.09 个百分点** |
| When | 67.89%（4,248/6,257） | 69.73%（4,363/6,257） | **+1.84 个百分点** |
| Error-mode accuracy / Mode | 26.77%（1,675/6,257） | 27.39%（1,714/6,257） | **+0.62 个百分点** |
| Error-mode macro-F1 | 12.7%（17 类） | 14.06%（17 类） | **约 +1.4 个百分点** |
| Joint（Actor/Who、When、Mode 全部正确） | 21.86%（1,368/6,257） | 22.69%（1,420/6,257） | **+0.83 个百分点** |

旧报告列出的部分准确率四舍五入到一位小数；上表在有分子/分母时用原始计数重新计算后再比较。两次运行使用相同模型版本、相同 6,257 条 full-text split 和相同的 Actor 适用样本分母；本次五项汇总指标均高于 10/13 的 canonical All-at-Once 记录。

### 比较边界

两次运行并非只改变了 Graph/DAG 开关：旧运行 protocol 为 `canonical_db_all_at_once_v1`，API/DB 并发均为 16，输出上限为 16,384 tokens，reasoning effort 为 `none`；本次 protocol 为 `canonical_db_graph_all_at_once_v1`，API 并发为 6、DB 并发为 8，未设置显式 max-token 上限。两次输出异常计数也不同：旧报告记载 48 条空输出/解析警告，本次为 5 条空可见输出和 15 条解析警告。因此，分数变化只能描述为两次完整配置下的观察差异，不能归因为 DAG 模式单一因素，也不能据此证明 DAG 构图质量提升。

此外，数据没有语义依赖边真值；这些 Actor/When/Mode 与 Joint 分数衡量字段预测，不是 DAG 边级准确率。当前没有可用于公平对照的 edge precision、recall 或 F1。
## 完成与质量核验

- checkpoint 状态为 `complete`，完成数 6,257，剩余数 0；runner 已退出。
- 16 个 benchmark JSONL 共 6,257 行；预期 trace ID 校验通过，无重复、无缺失。
- 本轮失败尝试数为 0；最终汇总 `errors=0`，checkpoint 的 `last_error` 为空。
- 汇总记录包含 5 条 empty visible output 和 15 条 parse-warning rows。这些是输出/解析诊断项，按仓库汇总逻辑计入结果；它们不等同于 runner 失败数。
- 日志和暂停事件里有 6 条恢复前的缺少凭据错误记录。它们属于恢复前历史记录；恢复后的全量运行没有失败，不应与最终评测错误混为一谈。

## 用量与耗时

汇总器记录的有效用量为输入 33,661,362 tokens、输出 38,382,582 tokens，合计 72,043,944 tokens。累计有效请求耗时为 328,629 秒（约 91.3 小时）；这是请求耗时累计值，不代表从启动到结束的墙钟时间。

## 结果解读与限制

1. Who/Actor 与 When 单项准确率约为 70%–73%，但三项联合准确率为 22.69%，说明多个字段同时判对仍较困难。
2. Mode 准确率 27.39%、Error-mode macro-F1 14.06%，是当前汇总指标中较弱的部分，后续应优先检查类别混淆、类别不平衡及提示中的定义边界。
3. 数据集没有现成的语义依赖边标注。Graph/DAG 结构由模型根据轨迹内容推断，因此本报告不能把 span 的 parent-child 结构当成语义依赖边真值，也不报告未经标注支持的边级 precision/recall/F1。
4. 本报告的准确率来自仓库现有评测汇总口径。若要比较模型、提示词或 DAG 构图策略，应固定样本集合、解析规则及类别口径，并单独设计人工核验或带边标注的评测集。

## 产物位置

- 汇总 JSON：`/home/hqd/datasets/whowhen-pro/eval/full-text-dag-20261019/final-full-api-summary-20261021T1104Z/summary.json`
- 汇总目录内还包含按 benchmark 合并的结果行及 `expected-trace-ids.txt`。
- 原始 API 结果：`/home/hqd/datasets/whowhen-pro/eval/full-text-dag-20261019/full-api-run/openai_DeepSeek-V4.1-Flash/`
- 运行状态与日志：`/home/hqd/datasets/whowhen-pro/eval/full-text-dag-20261019/full-api-run/`
- 定时监测：全量完成后已停止。

---

**结论：** 本轮 Graph/DAG API 全量评测已完成，数据覆盖完整，运行失败为 0。总体上 Who 与 When 表现约七成，Mode 及三项联合判断准确率偏低；这些指标衡量的是当前仓库协议下的字段/模式预测结果，不代表 DAG 语义边本身已经通过带真值的边级评测。