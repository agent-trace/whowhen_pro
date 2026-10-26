# DAG + BLADE：DeepSeek 实现设计与验证记录

原方案日期：2026-10-22；文档状态更新：2026-10-26
文档说明：本文保留原始设计方案和早期 12 条 pilot 的历史记录，不代表当前运行状态。当前代码结构见 [BLADE 代码与报告索引](dag-blade-code-and-reports.md)；全量结果以 [DAG+BLADE v2 全量评测结果与数据来源](full-dag-blade-v2-results-2026-10-25.md) 为准，三方法对比见 [统一评分对比](full-dag-blade-comparison-2026-10-23.md)。

## 1. 目标

在现有 PostgreSQL-backed DAG Backtrace 上加入 BLADE 的“来源使用审计”机制，仍通过现有 OpenAI-compatible LiteLLM 通道调用 DeepSeek-V4.1-Flash。先做一轮受控小样本验证，确认协议可运行、输出可解析、成本和延迟可接受，再决定是否扩大样本。

BLADE 的核心不是单纯增加依赖边，而是检查接收方如何使用来源信息，并把“错误引入、错误传播、修复”作为证据交给最终归因。论文方法与原始实现均可参考：[论文](https://arxiv.org/abs/2610.18780)、[BLADE 源码仓库](https://github.com/GXG-CS/BLADE)。

## 2. 当前实现基线

当前回溯实现位于 `whowhen_eval/dag_pg.py` 与 `whowhen_eval/run_dag_pg.py`：

1. 从 PG 的 lossless `source.raw_json` 读取轨迹，构造稳定 step ID 和 PG span crosswalk。
2. 递归移除 ground-truth、失败标签和注入标记，避免模型看到答案。
3. 调用模型构建有向依赖边，再识别主要失败表现。
4. 从失败表现沿依赖边反向检查 `local_error` 和上游来源。
5. 最终选择范围限于被调查出的 attribution candidates。

6,257 条全量报告中的 `canonical_db_graph_all_at_once_v1` 是另一条 Graph/All-at-once 协议，不等于这里的 `canonical_db_dag_backtrace_v1`。新实验必须使用独立 protocol、method 和结果目录，不能覆盖或混入既有结果。

## 3. 参考 BLADE 源码的部分

BLADE 源码将流程拆成证据收集和最终判定两段：`BladePipeline.collect()` 运行前四个证据 operator，`run()` 再运行最终 diagnosis；每个 operator 在共享 run context 上增加结果。依赖重建为每个事件和关系生成选项式问题；后续审计依赖边选出的 handoff；最终 diagnosis 再读取完整轨迹和 blame graph。源码可重点阅读：

- `blade/pipeline.py`：operator 编排与 collect/run 分界。
- `blade/operators/dependency.py`：依赖关系问题构造、按阈值保存关系边。
- `blade/operators/` 下的 planning、contract audit、blame graph、diagnosis operators。
- `blade/jev/`：Jev 专用请求、选项概率和缓存接口。
- `LICENSE`：MIT。若直接复制代码，保留版权和许可证；本项目优先按相同职责自行实现，不复制 Jev 专用网络客户端。

BLADE 对每个目标事件评估 depends-on、repeats、contradicts、repairs；依赖分数用于选择 handoff，接收决策审计产生 findings，最终在全轨迹上重新判断 decisive step。论文用 Jev 对选项返回概率；这项接口能力是与通用 DeepSeek API 的主要差别。

## 4. DeepSeek 适配原则

本实验是 **BLADE-inspired 的 DeepSeek 适配**，不宣称复现 Jev 概率机制。

- 继续复用 whowhen_eval.llm.generate、--api-base、--api-key-env 和现有 PG/snapshot 读取器。对 OpenAI-compatible 自定义 endpoint，裸模型名由 runner 在 LiteLLM 内部按 openai/<model> 路由；发送给 endpoint 的模型名仍保持原值。
- 所有请求使用 JSON 输出并进行严格 schema 校验；不接受模型改写 step ID。
- 先用单条请求探测 endpoint 是否返回候选选项的原生 log probabilities。只有 endpoint 明确支持并通过小样本校验时，才把该数据标记为 `probability_source=provider_logprob`。
- 如果 endpoint 不提供原生概率，允许返回 `confidence` 作为模型自报置信度，但结果必须标记 `probability_source=self_reported_confidence`。不能把它描述为校准概率，也不能直接沿用论文的 θ=0.3 作为可靠概率阈值。
- 小样本阶段把每个关系问题压缩为一个最可能的前序事件或 `NONE`，并返回置信度和一句证据；优先保留 top-2 depends-on 来源用于审计。阈值作为配置项，由试跑结果和人工抽查决定。
- 需求 spans 只从任务 query 抽取；审计输入可读来源事件、接收事件、任务要求和必要上下文，不读 canonical labels、注入标记或答案字段。

建议关系输出结构：

```json
{
  "relations": [
    {"target": 2, "relation": "depends_on", "sources": [{"source": 0, "confidence": 0.78, "evidence": "..."}]},
    {"target": 2, "relation": "repeats", "sources": []}
  ]
}
```

建议审计输出保留 BLADE 的九个原始标签并在代码中映射为三组：`kept`、`breach`、`inherited_or_repaired`。不要在不同 prompt 之间临时改标签定义；标签含义和映射应从 BLADE 源码固定下来。

## 5. 推荐实现结构

新增独立模块，不改变现有 DAG baseline：

- `whowhen_eval/dag_blade.py`：BLADE-inspired 数据结构、关系/审计/全局判定 prompts、JSON schema 校验、confidence 来源标记、pipeline 状态机。
- `whowhen_eval/run_dag_blade_pg.py`：复用 `PostgresDAGRepository`、canonical snapshot、`generate`、结果落盘与断点续跑逻辑；对外提供与 `run_dag_pg.py` 一致的 PG/API 参数。
- `tests/test_dag_blade.py`：使用 mock JSON requester 覆盖关系 schema、未知 step、label 泄漏、handoff 限制、审计标签映射、全局决策可选任意 step、部分 API 失败和 resume。
- `docs/dag-blade-implementation-plan.md`：本方案和试跑结果。

独立标识：

- `protocol = canonical_db_dag_blade_v2`
- `method = dag_blade_ds_v2`
- 输出目录：`results/dag_blade_ds_v2/<model>/<benchmark>.jsonl`

每条记录保存 prompt hash、阶段名、每次调用的 usage/耗时/finish reason、confidence 来源、关系边、handoff findings、最终 step 与 PG span 映射、错误类型。不得记录 API key 或原始认证头。

## 6. Pipeline

1. **安全轨迹适配**：继续使用现有 `DAGBundle` 和 span crosswalk，验证每个事件 ID 与源轨迹/PG span 一致。
2. **Requirement extraction**：从任务 query 抽取短的、可核对的要求片段；不提供参考答案或失败标签。
3. **关系发现**：逐目标事件对四类关系各返回一个判断；sources 只能列出较早 step，若没有来源则返回空数组 []，不返回 null source ID。自报置信度与原生 logprob 分开存储。
4. **Audit planning**：按 depends-on confidence 选择每个目标最多两个来源；小样本阶段固定 `K=2`，阈值可配置。没有高于阈值的来源时不生成 handoff。
5. **Contract audit**：对被选 handoff 判断接收决策是否忠实使用来源、是否违反任务要求，以及错误属于新引入、继承/传播或修复。非决策事件作为 evidence，不直接创建责任 finding。
6. **Blame graph**：保存原始节点、带类型的关系边、requirements、findings 和每项置信度。
7. **全局 diagnosis**：将完整轨迹、blame graph 和 17 类 taxonomy 一起交给 DeepSeek，要求输出 decisive step、该 step 对应的一个精确 PG span ID、canonical error-mode code 和 reason。span ID 必须属于选中 step 的 crosswalk；模型看不到 gold label。
8. **离线评分**：DAG+BLADE 输出转换为 canonical prediction 字段 `actor`、`target_span_id`、`error_mode`，直接复用 `canonical.score_canonical`。三种方法统一计算 Actor/Who、精确 canonical When、Mode、Joint；同一 `summarize_canonical_pg` 汇总器计算四项 accuracy 与 17 类 macro-F1。Actor 缺少 gold 的样本继续排除在 Actor 分母外，其他指标覆盖完整样本；解析失败按错误预测保留在分母中。

## 7. 小范围验证设计

### 阶段 A：接口与输出 smoke test（12 条）

从 Who&When Pro 的 Magentic-One、DyLAN、MetaGPT 三类轨迹各取 4 条；尽量覆盖短、中、长轨迹。固定模型为 `DeepSeek-V4.1-Flash`、temperature 0，并使用当前已配置的 OpenAI-compatible API。先串行或低并发运行，启用逐调用落盘和断点续跑。

阶段 A 只验证：

- 轨迹适配、label/注入字段隔离、step/span 映射正确。
- 关系和审计 JSON 可解析且 step ID 合法。
- 返回空正文、截断、finish reason 异常时能保留现场并记为技术失败。
- 每条轨迹的输入/输出 tokens、调用数、耗时和失败原因完整记录。
- 检查 DeepSeek endpoint 是否真的提供原生选项概率；没有则明确使用 self-reported confidence 路径。

### 阶段 B：小样本同集对照（最多 60 条）

阶段 A 技术稳定后，再取 60 条（3 类各 20 条），用完全相同的 trace ID 比较：

1. 当前 DAG Backtrace。
2. DAG + handoff audit + 全局 diagnosis（BLADE-inspired）。

主指标：Who/Actor exact accuracy、When exact accuracy、joint accuracy、有效结果率。辅助指标：每条 trace 的 API 调用数、token、耗时、空/无效 JSON 率。另抽查至少 20 个 handoff finding，评估来源是否相关、接收方是否误用，以及标签是否能被轨迹内容支持。

### 解释边界与继续条件

- 60 条只能作为工程和方向性 pilot，不能据此声称统计显著提升。
- 数据集没有人工语义依赖边标注，不能报告 DAG edge precision/recall/F1；span parent-child 也不等于语义边真值。
- 先看技术可用率和调用成本，再看同集准确率变化。若 JSON/schema 稳定、技术失败率低、审计证据经抽查有用，才扩大到更大样本。
- 若长轨迹持续空响应或输出超长，先将关系发现和审计请求分批、限制每次 handoff 数量，再试跑；不要仅靠增大输出 token 上限。
- 不把论文的 57.6% step accuracy 或 θ=0.3 直接当作本项目目标/最佳配置。论文数据、模型接口、数据切分和当前全量 Graph 报告协议不同。

## 8. 风险与待确认项

1. **概率不可比**：Jev 的 option probability 与 DeepSeek JSON 自报置信度语义不同。报告必须区分，并在样本足够时做置信度校准。
2. **长输入和成本**：BLADE 源码一次对多个问题共享状态并通过 Jev 批处理；普通 chat completion 未必有相同的批量效率，逐目标询问会增加调用数。
3. **模型行为偏差**：错误模式、九类 audit labels 和来源关系都由同一个模型判断，可能相互强化；最终 diagnosis 应能查看完整原始轨迹并推翻局部 findings。
4. **标签边界**：Who&When Pro 的 target span 有确定映射，但一条 trajectory 可能包含多个 turn/span；保持现有 source-coordinate 规则，要求模型在选中事件对应的 PG span 中选一个精确 span，映射不合法时将该 trace 记为技术失败，不猜测。
5. **API 中断**：遇到 429、配额或鉴权错误时暂停并保存 JSONL/run-state，不自动重启，保留同目录断点续跑命令。

## 9. 参考资料

- Guo et al., *BLADE: Auditing Information Use for Failure Attribution in LLM Multi-Agent Systems*, arXiv:2610.18780, 2026：[论文](https://arxiv.org/abs/2610.18780)。
- 原始实现（MIT）：[GXG-CS/BLADE](https://github.com/GXG-CS/BLADE)。建议优先参考 `blade/pipeline.py`、`blade/operators/dependency.py`、其余 operators、`blade/types.py` 和 `blade/jev/`；复用算法结构，按项目现有 LiteLLM/DeepSeek/PG 数据层编写适配。
- 当前项目：[PG-backed DAG Backtrace 说明](dag-backtrace-pg.md)、`whowhen_eval/dag_pg.py`、`whowhen_eval/run_dag_pg.py`。
## 10. 实现落地与早期试跑记录（历史）

### 实现和验证

- 工作分支：codex/dag-blade-pilot-plan。
- 新增 whowhen_eval/dag_blade.py、whowhen_eval/run_dag_blade_pg.py、tests/test_dag_blade.py，并在 pyproject.toml 注册 runner；现有 DAG Backtrace runner 保持独立。
- 自定义 OpenAI-compatible endpoint 的裸模型名通过 LiteLLM 内部 openai/ 路由；服务器收到的模型名仍是 DeepSeek-V4.1-Flash。
- PG 测试库中 events_dedup 是 events 的视图。原始导入未保存 source.raw_json；只为 12 条 pilot 根 span 在 events.metadata 补入对应原始 envelope，未修改其他 trace。12 条 dry-run 全部通过，API 调用数为 0。
- 完整 unittest：23 项通过。测试覆盖数据标签隔离、relation/audit schema、provider 路由和错误时暂停/续跑。
- 真实试跑最初暴露两个问题并已修复：LiteLLM 裸模型名 provider 路由；关系 prompt 的 source=null 与 validator 的 sources=[] 不一致。修复后模型按 JSON schema 返回有效结果。

### 12 条 DeepSeek pilot

使用模型 DeepSeek-V4.1-Flash、并发 6、无显式输出 token 上限、零重试。样本覆盖 3 个 framework × 2 个 benchmark，每个 benchmark 2 条；12 个 trace ID 与固定 manifest 完全一致。最终 run-state 为 complete_with_trace_failures：11 条成功，1 条请求超时；没有 401/403/429 或配额错误。

| Benchmark | 技术成功 | Who 正确 | When 正确 | Who 与 When 都正确 |
|---|---:|---:|---:|---:|
| bigcodebench | 2/2 | 2/2 | 2/2 | 2/2 |
| gaia | 2/2 | 1/2 | 1/2 | 1/2 |
| gpqa | 2/2 | 1/2 | 1/2 | 1/2 |
| livecodebench_pro | 1/2 | 1/2 | 1/2 | 1/2 |
| scibench | 2/2 | 0/2 | 0/2 | 0/2 |
| simpleqa_verified | 2/2 | 2/2 | 2/2 | 2/2 |
| 总计（端到端） | 11/12 | 7/12（58.3%） | 7/12（58.3%） | 7/12（58.3%） |

表中准确率的分母包含 12 条样本；技术失败按错误计入。只看 11 条成功 completion 时，Who 为 7/11（63.6%），When 为 7/11（63.6%），两项同时正确为 7/11（63.6%）。联合正确率由 Who 与 When 两个布尔分数同时为真计算；现有 DAG scorer 的 all 字段对本协议留空。Mode 未评测。

最终 run 记录 45 次 API 调用：44 次成功返回，1 次超时。报告用量为 input 268,234、output 319,066、total 587,300 tokens；服务端另报告 reasoning tokens 296,947。每条轨迹的 duration 合计 3,323.6 秒，平均 277.0 秒；wall-clock 约 58 分 52 秒。超时轨迹耗时约 600 秒。

此前为修复关系格式而中止的一次尝试另记录 8 次成功 completion、45,350 tokens。因此，所有已落盘试跑记录合计至少 52 次成功 completion、632,650 tokens；中止时可能有未完成且未落盘的 in-flight 请求，实际用量可能更高。首次裸模型路由失败没有记录成功 completion 或 token usage。

最终 JSONL 位于 WSL：/home/hqd/datasets/whowhen-pro/eval/dag-blade-pilot-20261022/dag_blade_ds_pilot_prompt_fix_20261022/dag_blade_ds_pilot/DeepSeek-V4.1-Flash/。评分使用 canonical snapshot，模型输入不含 ground-truth 或 injection labels。

### 结果解释与下一步

- 这 12 条只验证工程可运行性并提供方向性准确率，不足以证明 BLADE-inspired 方法优于现有 DAG。样本没有与相同 trace ID 的 DAG Backtrace 结果做对照。
- 不与 full-text-all-at-once 报告混算；两者 protocol 不同。数据集也没有语义依赖边真值，因此本 pilot 不报告 DAG edge precision/recall。
- 该版本每条轨迹最多 4 次 completion；本轮 average 277 秒/trace、final-run usage 587,300 tokens。扩大到 60 条前应先比较成本和时间，并用相同 12 个 trace ID 跑 DAG Backtrace baseline。
- 结果已完整落盘；超时 trace 可用同一输出目录断点续跑，runner 只跳过成功记录。

## 11. Canonical 评分对齐与全量结果（2026-10-23 至 2026-10-25）

此前 300 条 BLADE pilot 是 v1：只预测 Who/When，且 When 用“所选 step 包含目标 span”计分。它缺少 Mode，When 粒度也比全量报告的精确 canonical span 宽，**不能用于与 full-dag-evaluation-report 中 all-at-once / DAG 分数做全指标对比**。

当前 v2 已补齐：

- Diagnosis 输出 `step_id`、`target_span_id`、17 类 `error_mode` 和 reason；`target_span_id` 经校验必须属于选中的 step。
- BLADE 的评分直接调用 `canonical.score_canonical`，预测与报告使用同一 actor、span、mode 和 joint 语义。
- `compare_canonical_pg` 支持 all-at-once、graph/DAG 和 DAG+BLADE 三方法配对，强制核对 trace ID 与 canonical labels，并输出四项 accuracy、17 类 Mode macro-F1 和配对差值。
- Runner 默认 API/DB 并发为 6，逐 trace 落盘；遇到配额/鉴权错误暂停，run-state 保存进度。重复运行产生的尝试行会合并成每条 trace 一行的 `merged` 目录，避免重试重复计数。

DAG+BLADE v2 全量 API 评测已于 2026-10-25 完成：6,257 条最终 canonical 结果均成功，早期失败经两轮 failed-only retry 恢复。当前指标、资源用量、结果文件位置和评分说明见[全量结果与数据来源](full-dag-blade-v2-results-2026-10-25.md)；三方法的全量对比见[统一评分对比](full-dag-blade-comparison-2026-10-23.md)。早期 v1 pilot 的输出字段和 When 粒度不同，不纳入 v2 的统一全量比较。
