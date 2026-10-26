# DAG+BLADE v2 全量评测结果与数据来源

整理日期：2026-10-26；评测完成日期：2026-10-25
## 评测范围

| 项目 | 内容 |
| --- | --- |
| 数据集 | Who&When Pro canonical text |
| 样本量 | 6,257 条 trace |
| 方法 | DAG+BLADE v2（dag_blade_ds_v2） |
| 模型 | DeepSeek-V4.1-Flash |
| 最终有效结果 | 6,257 条；0 条未恢复失败 |
| 补跑情况 | 初始失败的 105 条经两轮 failed-only retry 全部恢复 |

结果按 trace ID 去重并采用成功重试结果；本报告的指标来自最终 canonical 汇总，而不是早期运行状态或中间 JSONL 行数。

## 统一评分结果

| 指标 | 正确数 / 分母 | 得分 |
| --- | ---: | ---: |
| Who | 2,042 / 2,670 | 76.48% |
| When | 4,421 / 6,257 | 70.66% |
| Mode Accuracy | 1,719 / 6,257 | 27.47% |
| Mode Macro-F1 | 固定 17 类 taxonomy | 13.79% |
| Joint | 1,432 / 6,257 | 22.89% |

Who 只在存在主体真值的 2,670 条样本上评分。When 按目标 span ID 精确匹配；Mode Accuracy 按错误类型代码精确匹配；Mode Macro-F1 对固定 17 类分别计算 F1 后宏平均；Joint 按 canonical scorer 判定 Who、When、Mode 是否同时正确。

## 与 DAG 的逐样本配对结果

配对统计以 trace_id 对齐两种方法的结果。每行依次列出：两者都正确、仅 DAG+BLADE 正确、仅 DAG 正确、两者都错误。

| 指标 | 两者都正确 | 仅 DAG+BLADE 正确 | 仅 DAG 正确 | 两者都错误 |
| --- | ---: | ---: | ---: | ---: |
| Who（2,670 条） | 1,874 | 168 | 80 | 548 |
| When（6,257 条） | 4,094 | 327 | 269 | 1,567 |
| Mode（6,257 条） | 1,315 | 404 | 399 | 4,139 |
| Joint（6,257 条） | 1,103 | 329 | 317 | 4,508 |

净变化为：Who +88 条正确、When +58 条、Mode Accuracy +5 条、Joint +12 条。Mode 的改善与退化样本数接近，且其 Macro-F1 为 13.79%，比 DAG 的 14.06% 低 0.27 个百分点。

## API 用量与耗时

| 统计口径 | 输入 tokens | 输出 tokens | 合计 tokens | 累计 trace 耗时 |
| --- | ---: | ---: | ---: | ---: |
| 所有已落盘尝试（初始运行及两轮 failed-only retry） | 89,618,355 | 158,463,887 | 248,082,242 | 1,057,481.979 秒（约 293.74 小时） |
| 最终采用的 6,257 条结果 | 89,037,783 | 156,504,850 | 245,542,633 | 1,044,419.171 秒（约 290.12 小时） |

原始记录共 6,376 行：初始运行 6,265 行（对应 6,257 个不同 trace_id，其中 8 行是重复尝试记录），第一轮重试 105 行，第二轮重试 6 行。所有行都记录了 usage。两种统计口径的差值来自未被最终结果采用的失败或重复尝试。

按最终 6,257 条结果折算，平均约 39,243 tokens/trace、166.9 秒/trace。累计 trace 耗时是逐条结果的 duration_s 求和；并发执行时各 trace 时间会重叠，因此它不是进程实际运行时长。按结果行时间戳，首条记录为 2026-10-23 10:09:20、末轮最后记录为 2026-10-25 21:35:25（Asia/Shanghai），跨度约 59.43 小时；该跨度包含暂停和等待间隔，也不等于活跃执行时间。token 统计未换算货币费用，因为当前没有可核实的实际账单或适用价格。


## 评测配置与评分实现

- 模型：DeepSeek-V4.1-Flash；benchmark：all；modality：text。
- API 并发 6，DB 并发 6，temperature 0，reasoning effort low，num-retries 0。
- Canonical 标签快照：/home/hqd/datasets/whowhen-pro/eval/full-text-dag-20261019/canonical-snapshot.jsonl。
- Taxonomy：/home/hqd/datasets/whowhen-pro/taxonomy.yaml。
- Canonical 汇总及配对比较实现：[summarize_canonical_pg.py](../whowhen_eval/summarize_canonical_pg.py) 与 [compare_canonical_pg.py](../whowhen_eval/compare_canonical_pg.py)。
- 实现模块与测试入口：[BLADE 代码与报告索引](dag-blade-code-and-reports.md)。

## 原始结果与最终汇总文件

- 最终 canonical 汇总：[summary.json](/home/hqd/datasets/whowhen-pro/eval/dag-blade-canonical-v2-20261023/full-run/retry-failed-20261025/final-after-retry-round2/summary.json)。
- 原始逐 benchmark 预测 JSONL 目录：[DeepSeek-V4.1-Flash 结果目录](/home/hqd/datasets/whowhen-pro/eval/dag-blade-canonical-v2-20261023/full-run/dag_blade_ds_v2/DeepSeek-V4.1-Flash/)。
- 第一轮 failed-only retry JSONL：[第一轮重试结果目录](/home/hqd/datasets/whowhen-pro/eval/dag-blade-canonical-v2-20261023/full-run/retry-failed-20261025/dag_blade_ds_v2/DeepSeek-V4.1-Flash/)。
- 第二轮 failed-only retry JSONL：[第二轮重试结果目录](/home/hqd/datasets/whowhen-pro/eval/dag-blade-canonical-v2-20261023/full-run/retry-failed-20261025-round2/dag_blade_ds_v2/DeepSeek-V4.1-Flash/)。

## 解释边界

DAG 和 DAG+BLADE 的评分数据与 canonical 口径一致，因而可以比较任务分数和逐样本变化。不过推理设置不完全相同，分数差异不能单独归因于 BLADE。数据集没有语义依赖边的 gold labels，因此这些指标也不等价于 DAG 边的 precision、recall 或 F1。
