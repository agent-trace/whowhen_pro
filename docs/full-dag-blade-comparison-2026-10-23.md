# All-at-Once、DAG 与 DAG+BLADE 统一评分对比

更新日期：2026-10-26

## 结论摘要

在同一批 6,257 条轨迹和统一 canonical 评分口径下，DAG+BLADE v2 相比 DAG 的 Who、When 和 Joint 指标更高；Mode accuracy 仅小幅提高，Mode macro-F1 略有下降。结果显示 BLADE 对主体与目标步骤识别有帮助，但目前不能据此断言所有任务维度都有提升，也不能把差值完全归因于 BLADE 算法本身，因为各次运行的推理配置并不完全一致。

## 统一评分口径

| 指标 | 评分规则 | 分母 |
| --- | --- | ---: |
| Who | 预测主体与 canonical 主体标签完全匹配 | 有主体真值的 2,670 条 |
| When | 预测目标 span ID 与 canonical 目标 span ID 完全匹配 | 6,257 |
| Mode Accuracy | 预测的错误类型代码与真值一致 | 6,257 |
| Mode Macro-F1 | 固定 17 类 taxonomy 分别计算 F1 后取宏平均 | 17 类 |
| Joint | 按 canonical scorer 判定 Who、When、Mode 同时正确 | 6,257 |

三组结果使用相同的 canonical 标签快照、taxonomy 和评分定义。All-at-Once 的汇总值来自其全量评测报告；DAG 与 DAG+BLADE 的结果来自对应全量运行的 canonical 汇总。

## 全量评分结果

| 方法 | Who | When | Mode Accuracy | Mode Macro-F1 | Joint |
| --- | ---: | ---: | ---: | ---: | ---: |
| All-at-Once | 72.10% (1,925/2,670) | 67.89% (4,248/6,257) | 26.77% (1,675/6,257) | 12.70% | 21.86% (1,368/6,257) |
| DAG | 73.18% (1,954/2,670) | 69.73% (4,363/6,257) | 27.39% (1,714/6,257) | 14.06% | 22.69% (1,420/6,257) |
| DAG+BLADE v2 | 76.48% (2,042/2,670) | 70.66% (4,421/6,257) | 27.47% (1,719/6,257) | 13.79% | 22.89% (1,432/6,257) |
| DAG+BLADE v2 − DAG | **+3.30 pp** | **+0.93 pp** | **+0.08 pp** | **−0.27 pp** | **+0.19 pp** |
| DAG − All-at-Once | +1.09 pp | +1.84 pp | +0.62 pp | +1.36 pp | +0.83 pp |
| DAG+BLADE v2 − All-at-Once | +4.38 pp | +2.77 pp | +0.70 pp | +1.09 pp | +1.02 pp |

pp 表示百分点。Mode macro-F1 的百分点变化按未四舍五入的底层分数计算。

## 运行用量与耗时

| 方法 | Token 用量（input + output） | 累计 trace 耗时 | 首末结果时间戳跨度 |
| --- | ---: | ---: | ---: |
| All-at-Once | 69,095,342（31,959,079 + 37,136,263） | 报告未提供 | 约 13.0 小时 |
| DAG | 72,043,944（33,661,362 + 38,382,582） | 328,629 秒，约 91.3 小时 | 约 49.86 小时 |
| DAG+BLADE v2 | 所有已记录尝试 248,082,242（89,618,355 + 158,463,887）；最终采用结果 245,542,633 | 所有尝试累计 1,057,482 秒，约 293.74 小时；最终结果累计 1,044,419 秒，约 290.12 小时 | 约 59.43 小时 |

BLADE 的所有尝试总 token 约为 DAG 的 3.44 倍、All-at-Once 的 3.59 倍。这里的累计 trace 耗时是每条结果的 duration_s 之和，不是进程墙钟时间；首末结果跨度则包含运行间暂停和等待。All-at-Once、DAG 与 DAG+BLADE 的结果来源和具体口径见下方来源文档。


## DAG 与 DAG+BLADE 的逐样本配对

下表将每条样本按两种方法的结果分为“都正确”“仅 DAG+BLADE 正确”“仅 DAG 正确”“都错误”。四类之和等于对应指标分母。

| 指标 | 两者都正确 | 仅 DAG+BLADE 正确 | 仅 DAG 正确 | 两者都错误 |
| --- | ---: | ---: | ---: | ---: |
| Who（2,670 条） | 1,874 | 168 | 80 | 548 |
| When（6,257 条） | 4,094 | 327 | 269 | 1,567 |
| Mode（6,257 条） | 1,315 | 404 | 399 | 4,139 |
| Joint（6,257 条） | 1,103 | 329 | 317 | 4,508 |

配对结果表明，DAG+BLADE 在 Who 上净增 88 条正确样本，在 When 上净增 58 条，在 Mode accuracy 上净增 5 条，在 Joint 上净增 12 条。Mode 的改进与退化样本数量接近，macro-F1 最终低于 DAG 0.27 个百分点。

## 可比性与限制

- **评分可比。** DAG 和 DAG+BLADE 使用相同的 6,257 条轨迹 ID、canonical 标签快照、17 类 taxonomy 和评分逻辑；其逐样本配对统计可直接比较。All-at-Once 仅有已发布的全量汇总结果，因此这里只比较汇总指标，不能计算它与另外两组的逐样本胜负或显著性。
- **推理配置存在差异。** 三次运行使用同一模型系列和 API endpoint，但并发及推理设置并非完全一致：All-at-Once 使用 API/DB 并发 16、输出上限 16,384、reasoning effort none；DAG 报告记录 API 并发 6、DB 并发 8，未记录显式输出上限；DAG+BLADE v2 使用 API/DB 并发 6、temperature 0、reasoning effort low，未设置显式输出上限。因此评分规则一致，实验配置并非严格控制变量。
- **结果解释。** DAG+BLADE v2 对 DAG 的 Who、When 和 Joint 有正向差值，Mode accuracy 基本持平，Mode macro-F1 略降。应将其视为这组运行配置下的观察结果，不能单独证明 BLADE 导致了全部变化。
- **图边准确率未评测。** 数据没有预先标注的语义依赖边真值。本报告比较的是 Who、When、Mode 和 Joint 任务分数，不代表 DAG 边的 precision、recall 或 F1。

## 结果来源

- All-at-Once：[full-text-all-at-once-report.md](full-text-all-at-once-report.md)
- DAG：[full-dag-evaluation-report-2026-10-21.md](full-dag-evaluation-report-2026-10-21.md)
- DAG+BLADE v2：[全量评测结果与数据来源](full-dag-blade-v2-results-2026-10-25.md)，其中列有 canonical 汇总文件、逐 benchmark JSONL 目录、评分实现和 DAG 配对统计。
- BLADE 实现、离线测试和文档导航：[代码与报告索引](dag-blade-code-and-reports.md)。
