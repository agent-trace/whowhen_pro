# DAG+BLADE 代码与报告索引

更新日期：2026-10-26

## 方法边界

仓库实现的是面向 Who&When Pro、DeepSeek/OpenAI-compatible API 和 PostgreSQL 轨迹的 **BLADE-inspired 适配**。它借鉴“审计信息如何被接收方使用”的流程，不是原论文 Jev 概率机制的复现。现有 DAG baseline 保持独立；比较结论以统一 canonical 任务评分为准。

## 代码结构

| 文件 | 职责 |
| --- | --- |
| [whowhen_eval/dag_blade.py](../whowhen_eval/dag_blade.py) | BLADE-inspired 核心：四阶段 prompts、JSON/schema 校验、handoff 选择、Blame graph 组装、全轨迹诊断和 canonical prediction scoring。 |
| [whowhen_eval/run_dag_blade_pg.py](../whowhen_eval/run_dag_blade_pg.py) | 独立 PG/API runner：读取 snapshot 与轨迹、调用模型、并发控制、逐条落盘、usage/error/run-state 记录、resume 和重试结果合并。 |
| [whowhen_eval/dag_pg.py](../whowhen_eval/dag_pg.py) / [dag_pg_source.py](../whowhen_eval/dag_pg_source.py) | 与 DAG baseline 共用的轨迹结构、step/span crosswalk 和 PostgreSQL 数据读取。 |
| [whowhen_eval/compare_canonical_pg.py](../whowhen_eval/compare_canonical_pg.py) | 按 trace ID 校验并配对 All-at-Once、DAG、DAG+BLADE；检查 canonical 标签一致并生成各项指标。 |
| [tests/test_dag_blade.py](../tests/test_dag_blade.py) | BLADE schema、label-blind 输入、span 合法性、runner/provider 行为和错误暂停等离线单元测试。 |

当前模块边界已经把方法流程与 PG/API 执行分开。全量评测完成后没有为了目录层次而移动模块，避免改变稳定导入路径和历史运行入口。

## 运行入口

- Python 模块：python -m whowhen_eval.run_dag_blade_pg --help
- 已注册命令：whowhen-eval-dag-blade-pg
- 离线测试文件：tests/test_dag_blade.py

正式评测时应复用固定 trace ID 清单、canonical snapshot 和 taxonomy；DAG 与 DAG+BLADE 必须对齐样本后才能做逐样本比较。API key 只从环境变量读取，不写入代码、run-state 或结果文件。

## 报告导航

| 文档 | 用途 |
| --- | --- |
| [实现设计与历史 pilot](dag-blade-implementation-plan.md) | 原方案、BLADE 概念映射和早期 12 条工程 pilot；不是当前全量结果来源。 |
| [DAG+BLADE v2 全量结果与数据来源](full-dag-blade-v2-results-2026-10-25.md) | 最终 canonical 指标、逐样本配对、API token 用量/累计耗时和原始结果路径。 |
| [All-at-Once / DAG / DAG+BLADE 统一评分对比](full-dag-blade-comparison-2026-10-23.md) | 三种方法的汇总分数、百分点差值、资源比较和可比性限制。 |
| [DAG Backtrace 全量报告](full-dag-evaluation-report-2026-10-21.md) | DAG baseline 的独立运行与评分来源。 |
| [PG-backed DAG Backtrace 说明](dag-backtrace-pg.md) | 现有 DAG baseline 的 PG 数据适配说明。 |

## 当前结果摘要

DAG+BLADE v2 的 6,257 条 canonical 评测结果已完成；最终结果文件、失败重试归并方式和指标分母请以[全量结果报告](full-dag-blade-v2-results-2026-10-25.md)为准。三方法对比的稳定摘要见[统一对比报告](full-dag-blade-comparison-2026-10-23.md)。两份报告不记录周期性 runner 状态。
