# HEARTBEAT.md - 股票回测定时任务

## 定时任务说明
每30分钟执行一次股票回测任务

## 待执行任务
1. 运行批量回测: `python3 /root/.openclaw/workspace/quant_backtest/run_batch.py`
2. 检查任务状态
3. 更新Git仓库

## 检查规则
- 如果有待处理任务且没有正在运行的任务，则启动新批次
- 本批次每次处理5只股票
- 每只股票超时10分钟