#!/usr/bin/env python3
"""
批量执行股票回测
每批处理N只股票，完成后更新任务状态到Git
"""

import json
import os
import subprocess
import time
from datetime import datetime

WORKSPACE = "/root/.openclaw/workspace"
TASKS_FILE = f"{WORKSPACE}/quant_backtest/tasks.json"
RESULTS_DIR = f"{WORKSPACE}/quant_backtest/results"

# 任务配置
BATCH_SIZE = 5  # 每批处理数量
TIMEOUT_PER_STOCK = 600  # 单只股票超时时间(秒)
SLEEP_BETWEEN_STOCKS = 10  # 股票间隔时间(秒)

def load_tasks():
    """加载任务清单"""
    with open(TASKS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_tasks(tasks):
    """保存任务清单"""
    with open(TASKS_FILE, 'w', encoding='utf-8') as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2)

def run_single_backtest(symbol, market):
    """运行单只股票回测"""
    cmd = [
        "python3",
        f"{WORKSPACE}/quant_backtest/backtest_v2.py",
        "--symbol", symbol,
        "--market", market,
        "--start", "20000101",
        "--end", datetime.now().strftime("%Y%m%d")
    ]
    
    try:
        result = subprocess.run(
            cmd,
            timeout=TIMEOUT_PER_STOCK,
            capture_output=True,
            text=True
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "超时"
    except Exception as e:
        return False, "", str(e)

def commit_and_push(message):
    """提交到Git"""
    os.chdir(WORKSPACE)
    subprocess.run(["git", "add", "quant_backtest/"], check=False)
    subprocess.run(["git", "commit", "-m", message], check=False)
    subprocess.run(["git", "push", "origin", "main"], check=False)

def main():
    print("=" * 50)
    print("股票批量回测任务开始")
    print("=" * 50)
    
    tasks = load_tasks()
    task_list = tasks.get("任务列表", [])
    
    # 统计
    pending = [t for t in task_list if t.get("状态") == "pending"]
    print(f"待处理任务数: {len(pending)}")
    
    # 取前BATCH_SIZE个任务
    batch = pending[:BATCH_SIZE]
    if not batch:
        print("没有待处理任务")
        return
    
    print(f"\n本批次处理: {len(batch)} 只股票")
    
    success_count = 0
    fail_count = 0
    
    for task in batch:
        rank = task.get("排名")
        name = task.get("公司名称")
        symbol = task.get("股票代码")
        market = task.get("市场")
        
        print(f"\n[{rank}/{name}] 股票代码: {symbol} 市场: {market}")
        
        # 更新状态为进行中
        task["状态"] = "running"
        task["开始时间"] = datetime.now().isoformat()
        save_tasks(tasks)
        
        # 执行回测
        success, stdout, stderr = run_single_backtest(symbol, market)
        
        if success:
            task["状态"] = "completed"
            task["完成时间"] = datetime.now().isoformat()
            print(f"✓ {name} 回测完成")
            success_count += 1
        else:
            task["状态"] = "failed"
            task["错误信息"] = stderr[:200]
            print(f"✗ {name} 回测失败: {stderr[:100]}")
            fail_count += 1
        
        save_tasks(tasks)
        
        # 间隔
        time.sleep(SLEEP_BETWEEN_STOCKS)
    
    # 提交结果
    commit_msg = f"feat: 完成 {len(batch)} 只股票回测 ({success_count}成功/{fail_count}失败)"
    commit_and_push(commit_msg)
    
    print("\n" + "=" * 50)
    print(f"本批次完成: 成功 {success_count}, 失败 {fail_count}")
    print("=" * 50)

if __name__ == "__main__":
    main()