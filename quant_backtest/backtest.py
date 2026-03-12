#!/usr/bin/env python3
"""
股票回测框架
支持多种策略：买入持有、均线交叉、定期定投等
数据源：yfinance
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# 配置
START_DATE = "2000-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")
INITIAL_CAPITAL = 100000  # 初始资金
OUTPUT_DIR = "/root/.openclaw/workspace/quant_backtest/results"

class BacktestEngine:
    def __init__(self, symbol: str, start_date: str = START_DATE, end_date: str = END_DATE):
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.data = None
        self.trades = []
        self.portfolio_value = []
        
    def download_data(self) -> bool:
        """下载股票数据"""
        try:
            print(f"正在下载 {self.symbol} 数据...")
            ticker = yf.Ticker(self.symbol)
            self.data = ticker.history(start=self.start_date, end=self.end_date)
            
            if self.data.empty:
                print(f"⚠️ {self.symbol} 没有数据")
                return False
            
            # 重置索引，将Date作为列
            self.data.reset_index(inplace=True)
            self.data['Date'] = pd.to_datetime(self.data['Date']).dt.tz_localize(None)
            
            print(f"✅ 获取到 {self.symbol} 数据: {len(self.data)} 条记录")
            print(f"   时间范围: {self.data['Date'].min()} ~ {self.data['Date'].max()}")
            return True
            
        except Exception as e:
            print(f"❌ 下载失败: {e}")
            return False
    
    def calculate_returns(self) -> pd.DataFrame:
        """计算收益率指标"""
        if self.data is None:
            return None
            
        df = self.data.copy()
        
        # 日收益率
        df['Daily_Return'] = df['Close'].pct_change()
        
        # 累计收益率
        df['Cumulative_Return'] = (1 + df['Daily_Return']).cumprod() - 1
        
        # 移动平均线
        df['MA_20'] = df['Close'].rolling(window=20).mean()
        df['MA_50'] = df['Close'].rolling(window=50).mean()
        df['MA_200'] = df['Close'].rolling(window=200).mean()
        
        # 波动率 (20日)
        df['Volatility_20'] = df['Daily_Return'].rolling(window=20).std() * np.sqrt(252)
        
        return df
    
    # ============ 策略部分 ============
    
    def strategy_buy_and_hold(self) -> Dict:
        """策略1: 买入持有"""
        if self.data is None or len(self.data) < 10:
            return None
            
        df = self.calculate_returns()
        
        initial_price = df['Close'].iloc[0]
        final_price = df['Close'].iloc[-1]
        shares = INITIAL_CAPITAL / initial_price
        
        # 模拟持有过程
        portfolio = []
        for idx, row in df.iterrows():
            value = shares * row['Close']
            portfolio.append({
                'date': row['Date'].strftime('%Y-%m-%d'),
                'value': value,
                'return': (value - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
            })
        
        total_return = (final_price - initial_price) / initial_price * 100
        years = (df['Date'].iloc[-1] - df['Date'].iloc[0]).days / 365.25
        cagr = ((final_price / initial_price) ** (1/years) - 1) * 100 if years > 0 else 0
        
        # 计算最大回撤
        df['Peak'] = df['Close'].cummax()
        df['Drawdown'] = (df['Close'] - df['Peak']) / df['Peak'] * 100
        max_drawdown = df['Drawdown'].min()
        
        # 按时间段统计 (每180天)
        period_returns = []
        for i in range(0, len(df), 180):
            period_df = df.iloc[i:i+180]
            if len(period_df) > 10:
                period_start = period_df['Close'].iloc[0]
                period_end = period_df['Close'].iloc[-1]
                period_return = (period_end - period_start) / period_start * 100
                period_returns.append({
                    'period_start': period_df['Date'].iloc[0].strftime('%Y-%m-%d'),
                    'period_end': period_df['Date'].iloc[-1].strftime('%Y-%m-%d'),
                    'return_pct': round(period_return, 2)
                })
        
        return {
            'strategy': 'Buy and Hold (买入持有)',
            'symbol': self.symbol,
            'initial_capital': INITIAL_CAPITAL,
            'final_value': round(shares * final_price, 2),
            'total_return_pct': round(total_return, 2),
            'cagr_pct': round(cagr, 2),
            'max_drawdown_pct': round(max_drawdown, 2),
            'years': round(years, 2),
            'data_points': len(df),
            'period_returns': period_returns,
            'portfolio_history': portfolio
        }
    
    def strategy_ma_cross(self, short_ma: int = 20, long_ma: int = 50) -> Dict:
        """策略2: 均线交叉策略"""
        if self.data is None or len(self.data) < long_ma + 10:
            return None
            
        df = self.calculate_returns()
        
        cash = INITIAL_CAPITAL
        shares = 0
        position = 0  # 0=空仓, 1=持仓
        
        trades = []
        portfolio = []
        
        for idx, row in df.iterrows():
            if pd.isna(row['MA_20']) or pd.isna(row['MA_50']):
                continue
                
            date_str = row['Date'].strftime('%Y-%m-%d')
            
            # 买入信号: 短均线突破长均线
            if row['MA_20'] > row['MA_50'] and position == 0:
                shares = cash / row['Close']
                cash = 0
                position = 1
                trades.append({
                    'date': date_str,
                    'action': 'BUY',
                    'price': row['Close']
                })
            
            # 卖出信号: 短均线跌破长均线
            elif row['MA_20'] < row['MA_50'] and position == 1:
                cash = shares * row['Close']
                shares = 0
                position = 0
                trades.append({
                    'date': date_str,
                    'action': 'SELL',
                    'price': row['Close']
                })
            
            # 记录组合价值
            value = cash + shares * row['Close']
            portfolio.append({
                'date': date_str,
                'value': value,
                'return': (value - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
            })
        
        final_value = cash + shares * df['Close'].iloc[-1]
        total_return = (final_value - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100
        
        # 按时间段统计
        df_temp = pd.DataFrame(portfolio)
        period_returns = []
        for i in range(0, len(df_temp), 180):
            period_df = df_temp.iloc[i:i+180]
            if len(period_df) > 10:
                period_return = period_df['return'].iloc[-1] - period_df['return'].iloc[0]
                period_returns.append({
                    'period_start': period_df['date'].iloc[0],
                    'period_end': period_df['date'].iloc[-1],
                    'return_pct': round(period_return, 2)
                })
        
        return {
            'strategy': f'MA Cross ({short_ma}/{long_ma}均线交叉)',
            'symbol': self.symbol,
            'initial_capital': INITIAL_CAPITAL,
            'final_value': round(final_value, 2),
            'total_return_pct': round(total_return, 2),
            'total_trades': len(trades),
            'trades': trades,
            'period_returns': period_returns,
            'portfolio_history': portfolio
        }
    
    def strategy_dca(self, interval_days: int = 30, amount: int = None) -> Dict:
        """策略3: 定期定投 (Dollar Cost Averaging)"""
        if self.data is None:
            return None
            
        df = self.calculate_returns()
        
        if amount is None:
            amount = INITIAL_CAPITAL / (len(df) / interval_days)
        
        total_invested = 0
        total_shares = 0
        trades = []
        portfolio = []
        
        invest_idx = 0
        for idx, row in df.iterrows():
            date_str = row['Date'].strftime('%Y-%m-%d')
            
            # 定期买入
            if idx >= invest_idx:
                shares_bought = amount / row['Close']
                total_shares += shares_bought
                total_invested += amount
                trades.append({
                    'date': date_str,
                    'action': 'BUY',
                    'amount': amount,
                    'price': row['Close'],
                    'shares': shares_bought
                })
                invest_idx += interval_days
            
            # 记录组合价值
            value = total_shares * row['Close']
            portfolio.append({
                'date': date_str,
                'value': value,
                'invested': total_invested,
                'return_pct': (value - total_invested) / total_invested * 100 if total_invested > 0 else 0
            })
        
        final_value = total_shares * df['Close'].iloc[-1]
        total_return = (final_value - total_invested) / total_invested * 100 if total_invested > 0 else 0
        
        # 按时间段统计
        df_temp = pd.DataFrame(portfolio)
        period_returns = []
        for i in range(0, len(df_temp), 180):
            period_df = df_temp.iloc[i:i+180]
            if len(period_df) > 10:
                period_invested = period_df['invested'].iloc[-1]
                period_value = period_df['value'].iloc[-1]
                period_return = (period_value - period_invested) / period_invested * 100 if period_invested > 0 else 0
                period_returns.append({
                    'period_start': period_df['date'].iloc[0],
                    'period_end': period_df['date'].iloc[-1],
                    'return_pct': round(period_return, 2)
                })
        
        return {
            'strategy': f'DCA (定期定投，每{interval_days}天)',
            'symbol': self.symbol,
            'total_invested': round(total_invested, 2),
            'final_value': round(final_value, 2),
            'total_return_pct': round(total_return, 2),
            'total_trades': len(trades),
            'period_returns': period_returns,
            'portfolio_history': portfolio
        }
    
    def run_all_strategies(self) -> Dict:
        """运行所有策略并生成报告"""
        if not self.download_data():
            return {'error': f'无法获取 {self.symbol} 的数据'}
        
        results = {
            'symbol': self.symbol,
            'data_range': f"{self.start_date} ~ {self.end_date}",
            'strategies': {}
        }
        
        # 策略1: 买入持有
        bh_result = self.strategy_buy_and_hold()
        if bh_result:
            results['strategies']['buy_and_hold'] = bh_result
        
        # 策略2: 均线交叉
        ma_result = self.strategy_ma_cross(20, 50)
        if ma_result:
            results['strategies']['ma_cross'] = ma_result
        
        # 策略3: 定期定投
        dca_result = self.strategy_dca(30)
        if dca_result:
            results['strategies']['dca'] = dca_result
        
        return results


def run_backtest(symbol: str) -> Dict:
    """运行单只股票的回测"""
    engine = BacktestEngine(symbol)
    return engine.run_all_strategies()


def save_results(results: Dict, output_dir: str = OUTPUT_DIR):
    """保存结果到文件"""
    os.makedirs(output_dir, exist_ok=True)
    
    symbol = results.get('symbol', 'unknown')
    
    # 保存完整JSON
    json_path = os.path.join(output_dir, f"{symbol}.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    # 生成Markdown报告
    md_path = os.path.join(output_dir, f"{symbol}.md")
    generate_markdown_report(results, md_path)
    
    print(f"✅ 结果已保存: {json_path}")
    print(f"✅ 报告已生成: {md_path}")
    
    return json_path, md_path


def generate_markdown_report(results: Dict, output_path: str):
    """生成Markdown格式的投资报告"""
    symbol = results.get('symbol', 'N/A')
    data_range = results.get('data_range', 'N/A')
    
    md_content = f"""# {symbol} 投资回测报告

## 基本信息
- **股票代码**: {symbol}
- **数据时间范围**: {data_range}
- **初始资金**: ¥100,000

---
"""
    
    strategies = results.get('strategies', {})
    
    for strategy_name, strategy_data in strategies.items():
        md_content += f"""
## 📊 {strategy_data.get('strategy', strategy_name)}

### 收益概览
| 指标 | 数值 |
|------|------|
| 初始资金 | ¥{strategy_data.get('initial_capital', strategy_data.get('total_invested', 'N/A')):,.2f} |
| 最终价值 | ¥{strategy_data.get('final_value', 'N/A'):,.2f} |
| 总收益率 | {strategy_data.get('total_return_pct', 'N/A'):.2f}% |
"""
        
        if 'cagr_pct' in strategy_data:
            md_content += f"| 年化收益率(CAGR) | {strategy_data.get('cagr_pct', 'N/A'):.2f}% |\n"
        if 'max_drawdown_pct' in strategy_data:
            md_content += f"| 最大回撤 | {strategy_data.get('max_drawdown_pct', 'N/A'):.2f}% |\n"
        if 'total_trades' in strategy_data:
            md_content += f"| 总交易次数 | {strategy_data.get('total_trades', 'N/A')} |\n"
        
        # 时间段收益
        period_returns = strategy_data.get('period_returns', [])
        if period_returns:
            md_content += """
### 📅 各时间段收益率

| 时间段 | 收益率 |
|--------|--------|
"""
            for period in period_returns[:10]:  # 只显示前10个时间段
                md_content += f"| {period['period_start']} ~ {period['period_end']} | {period['return_pct']:+.2f}% |\n"
        
        # 交易记录
        if 'trades' in strategy_data and strategy_data['trades']:
            trades = strategy_data['trades'][:20]  # 只显示前20笔
            md_content += """
### 📝 交易记录

| 日期 | 操作 | 价格 |
|------|------|------|
"""
            for trade in trades:
                md_content += f"| {trade['date']} | {trade['action']} | ¥{trade['price']:.2f} |\n"
        
        md_content += "\n---\n"
    
    # 策略对比
    if len(strategies) > 1:
        md_content += """
## 📈 策略对比

| 策略 | 总收益率 | 年化收益率 |
|------|----------|------------|
"""
        for name, data in strategies.items():
            md_content += f"| {data.get('strategy', name)} | {data.get('total_return_pct', 0):+.2f}% | {data.get('cagr_pct', 'N/A')}%\n"
    
    md_content += """
---
*本报告由自动回测系统生成，仅供参考，不构成投资建议。*
"""
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(md_content)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        # 命令行参数: python backtest.py AAPL
        symbol = sys.argv[1]
        print(f"开始回测: {symbol}")
        results = run_backtest(symbol)
        save_results(results)
        print("\n回测完成!")
    else:
        # 默认测试
        print("=" * 50)
        print("股票回测框架测试")
        print("=" * 50)
        
        # 测试几只股票
        test_symbols = ['AAPL', 'MSFT', 'GOOGL']
        
        for symbol in test_symbols:
            print(f"\n{'='*40}")
            print(f"测试股票: {symbol}")
            print("=" * 40)
            
            results = run_backtest(symbol)
            
            if 'error' not in results:
                # 显示简要结果
                for strategy_name, data in results['strategies'].items():
                    print(f"\n📊 {data['strategy']}")
                    print(f"   总收益率: {data['total_return_pct']:+.2f}%")
                    if 'cagr_pct' in data:
                        print(f"   年化收益率: {data['cagr_pct']:.2f}%")
                
                # 保存结果
                save_results(results)
            else:
                print(f"❌ 错误: {results['error']}")