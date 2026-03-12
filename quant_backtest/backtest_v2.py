#!/usr/bin/env python3
"""
股票回测框架 v2
支持多种策略：买入持有、均线交叉、定期定投
数据源：akshare (A股/港股), yfinance (美股)
"""

import akshare as ak
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import time
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# 配置
START_DATE = "20000101"
END_DATE = datetime.now().strftime("%Y%m%d")
INITIAL_CAPITAL = 100000  # 初始资金
OUTPUT_DIR = "/root/.openclaw/workspace/quant_backtest/results"
SLEEP_INTERVAL = 30  # 每完成一只股票休眠30秒

class BacktestEngine:
    def __init__(self, symbol: str, market: str = "auto", start_date: str = START_DATE, end_date: str = END_DATE):
        self.symbol = symbol
        self.market = market  # "a股", "港股", "美股", "auto"
        self.start_date = start_date
        self.end_date = end_date
        self.data = None
        self.company_name = ""
        
    def detect_market(self) -> str:
        """自动识别市场"""
        if self.market and self.market.lower() != "auto":
            return self.market.lower()
        
        # 根据代码判断
        if self.symbol.isdigit():
            return "a股"
        if self.symbol.endswith('.HK'):
            return "港股"
        if self.symbol.endswith('.T') or self.symbol.isdigit() == False:
            return "美股"
        if '.' in self.symbol:
            # 如 2222.SR 为沙特
            return "其他"
        return "美股"
    
    def download_data(self) -> bool:
        """下载股票数据"""
        market = self.detect_market()
        
        try:
            if market == "a股":
                return self._download_a_stock()
            elif market == "港股":
                return self._download_hk_stock()
            else:
                return self._download_us_stock()
        except Exception as e:
            print(f"❌ 下载失败: {e}")
            return False
    
    def _download_a_stock(self) -> bool:
        """下载A股数据"""
        try:
            print(f"正在下载 A股 {self.symbol} 数据...")
            # A股需要6位数字代码
            symbol_code = self.symbol.zfill(6) if self.symbol.isdigit() else self.symbol
            
            df = ak.stock_zh_a_hist(symbol=symbol_code, adjust='qfq', 
                                   start_date=self.start_date, end_date=self.end_date)
            
            if df.empty:
                print(f"⚠️ A股 {self.symbol} 没有数据")
                return False
            
            # 重命名列
            df = df.rename(columns={
                '日期': 'Date',
                '开盘': 'Open',
                '收盘': 'Close',
                '最高': 'High',
                '最低': 'Low',
                '成交量': 'Volume',
                '成交额': 'Amount',
                '振幅': 'Amplitude',
                '涨跌幅': 'ChangePct',
                '涨跌额': 'Change',
                '换手率': 'Turnover'
            })
            
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date')
            
            # 过滤零值和异常数据
            df = df[df['Close'] > 0]
            
            self.data = df
            
            # 获取公司名称
            try:
                stock_info = ak.stock_individual_info_em(symbol=symbol_code)
                self.company_name = stock_info[stock_info['item'] == '股票简称']['value'].values[0] if len(stock_info) > 0 else symbol_code
            except:
                self.company_name = symbol_code
            
            print(f"✅ 获取到 A股 {self.symbol} ({self.company_name}) 数据: {len(df)} 条")
            print(f"   时间范围: {df['Date'].min().strftime('%Y-%m-%d')} ~ {df['Date'].max().strftime('%Y-%m-%d')}")
            return True
            
        except Exception as e:
            print(f"❌ A股 {self.symbol} 下载失败: {e}")
            return False
    
    def _download_hk_stock(self) -> bool:
        """下载港股数据"""
        try:
            print(f"正在下载 港股 {self.symbol} 数据...")
            
            # 港股代码处理
            hk_code = self.symbol.replace('.HK', '').replace('HK', '')
            
            # 使用yfinance获取港股
            ticker = yf.Ticker(f"{hk_code}.HK")
            df = ticker.history(start="2000-01-01", end=datetime.now().strftime("%Y-%m-%d"))
            
            if df.empty:
                print(f"⚠️ 港股 {self.symbol} 没有数据")
                return False
            
            df.reset_index(inplace=True)
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
            df = df.rename(columns={
                'Open': 'Open',
                'Close': 'Close',
                'High': 'High',
                'Low': 'Low',
                'Volume': 'Volume'
            })
            
            self.company_name = hk_code
            self.data = df
            
            print(f"✅ 获取到 港股 {self.symbol} 数据: {len(df)} 条")
            return True
            
        except Exception as e:
            print(f"❌ 港股 {self.symbol} 下载失败: {e}")
            return False
    
    def _download_us_stock(self) -> bool:
        """下载美股数据"""
        try:
            print(f"正在下载 美股 {self.symbol} 数据...")
            
            ticker = yf.Ticker(self.symbol)
            df = ticker.history(start="2000-01-01", end=datetime.now().strftime("%Y-%m-%d"))
            
            if df.empty:
                print(f"⚠️ 美股 {self.symbol} 没有数据")
                return False
            
            df.reset_index(inplace=True)
            df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)
            
            self.company_name = self.symbol
            self.data = df
            
            print(f"✅ 获取到 美股 {self.symbol} 数据: {len(df)} 条")
            return True
            
        except Exception as e:
            print(f"❌ 美股 {self.symbol} 下载失败: {e}")
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
                'value': round(value, 2),
                'return': round((value - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100, 2)
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
                    'price': round(row['Close'], 2)
                })
            
            # 卖出信号: 短均线跌破长均线
            elif row['MA_20'] < row['MA_50'] and position == 1:
                cash = shares * row['Close']
                shares = 0
                position = 0
                trades.append({
                    'date': date_str,
                    'action': 'SELL',
                    'price': round(row['Close'], 2)
                })
            
            # 记录组合价值
            value = cash + shares * row['Close']
            portfolio.append({
                'date': date_str,
                'value': round(value, 2),
                'return': round((value - INITIAL_CAPITAL) / INITIAL_CAPITAL * 100, 2)
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
            amount = int(INITIAL_CAPITAL / (len(df) / interval_days))
        
        total_invested = 0
        total_shares = 0
        trades = []
        portfolio = []
        
        invest_idx = 0
        for idx, row in df.iterrows():
            date_str = row['Date'].strftime('%Y-%m-%d')
            
            # 定期买入
            if idx >= invest_idx and total_invested < INITIAL_CAPITAL * 10 and row['Close'] > 0:  # 限制最大投入
                shares_bought = amount / row['Close']
                total_shares += shares_bought
                total_invested += amount
                trades.append({
                    'date': date_str,
                    'action': 'BUY',
                    'amount': amount,
                    'price': round(row['Close'], 2),
                    'shares': round(shares_bought, 4)
                })
                invest_idx += interval_days
            
            # 记录组合价值
            value = total_shares * row['Close']
            portfolio.append({
                'date': date_str,
                'value': round(value, 2),
                'invested': round(total_invested, 2),
                'return_pct': round((value - total_invested) / total_invested * 100, 2) if total_invested > 0 else 0
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
            'company_name': self.company_name,
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


def run_backtest(symbol: str, market: str = "auto") -> Dict:
    """运行单只股票的回测"""
    engine = BacktestEngine(symbol, market, "20000101", "20260313")
    result = engine.run_all_strategies()
    # 添加原始数据用于保存
    result['data'] = engine.data
    return result


def save_results(results: Dict) -> Tuple[str, str]:
    """保存结果到文件"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    symbol = results.get('symbol', 'unknown')
    
    # 保存完整JSON（排除DataFrame）
    json_path = os.path.join(OUTPUT_DIR, f"{symbol}.json")
    save_data = {k: v for k, v in results.items() if k != 'data'}
    # 转换DataFrame为列表
    if results.get('data') is not None:
        save_data['data_rows'] = len(results['data'])
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(save_data, f, ensure_ascii=False, indent=2)
    
    # 生成Markdown报告
    md_path = os.path.join(OUTPUT_DIR, f"{symbol}.md")
    generate_markdown_report(results, md_path)
    
    return json_path, md_path


def generate_markdown_report(results: Dict, output_path: str):
    """生成Markdown格式的投资报告"""
    symbol = results.get('symbol', 'N/A')
    company_name = results.get('company_name', symbol)
    data_range = results.get('data_range', 'N/A')
    
    md_content = f"""# {symbol} - {company_name} 投资回测报告

## 基本信息
- **股票代码**: {symbol}
- **公司名称**: {company_name}
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
| 总收益率 | {strategy_data.get('total_return_pct', 'N/A'):+.2f}% |
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
            for period in period_returns[:10]:
                md_content += f"| {period['period_start']} ~ {period['period_end']} | {period['return_pct']:+.2f}% |\n"
        
        # 交易记录
        if 'trades' in strategy_data and strategy_data['trades']:
            trades = strategy_data['trades'][:20]
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

| 策略 | 总收益率 | 备注 |
|------|----------|------|
"""
        for name, data in strategies.items():
            md_content += f"| {data.get('strategy', name)} | {data.get('total_return_pct', 0):+.2f}% | |\n"
    
    md_content += """
---
*本报告由自动回测系统生成，仅供参考，不构成投资建议。*
"""
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(md_content)


if __name__ == "__main__":
    import sys
    
    # 默认测试股票列表
    test_stocks = [
        ("601318", "a股"),   # 中国平安
        ("600519", "a股"),   # 茅台
        ("000001", "a股"),   # 平安银行
        ("0700.HK", "港股"), # 腾讯
        ("AAPL", "美股"),    # 苹果
    ]
    
    print("=" * 60)
    print("股票回测框架 v2 - 多市场测试")
    print("=" * 60)
    
    for symbol, market in test_stocks:
        print(f"\n{'='*50}")
        print(f"▶ 正在回测: {symbol} ({market})")
        print("=" * 50)
        
        results = run_backtest(symbol, market)
        
        if 'error' not in results:
            # 显示简要结果
            print("\n📊 策略结果:")
            for strategy_name, data in results['strategies'].items():
                print(f"   • {data['strategy']}: {data['total_return_pct']:+.2f}%")
            
            # 保存结果
            json_path, md_path = save_results(results)
            print(f"\n✅ 结果已保存:")
            print(f"   JSON: {json_path}")
            print(f"   报告: {md_path}")
        else:
            print(f"❌ 错误: {results['error']}")
        
        # 休眠30秒
        print(f"\n😴 休眠 {SLEEP_INTERVAL} 秒...")
        time.sleep(SLEEP_INTERVAL)
    
    print("\n" + "=" * 60)
    print("所有测试完成!")
    print("=" * 60)