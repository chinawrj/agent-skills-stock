#!/usr/bin/env python3
"""
Pushover 推送通知脚本

使用前请确保已 source 凭证文件:
    source ~/.pushover_credentials

或在 .bashrc/.zshrc 中添加:
    source ~/.pushover_credentials
"""

import os
import sys
import requests
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta


def get_credentials():
    """从环境变量获取 Pushover 凭证"""
    app_token = os.environ.get("PUSHOVER_APP_TOKEN")
    user_key = os.environ.get("PUSHOVER_USER_KEY")
    
    if not app_token or not user_key:
        print("错误: 未找到 Pushover 凭证")
        print("请先执行: source ~/.pushover_credentials")
        sys.exit(1)
    
    return app_token, user_key


def push_notification(title: str, message: str, html: bool = True) -> dict:
    """
    发送 Pushover 推送通知
    
    Args:
        title: 消息标题
        message: 消息内容
        html: 是否使用 HTML 格式
    
    Returns:
        API 响应
    """
    app_token, user_key = get_credentials()
    
    data = {
        "token": app_token,
        "user": user_key,
        "title": title,
        "message": message,
    }
    
    if html:
        data["html"] = 1
    
    response = requests.post(
        "https://api.pushover.net/1/messages.json",
        data=data
    )
    
    return response.json()


def get_stock_price_changes(codes: list) -> dict:
    """
    获取股票涨跌幅数据（日/周/月）
    
    Args:
        codes: 股票代码列表
    
    Returns:
        {代码: {'day': 日涨跌幅, 'week': 周涨跌幅, 'month': 月涨跌幅}}
    """
    import akshare as ak
    
    result = {}
    
    # 先从实时行情获取日涨跌幅和60日涨跌幅
    try:
        df_realtime = ak.stock_zh_a_spot_em()
        df_realtime['代码'] = df_realtime['代码'].astype(str)
        realtime_dict = df_realtime.set_index('代码')[['涨跌幅', '60日涨跌幅']].to_dict('index')
    except:
        realtime_dict = {}
    
    # 计算周涨跌幅需要历史数据
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=40)).strftime('%Y%m%d')
    
    for code in codes:
        code_str = str(code).zfill(6)
        
        # 默认值
        result[code_str] = {'day': None, 'week': None, 'month': None}
        
        # 从实时数据获取日涨跌幅
        if code_str in realtime_dict:
            result[code_str]['day'] = realtime_dict[code_str].get('涨跌幅')
        
        # 获取历史数据计算周/月涨跌幅
        try:
            df_hist = ak.stock_zh_a_hist(
                symbol=code_str, period="daily", 
                start_date=start_date, end_date=end_date, adjust="qfq"
            )
            if len(df_hist) >= 2:
                latest_price = df_hist.iloc[-1]['收盘']
                
                # 周涨跌幅（5个交易日前）
                if len(df_hist) >= 6:
                    week_ago_price = df_hist.iloc[-6]['收盘']
                    result[code_str]['week'] = round((latest_price - week_ago_price) / week_ago_price * 100, 2)
                
                # 月涨跌幅（20个交易日前）
                if len(df_hist) >= 21:
                    month_ago_price = df_hist.iloc[-21]['收盘']
                    result[code_str]['month'] = round((latest_price - month_ago_price) / month_ago_price * 100, 2)
        except:
            pass
    
    return result


def format_shareholders_report(csv_path: str, top_n: int = 10, full: bool = False, with_price: bool = False) -> list:
    """
    从 CSV 文件生成 HTML 格式的股东人数报告
    
    Pushover 支持的 HTML 标签:
    - <b>加粗</b>
    - <i>斜体</i>
    - <u>下划线</u>
    - <font color="#hex">颜色</font>
    - <a href="url">链接</a>
    
    Args:
        csv_path: CSV 文件路径
        top_n: 显示前 N 只股票（full=True 时忽略）
        full: 是否发送完整结果（分批发送）
        with_price: 是否包含涨跌幅数据（日/周/月）
    
    Returns:
        消息列表（可能多条，用于分批发送）
    """
    df = pd.read_csv(csv_path, dtype={'代码': str})
    total = len(df)
    
    # 统计信息
    avg_drop = df['总降幅(%)'].mean()
    max_drop = df['总降幅(%)'].min()
    
    # 获取数据时间范围
    if '最新日期' in df.columns:
        latest_date = df['最新日期'].iloc[0]
    else:
        latest_date = datetime.now().strftime('%Y-%m-%d')
    
    # 获取涨跌幅数据
    price_changes = {}
    if with_price:
        print("  获取涨跌幅数据...")
        codes = df['代码'].tolist()
        price_changes = get_stock_price_changes(codes)
    
    def format_price_change(val):
        """格式化涨跌幅显示"""
        if val is None:
            return "<font color=\"#95a5a6\">--</font>"
        color = "#2ecc71" if val >= 0 else "#e74c3c"
        return f"<font color=\"{color}\">{val:+.1f}%</font>"
    
    def format_stock_line(i, row, include_price=False):
        """格式化单只股票行"""
        code = row['代码']
        name = str(row['名称'])[:4]
        drop = row['总降幅(%)']
        latest = row['最新股东数']
        
        # 根据降幅设置颜色
        if drop <= -20:
            color = "#e74c3c"
        elif drop <= -10:
            color = "#e67e22"
        else:
            color = "#f39c12"
        
        line = f"<b>{code}</b> {name:<4} <font color=\"{color}\">{drop:>+.1f}%</font>"
        
        if include_price and code in price_changes:
            pc = price_changes[code]
            day_str = format_price_change(pc.get('day'))
            week_str = format_price_change(pc.get('week'))
            month_str = format_price_change(pc.get('month'))
            line += f"\n      日{day_str} 周{week_str} 月{month_str}"
        
        return f"   {i:>2}. {line}\n"
    
    # Pushover 消息长度限制
    MAX_MSG_LEN = 1000  # 留一些余量
    
    # 统一使用分批发送模式
    messages = []
    
    # 第一条：摘要信息
    header = f"""<b>━━━━ 📊 股东人数筛选报告 ━━━━</b>

<font color="#4a90d9">▎筛选结果</font>
   共 <b>{total}</b> 只股票符合条件
   数据截止: {latest_date}

<font color="#4a90d9">▎统计汇总</font>
   平均降幅: <font color="#e74c3c"><b>{avg_drop:.1f}%</b></font>
   最大降幅: <font color="#e74c3c"><b>{max_drop:.1f}%</b></font>

<font color="#95a5a6"><i>📈 筹码集中 = 庄家吸筹信号</i></font>"""
    messages.append(header)
    
    # 确定要发送的股票
    df_to_send = df if full else df.head(top_n)
    total_to_send = len(df_to_send)
    
    # 动态分批：根据消息长度自动拆分
    price_header = "<i>日/周/月涨跌幅</i>\n" if with_price else ""
    current_batch = f"<font color=\"#4a90d9\">▎股票列表</font>\n{price_header}"
    batch_start = 1
    
    for i, (_, row) in enumerate(df_to_send.iterrows(), 1):
        line = format_stock_line(i, row, include_price=with_price)
        
        # 检查是否会超过长度限制
        if len(current_batch) + len(line) > MAX_MSG_LEN:
            # 保存当前批次
            messages.append(current_batch)
            # 开始新批次
            current_batch = f"<font color=\"#4a90d9\">▎股票列表 (续)</font>\n{price_header}"
        
        current_batch += line
    
    # 保存最后一批
    if current_batch.strip():
        messages.append(current_batch)
    
    return messages


def main():
    parser = argparse.ArgumentParser(description='Pushover 推送通知')
    parser.add_argument('-t', '--title', type=str, default='A股筛选通知',
                        help='消息标题')
    parser.add_argument('-m', '--message', type=str,
                        help='消息内容（与 --csv 二选一）')
    parser.add_argument('-c', '--csv', type=str,
                        help='从 CSV 文件生成报告（股东人数筛选结果）')
    parser.add_argument('-n', '--top', type=int, default=10,
                        help='显示前 N 只股票（默认10）')
    parser.add_argument('--full', action='store_true',
                        help='发送完整结果（分批推送多条消息）')
    parser.add_argument('--with-price', action='store_true',
                        help='包含涨跌幅数据（日/周/月）')
    parser.add_argument('--no-html', action='store_true',
                        help='禁用 HTML 格式')
    
    args = parser.parse_args()
    
    if args.csv:
        # 从 CSV 生成报告
        csv_path = args.csv
        if not os.path.exists(csv_path):
            # 尝试在项目根目录查找
            project_root = Path(__file__).parent.parent.parent.parent.parent
            csv_path = project_root / args.csv
        
        messages = format_shareholders_report(str(csv_path), args.top, full=args.full, with_price=args.with_price)
        title = args.title
        
        # 发送所有消息
        import time
        for i, message in enumerate(messages):
            if i > 0:
                time.sleep(0.5)  # 避免频率限制
            result = push_notification(title, message, html=not args.no_html)
            if result.get('status') == 1:
                print(f"✅ 推送成功 ({i+1}/{len(messages)})")
            else:
                print(f"❌ 推送失败 ({i+1}/{len(messages)}): {result}")
        
        print(f"   共发送 {len(messages)} 条消息")
        return
        
    elif args.message:
        message = args.message
        title = args.title
    else:
        print("错误: 请指定 --message 或 --csv 参数")
        sys.exit(1)
    
    # 发送推送
    result = push_notification(title, message, html=not args.no_html)
    
    if result.get('status') == 1:
        print(f"✅ 推送成功!")
        print(f"   请求ID: {result.get('request')}")
    else:
        print(f"❌ 推送失败: {result}")


if __name__ == "__main__":
    main()
