import os
import requests
import pandas as pd
import akshare as ak
from datetime import datetime

# 飞书配置
FEISHU_API = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
TOKEN_API = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"

# 环境变量
config = {
    "app_id": os.getenv('FEISHU_APP_ID'),
    "app_secret": os.getenv('FEISHU_APP_SECRET'),
    "app_token": os.getenv('FEISHU_APP_TOKEN'),
    "table_id": os.getenv('FEISHU_TABLE_ID')
}

def get_feishu_token():
    """获取飞书token"""
    res = requests.post(TOKEN_API, json={
        "app_id": config["app_id"],
        "app_secret": config["app_secret"]
    })
    return res.json().get('tenant_access_token')

def push_to_feishu(data):
    """推送数据到飞书"""
    token = get_feishu_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = FEISHU_API.format(app_token=config["app_token"], table_id=config["table_id"])
    
    records = [{"fields": {
        "股票代码": row["代码"],
        "股票名称": row["名称"],
        "涨停价格": row["最新价"],
        "涨停日期": datetime.now().strftime("%Y%m%d"),
        "涨幅%": round(row["涨跌幅"], 2),
        "行业": row.get("所属行业", "未知"),
        "涨停原因": row.get("涨停原因", "常规涨停")
    }} for _, row in data.iterrows()]
    
    # 分批写入
    for i in range(0, len(records), 100):
        res = requests.post(url, headers=headers, json={"records": records[i:i+100]})
        print(f"推送状态: {res.status_code}")

def main():
    # 使用AkShare获取涨停股数据
    try:
        # 东方财富涨停板数据
        df = ak.stock_zt_pool_em(date=datetime.now().strftime("%Y%m%d"))
        
        if not df.empty:
            print(f"获取到 {len(df)} 只涨停股")
            push_to_feishu(df)
        else:
            print("今日无涨停股票")
    except Exception as e:
        print(f"数据获取异常: {e}")
        # 备选数据源：新浪财经
        try:
            df = ak.stock_zh_a_spot_em()
            # 筛选涨幅≥9.5%的股票
            df = df[df["涨跌幅"] >= 9.5]
            print(f"从新浪获取到 {len(df)} 只涨停股")
            push_to_feishu(df)
        except Exception as e2:
            print(f"备选数据源异常: {e2}")

if __name__ == "__main__":
    main()
