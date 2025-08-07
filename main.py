import os
import requests
import pandas as pd
import tushare as ts
from datetime import datetime

# 飞书接口配置
FEISHU_API = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
TOKEN_API = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"

# 从环境变量获取参数
config = {
    "tushare_token": os.getenv('TUSHARE_TOKEN'),
    "app_id": os.getenv('FEISHU_APP_ID'),
    "app_secret": os.getenv('FEISHU_APP_SECRET'),
    "app_token": os.getenv('FEISHU_APP_TOKEN'),
    "table_id": os.getenv('FEISHU_TABLE_ID')
}

def get_feishu_token():
    """获取飞书访问令牌"""
    res = requests.post(TOKEN_API, json={
        "app_id": config["app_id"],
        "app_secret": config["app_secret"]
    })
    return res.json().get('tenant_access_token')

def push_to_feishu(data):
    """推送数据到飞书多维表格"""
    token = get_feishu_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = FEISHU_API.format(app_token=config["app_token"], table_id=config["table_id"])
    
    # 构建飞书数据格式
    records = [{"fields": {
        "股票代码": f'{row["ts_code"][:6]}',
        "股票名称": row["name"],
        "涨停价格": row["close"],
        "涨停日期": row["trade_date"]
    }} for _, row in data.iterrows()]
    
    # 分批写入（飞书单次上限100条）
    for i in range(0, len(records), 100):
        res = requests.post(url, headers=headers, json={"records": records[i:i+100]})
        print(f"推送状态: {res.status_code}, 响应: {res.text}")

def main():
    """主函数：获取涨停股并推送到飞书"""
    # 设置tushare
    ts.set_token(config["tushare_token"])
    pro = ts.pro_api()
    
    # 获取当日涨停股
    today = datetime.now().strftime("%Y%m%d")
    df = pro.limit_list(trade_date=today, limit_type='U', fields='ts_code,name,close,trade_date')
    
    if not df.empty:
        push_to_feishu(df)
        print(f"成功推送 {len(df)} 条涨停数据")
    else:
        print("今日无涨停股票数据")

if __name__ == "__main__":
    main()
