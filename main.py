import requests
import json
from datetime import datetime

# 1. 你的飞书 App 配置
APP_ID = "cli_a819f9a899bed00d"
APP_SECRET = "tQCP2GE4BXw0RlQctRS7peUQiAqe1PXG"

# 2. 你的飞书多维表格 ID 信息
APP_TOKEN = "JsWqbseLxaktydsnElVcIGCCnLf"  # 从链接里提取
TABLE_ID = "tbljZZCk6C97kLkW"  # 从链接里提取

# 3. 获取 tenant_access_token
def get_tenant_access_token():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json"}
    data = {"app_id": APP_ID, "app_secret": APP_SECRET}
    resp = requests.post(url, headers=headers, json=data)
    token = resp.json()["tenant_access_token"]
    return token

# 4. 写入测试数据
def insert_test_record():
    token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    today = datetime.now().strftime("%Y-%m-%d")
    data = {
        "fields": {
            "日期": today,
            "代码": "TEST001",
            "名称": "测试股票",
            "最新价": 123.45,
            "涨幅": 9.99,
            "涨停价": 130.00,
            "封单金额": "1亿",
            "首次封板时间": "09:25"
        }
    }
    resp = requests.post(url, headers=headers, json={"records": [data]})
    print("返回结果：", resp.json())

if __name__ == "__main__":
    insert_test_record()
