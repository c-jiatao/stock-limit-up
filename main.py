#!/usr/bin/env python3
# main.py
# 功能：抓取当日 A 股涨停池（AKShare），并把结果写入飞书多维表格（Base）
# 说明：请在仓库 Secrets 里设置 FEISHU_APP_ID, FEISHU_APP_SECRET
#       并在 workflow 的环境变量中设置 FEISHU_APP_TOKEN 与 FEISHU_TABLE_ID（或也放 Secrets）

import os, sys, json, requests
from datetime import datetime, timedelta

def today_yyyymmdd():
    # 使用 UTC+8 的本地日期（避免时区复杂性）
    return (datetime.utcnow() + timedelta(hours=8)).strftime("%Y%m%d")

def fetch_limit_up_with_ak(date_str):
    try:
        import akshare as ak
    except Exception as e:
        print("缺少 akshare，请先安装：pip install akshare")
        raise
    try:
        df = ak.stock_zt_pool_em(date=date_str)
        return df
    except Exception as e:
        print("调用 AKShare 获取涨停池失败：", e)
        return None

def get_tenant_access_token(app_id, app_secret):
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    data = {"app_id": app_id, "app_secret": app_secret}
    r = requests.post(url, headers=headers, json=data, timeout=10)
    r.raise_for_status()
    j = r.json()
    # 正常响应示例包含 {"code":0, "msg":"ok", "tenant_access_token":"xxx","expire":7200}
    if j.get("code", 0) != 0:
        raise Exception(f"获取 tenant_access_token 错误: {j}")
    return j["tenant_access_token"]

def build_records_from_df(df, date_str):
    records = []
    # akshare 返回的列通常包含：'序号','代码','名称','最新价','涨跌幅','涨停价','封单金额','首次封板时间', ...
    for _, row in df.iterrows():
        code = str(row.get("代码") or row.get("代码") or row.get("代码",""))
        name = row.get("名称") or row.get("名称","")
        latest = row.get("最新价") if ("最新价" in row) else (row.get("最新价", None))
        pct = row.get("涨跌幅") or row.get("涨幅") or row.get("涨停幅", "")
        limit_price = row.get("涨停价", "")
        fd_amount = row.get("封单金额", "")
        first_time = row.get("首次封板时间", "")
        rec = {
            "fields": {
                "日期": date_str,
                "代码": code,
                "名称": name,
                "最新价": float(latest) if latest not in (None, "") else "",
                "涨幅": float(pct) if pct not in (None, "") else "",
                "涨停价": float(limit_price) if limit_price not in (None, "") else "",
                "封单金额": str(fd_amount),
                "首次封板时间": str(first_time)
            }
        }
        records.append(rec)
    return records

def push_records_to_feishu(app_token, table_id, tenant_token, records):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
    headers = {
        "Authorization": f"Bearer {tenant_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    body = {"records": records}
    r = requests.post(url, headers=headers, json=body, timeout=15)
    r.raise_for_status()
    return r.json()

def main():
    app_id = os.environ.get("FEISHU_APP_ID")
    app_secret = os.environ.get("FEISHU_APP_SECRET")
    app_token = os.environ.get("FEISHU_APP_TOKEN")
    table_id = os.environ.get("FEISHU_TABLE_ID")

    if not app_id or not app_secret:
        print("需要在仓库 Secrets 中设置 FEISHU_APP_ID 与 FEISHU_APP_SECRET")
        sys.exit(1)
    if not app_token or not table_id:
        print("需要在 workflow 环境变量中设置 FEISHU_APP_TOKEN 与 FEISHU_TABLE_ID")
        sys.exit(1)

    date_str = today_yyyymmdd()
    print("准备抓取日期：", date_str)
    df = fetch_limit_up_with_ak(date_str)
    if df is None:
        print("未获取到数据（AKShare 返回为空或请求失败）。请检查网络或代码日志。")
        return
    if df.empty:
        print("当天无涨停数据（可能是周末或非交易日）。")
        return

    print("获取到涨停股票数：", len(df))
    records = build_records_from_df(df, date_str)
    # 分批上传：单次 API 支持最多 500 条，这里按 200 分批（视需要）
    batch_size = 200
    tenant_token = get_tenant_access_token(app_id, app_secret)
    for i in range(0, len(records), batch_size):
        chunk = records[i:i+batch_size]
        resp = push_records_to_feishu(app_token, table_id, tenant_token, chunk)
        print("上传分片结果：", resp.get("code"), resp.get("msg"))
    print("全部上传完成。")

if __name__ == "__main__":
    main()
