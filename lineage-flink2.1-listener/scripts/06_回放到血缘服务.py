#!/usr/bin/env python3
import argparse
import base64
import json
import time
import urllib.error
import urllib.request
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="把 Flink Listener 事件里的 schema 回放到 flink-sql-lineage。")
    parser.add_argument("--事件文件", default="/tmp/flink21-lineage-events.jsonl")
    parser.add_argument("--SQL文件", default=str(Path(__file__).resolve().parents[1] / "sql" / "listener-insert.sql"))
    parser.add_argument("--血缘服务", default="http://127.0.0.1:8194")
    parser.add_argument("--用户名", default="admin")
    parser.add_argument("--密码", default="admin")
    parser.add_argument("--catalogId", type=int, default=1)
    parser.add_argument("--database", default="default")
    parser.add_argument("--任务名", default=None)
    args = parser.parse_args()
    auth_header = basic_auth(args.用户名, args.密码)

    event = read_last_event(Path(args.事件文件))
    insert_sql = read_insert_sql(Path(args.SQL文件))
    task_name = args.任务名 or "监听器回放-%d" % int(time.time())

    datasets = event.get("inputs", []) + event.get("outputs", [])
    if not datasets:
        raise SystemExit("事件里没有 inputs/outputs：%s" % args.事件文件)

    for dataset in datasets:
        ddl = to_create_table_ddl(dataset)
        print("\n--- 注册表：%s" % dataset.get("name"))
        print(ddl)
        request_json(
            "POST",
            "%s/catalogs/%s/databases/%s/tables" % (args.血缘服务, args.catalogId, args.database),
            {"ddl": base64.b64encode(ddl.encode("utf-8")).decode("ascii")},
            auth_header,
            tolerate_existing=True,
        )

    print("\n--- 创建血缘任务：%s" % task_name)
    task_id = request_json(
        "POST",
        "%s/tasks" % args.血缘服务,
        {
            "taskName": task_name,
            "descr": "把 Flink 2.1 Listener schema 回放到 flink-sql-lineage",
            "catalogId": args.catalogId,
            "database": args.database,
        },
        auth_header,
    )["data"]

    encoded_sql = base64.b64encode(insert_sql.encode("utf-8")).decode("ascii")
    request_json(
        "PUT",
        "%s/tasks/%s" % (args.血缘服务, task_id),
        {
            "taskName": task_name,
            "descr": "把 Flink 2.1 Listener schema 回放到 flink-sql-lineage",
            "catalogId": args.catalogId,
            "source": encoded_sql,
        },
        auth_header,
    )

    print("\n--- 分析血缘任务：%s" % task_id)
    result = request_json("POST", "%s/tasks/%s/lineage" % (args.血缘服务, task_id), {}, auth_header)
    data = result.get("data") or {}
    print("taskId=%s" % task_id)
    print("taskStatus=%s" % data.get("taskStatus"))
    print("taskLog=%s" % data.get("taskLog"))


def read_last_event(path):
    lines = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    if not lines:
        raise SystemExit("事件文件为空：%s" % path)
    return json.loads(lines[-1])


def read_insert_sql(path):
    text = path.read_text()
    statements = [statement.strip() for statement in text.split(";") if statement.strip()]
    inserts = [statement for statement in statements if statement.upper().startswith("INSERT")]
    if not inserts:
        raise SystemExit("SQL 文件里没有 INSERT：%s" % path)
    return inserts[-1]


def to_create_table_ddl(dataset):
    table = table_name(dataset.get("name", "unknown_table"))
    columns = dataset.get("schema") or []
    if not columns:
        raise SystemExit("数据集没有 schema：%s" % dataset.get("name"))

    column_lines = []
    for column in columns:
        column_lines.append("    `%s` %s" % (column["name"], normalize_type(column["type"])))

    options = ((dataset.get("table") or {}).get("options") or {}).copy()
    options.setdefault("connector", "blackhole")
    option_lines = []
    for key in sorted(options):
        option_lines.append("    '%s' = '%s'" % (escape_sql(key), escape_sql(str(options[key]))))

    return (
        "CREATE TABLE IF NOT EXISTS `%s` (\n%s\n) WITH (\n%s\n)"
        % (table, ",\n".join(column_lines), ",\n".join(option_lines))
    )


def table_name(identifier):
    return identifier.split(".")[-1].strip("`")


def normalize_type(type_name):
    return type_name.replace(" NOT NULL", "")


def escape_sql(value):
    return value.replace("'", "''")


def basic_auth(username, password):
    token = base64.b64encode(("%s:%s" % (username, password)).encode("utf-8")).decode("ascii")
    return "Basic %s" % token


def request_json(method, url, payload, auth_header, tolerate_existing=False):
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "Authorization": auth_header},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            text = response.read().decode("utf-8")
            result = json.loads(text)
            if result.get("code") != 200:
                if tolerate_existing and "exist" in json.dumps(result).lower():
                    print("忽略已存在对象：%s" % result)
                    return result
                raise SystemExit("请求失败：%s %s -> %s" % (method, url, result))
            return result
    except urllib.error.HTTPError as error:
        text = error.read().decode("utf-8", errors="replace")
        if tolerate_existing and "exist" in text.lower():
            print("忽略已存在对象：%s" % text)
            return {"code": error.code, "message": text}
        raise


if __name__ == "__main__":
    main()
