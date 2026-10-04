#!/usr/bin/env python3
"""复供确认并发示例：两个调度员并发确认不同（或同一）待修复事件。

用法：
    python3 examples/restore_concurrency_demo.py [base_url]

默认 base_url 为 http://localhost:8000。

背景
----
旧实现复供确认“先锁各自事件行、再锁管网版本行、最后锁其余事件行”，两个调度员
基于同一版本并发确认不同事件时会形成锁环，请求被数据库判为死锁并向客户端报
500；与整网重导（先锁版本行再清空事件表）交错时锁序也相反。

修复后所有写路径统一锁序：**先锁 network_meta 版本行，再按事件编号锁事件行**。
并发写入在版本行上串行化，按**最终串行状态**判定：

  - 两个原本均安全的**同版**确认：至多一个 200 成功；后到者锁内读到的版本已被
    推进，得到 409 VERSION_CONFLICT（携带 current_version），刷新版本后可重试；
  - 并发确认**同一事件**：一个 200，另一个 409（版本过期）；其后串行重复确认
    得到 409 EVENT_ALREADY_RESTORED；
  - 与**整网重导**交错：重导先提交则事件已被清空 -> 404 EVENT_NOT_FOUND；
    复供先提交则 200（随后重导把版本重置为 1）；绝不死锁、绝不 500；
  - 不安全的确认 -> 422 RESTORE_UNSAFE，不重开任何阀门、不推进版本、不留快照。

本脚本如何验证
--------------
1. 导入一张“两条相互独立故障分支”的管网（故障 R1/R2，分别关 P2/P3 隔离），
   在版本 3 下复供事件 1、事件 2 **各自都安全**（分支互不共用阀门）；
2. 两个线程越过同一道 Barrier 后，同时用版本 3 分别确认事件 1、事件 2：
   断言恰好一个 200、一个 409 VERSION_CONFLICT，无 5xx；版本恰好 +1、只多一条
   快照、恰好一个阀门重开；
3. 负方按响应里的 current_version 刷新后重试：成功，版本再 +1（最终一致）；
4. 两线程同时确认**同一事件 1**：一个 200、一个 409，随后串行重复确认 ->
   409 EVENT_ALREADY_RESTORED；
5. 复供与**整网重导**并发：多轮交错，断言不出现 5xx；重导先提交时复供得到
   404 EVENT_NOT_FOUND，复供先提交时 200；最终状态恒以重导为准（版本 1、
   事件清空、阀门全部开放）；
6. 叠加故障管网（外层事件复供不安全）：并发确认两侧都必须是
   422 RESTORE_UNSAFE，阀位/版本/事件/快照均不变。

脚本只依赖标准库；需先启动服务（docker compose up --build 或本地 uvicorn）。
"""
import json
import os
import sys
import threading
import urllib.error
import urllib.request

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
    "BASE_URL", "http://localhost:8000"
)

# 两条相互独立的故障分支：
#   S --P1[阀]-- A --P2[阀]-- B --R1(无阀,故障)-- U1
#                    \-- P3[阀]-- C --R2(无阀,故障)-- U2
BRANCH_NETWORK = {
    "nodes": [
        {"id": "S", "type": "source"},
        {"id": "A", "type": "junction"},
        {"id": "B", "type": "junction"},
        {"id": "C", "type": "junction"},
        {"id": "U1", "type": "sink"},
        {"id": "U2", "type": "sink"},
    ],
    "pipes": [
        {"id": "P1", "from": "S", "to": "A", "has_valve": True},
        {"id": "P2", "from": "A", "to": "B", "has_valve": True},
        {"id": "R1", "from": "B", "to": "U1", "has_valve": False},
        {"id": "P3", "from": "A", "to": "C", "has_valve": True},
        {"id": "R2", "from": "C", "to": "U2", "has_valve": False},
    ],
}

# 叠加故障管网（复供外层事件会使仍待修复的区内故障端点重新进水，不安全）：
# 与 examples/restore_network.json 同构，这里内联以便脚本自包含。
NESTED_NETWORK = {
    "nodes": [
        {"id": n, "type": "source" if n == "S" else ("sink" if n[0] == "U" else "junction")}
        for n in ("S", "A", "B", "D", "E", "U1", "U2", "U3", "U4")
    ],
    "pipes": [
        {"id": "P1", "from": "S", "to": "A", "has_valve": True},
        {"id": "P2", "from": "A", "to": "B", "has_valve": True},
        {"id": "P3", "from": "A", "to": "U1", "has_valve": True},
        {"id": "P5", "from": "B", "to": "U2", "has_valve": True},
        {"id": "P4", "from": "B", "to": "D", "has_valve": True},
        {"id": "P6", "from": "D", "to": "U3", "has_valve": False},
        {"id": "P7", "from": "D", "to": "E", "has_valve": False},
        {"id": "P8", "from": "E", "to": "U4", "has_valve": True},
    ],
}


def call(method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def get_version() -> int:
    status, body = call("GET", "/api/network")
    assert status == 200, body
    return body["version"]


def list_open_events() -> list[dict]:
    status, body = call("GET", "/api/isolation/events")
    assert status == 200, body
    return [e for e in body if e["status"] == "open"]


def setup_two_independent_faults() -> tuple[int, int]:
    """导入分支管网并制造两条待修复事件：v1 -> 隔离 R1(关P2) v2 -> 隔离 R2(关P3) v3。

    返回 (事件1编号, 事件2编号)。整网重导清空事件但不复用 BIGSERIAL 编号，
    故编号必须取提交响应而非假定为 1/2。
    """
    status, body = call("POST", "/api/network/import", BRANCH_NETWORK)
    assert status == 200 and body["version"] == 1, body
    event_ids = []
    for version, fault, valve in ((1, "R1", "P2"), (2, "R2", "P3")):
        status, body = call(
            "POST",
            "/api/isolation/submit",
            {"fault_pipe_id": fault, "close_pipe_ids": [valve], "version": version},
        )
        assert status == 200, body
        event_ids.append(body["event_id"])
    assert get_version() == 3
    # 前置条件：两个复供在版本 3 下预演都安全
    for eid in event_ids:
        status, body = call(
            "POST", f"/api/isolation/events/{eid}/restore/preview", {"version": 3}
        )
        assert status == 200 and body["safe"], body
    return event_ids[0], event_ids[1]


def run_concurrently(jobs):
    """所有任务越过同一 Barrier 同时发起，返回 [(name, status, body), ...]。"""
    barrier = threading.Barrier(len(jobs))
    outcomes = []
    lock = threading.Lock()

    def worker(name, fn):
        barrier.wait(timeout=10)
        status, body = fn()
        with lock:
            outcomes.append((name, status, body))

    threads = [threading.Thread(target=worker, args=(n, f)) for n, f in jobs]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        if t.is_alive():
            raise RuntimeError("并发请求线程超时未结束（疑似死锁）")
    return outcomes


def restore(eid: int, version: int):
    return lambda: call(
        "POST", f"/api/isolation/events/{eid}/restore", {"version": version}
    )


def reimport():
    return lambda: call("POST", "/api/network/import", BRANCH_NETWORK)


def scenario_two_different_events():
    print("1. 两个调度员基于同一版本 3，并发确认不同事件（事件 1 / 事件 2）...")
    e1, e2 = setup_two_independent_faults()

    outcomes = run_concurrently([
        (f"调度员A: 复供事件{e1}", restore(e1, 3)),
        (f"调度员B: 复供事件{e2}", restore(e2, 3)),
    ])
    winners = [o for o in outcomes if o[1] == 200]
    losers = [o for o in outcomes if o[1] == 409]
    assert len(winners) == 1, outcomes
    assert len(losers) == 1, outcomes
    assert losers[0][2]["error"]["code"] == "VERSION_CONFLICT", outcomes
    assert all(o[1] != 500 for o in outcomes), outcomes

    winner = winners[0]
    loser = losers[0]
    print(f"   赢家 {winner[0]}: 200 -> 版本 {winner[2]['version']}，"
          f"重开 {winner[2]['reopened_pipe_ids']}，快照 {winner[2]['snapshot_id']}")
    print(f"   负方 {loser[0]}: 409 VERSION_CONFLICT，"
          f"current_version={loser[2]['error'].get('current_version')}")

    # 最终串行状态：版本恰好推进一次
    assert get_version() == 4
    open_events = list_open_events()
    assert len(open_events) == 1, open_events

    # 负方不盲目重试：先按 current_version / 重新读网，再预演确认安全后确认
    current = get_version()
    eid = open_events[0]["id"]
    status, body = call(
        "POST", f"/api/isolation/events/{eid}/restore/preview", {"version": current}
    )
    assert status == 200 and body["safe"], body
    status, body = call(
        "POST", f"/api/isolation/events/{eid}/restore", {"version": current}
    )
    assert status == 200, body
    assert get_version() == 5
    print(f"   负方刷新到版本 {current} 后重试事件 {eid}: 200 -> 版本 5，"
          f"最终两个事件均 restored，无 5xx、无部分变更。")


def scenario_same_event():
    print("\n2. 两个调度员基于同一版本 3，并发确认**同一事件** ...")
    e1, _e2 = setup_two_independent_faults()
    outcomes = run_concurrently([
        (f"调度员A: 复供事件{e1}", restore(e1, 3)),
        (f"调度员B: 复供事件{e1}", restore(e1, 3)),
    ])
    assert sorted(o[1] for o in outcomes) == [200, 409], outcomes
    assert all(o[1] != 500 for o in outcomes)
    for name, status, body in outcomes:
        code = body.get("error", {}).get("code") if status != 200 else "OK"
        print(f"   {name}: {status} {code}")

    # 版本只推进一次；另一事件仍 open（未被连带影响）
    assert get_version() == 4
    assert [e["id"] for e in list_open_events()] == [_e2]

    # 串行重复确认已复供事件 -> 409 EVENT_ALREADY_RESTORED
    status, body = call("POST", f"/api/isolation/events/{e1}/restore", {"version": 4})
    assert status == 409 and body["error"]["code"] == "EVENT_ALREADY_RESTORED", body
    print(f"   随后串行重复确认事件 {e1}: 409 EVENT_ALREADY_RESTORED（版本仍为 4）。")


def scenario_restore_vs_reimport():
    print("\n3. 复供确认与整网重导并发（旧锁序相反会死锁），连续 12 轮交错 ...")
    seen = set()
    for round_no in range(1, 13):
        e1, _e2 = setup_two_independent_faults()
        outcomes = run_concurrently([
            (f"复供事件{e1}", restore(e1, 3)),
            ("整网重导", reimport()),
        ])
        assert all(s != 500 for _, s, _ in outcomes), outcomes
        imp = next(o for o in outcomes if o[0] == "整网重导")
        rst = next(o for o in outcomes if o[0].startswith("复供事件"))
        assert imp[1] == 200, outcomes

        if rst[1] == 404:
            assert rst[2]["error"]["code"] == "EVENT_NOT_FOUND"
            seen.add("import-first(404)")
        elif rst[1] == 200:
            seen.add("restore-first(200)")
        else:  # pragma: no cover - 不应出现
            raise AssertionError(f"第 {round_no} 轮意外结局：{outcomes}")

        # 最终状态恒以整网重导为准：版本 1、事件清空、阀门全部开放
        assert get_version() == 1
        assert list_open_events() == []
        status, net = call("GET", "/api/network")
        valves = {p["id"]: p["valve_closed"] for p in net["pipes"] if p["valve_closed"] is not None}
        assert not any(valves.values()), valves

    print(f"   12 轮交错观测到的结局：{sorted(seen)}")
    print("   无死锁、无 5xx；重导先提交 -> 404 EVENT_NOT_FOUND；"
          "复供先提交 -> 200；最终状态恒为重导后的干净管网。")


def scenario_unsafe():
    print("\n4. 叠加故障下并发确认不安全的外层事件：两侧都必须 422，不留任何变更 ...")
    status, body = call("POST", "/api/network/import", NESTED_NETWORK)
    assert status == 200, body
    status, body = call(
        "POST", "/api/isolation/submit",
        {"fault_pipe_id": "P6", "close_pipe_ids": ["P4"], "version": 1},
    )
    assert status == 200, body
    outer_event = body["event_id"]
    status, body = call(
        "POST", "/api/isolation/submit",
        {"fault_pipe_id": "P7", "close_pipe_ids": [], "version": 2},
    )
    assert status == 200, body
    assert get_version() == 3

    outcomes = run_concurrently([
        (f"调度员A: 复供事件{outer_event}", restore(outer_event, 3)),
        (f"调度员B: 复供事件{outer_event}", restore(outer_event, 3)),
    ])
    assert all(s == 422 for _, s, _ in outcomes), outcomes
    for name, status, body in outcomes:
        reasons = [b["code"] for b in body["error"].get("issues", [])]
        assert reasons == ["FAULT_REWATER_RISK"], body
        print(f"   {name}: 422 RESTORE_UNSAFE（{reasons}）")

    assert get_version() == 3
    status, net = call("GET", "/api/network")
    assert next(p for p in net["pipes"] if p["id"] == "P4")["valve_closed"] is True
    assert len(list_open_events()) == 2
    print("   版本仍为 3、P4 仍关闭、两条事件仍 open、无新增快照：失败无部分变更。")


def main() -> None:
    print(f"目标服务：{BASE_URL}\n")
    scenario_two_different_events()
    scenario_same_event()
    scenario_restore_vs_reimport()
    scenario_unsafe()
    print(
        "\n全部并发场景通过：复供确认按统一锁序（版本行 -> 事件行）串行化，"
        "同版并发至多一个成功（另一个 409 VERSION_CONFLICT），重导交错 404/200，"
        "已复供 409，不安全 422；无死锁/500、无部分重开阀门、无多余版本与快照。"
    )


if __name__ == "__main__":
    main()
