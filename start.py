#!/usr/bin/env python3
"""
Smart Agent Router 一键启动器
用法: python3 start.py
"""
import os, sys, re, json, time, stat, subprocess, urllib.request, urllib.error
from pathlib import Path

PROJ        = Path(__file__).parent.resolve()
HOME        = Path.home()
VENV_PYTHON = PROJ / ".venv" / "bin" / "python"
CLOUDFLARED = PROJ / "cloudflared"
CF_CONFIG   = HOME / ".cloudflared" / "config.yml"
PORT        = 8787
API_KEY     = "sk-cursor-proxy-key"

# ─────────────────────── helpers ───────────────────────────────

def log(msg, end="\n"):
    print(msg, end=end, flush=True)

def http_get(url, headers=None, timeout=5):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return None, str(e)

def http_post(url, payload, headers=None, timeout=30):
    data = json.dumps(payload).encode()
    h = {"Content-Type": "application/json"}
    h.update(headers or {})
    req = urllib.request.Request(url, data=data, headers=h)
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return None, str(e)

def load_env():
    env = os.environ.copy()
    env_file = PROJ / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip("'\"")
    return env

# ─────────────────────── step 1: venv ──────────────────────────

def setup_venv():
    log("📦 检查 Python 环境...", end=" ")
    if not (PROJ / ".venv").exists():
        subprocess.run([sys.executable, "-m", "venv", str(PROJ / ".venv")],
                       check=True, capture_output=True)
    subprocess.run(
        [str(PROJ / ".venv/bin/pip"), "install", "-q", "-r",
         str(PROJ / "requirements.txt")],
        check=True, capture_output=True
    )
    log("✅")

# ─────────────────────── step 2: router ────────────────────────

def start_router():
    log("🚀 启动 Smart Agent Router (port 8787)...", end=" ")
    status, _ = http_get(f"http://localhost:{PORT}/health")
    if status == 200:
        log("✅ 已在运行")
        return True

    env = load_env()
    env.update({
        "PORT": str(PORT),
        "PROXY_HOST": "0.0.0.0",
        "PROXY_API_KEY": API_KEY,
        "HOME": str(HOME),
        "PATH": f"{PROJ}/.venv/bin:{env.get('PATH', '/usr/bin:/bin')}",
    })

    log_file = open(PROJ / "server.log", "a")
    proc = subprocess.Popen(
        [str(VENV_PYTHON), str(PROJ / "server.py")],
        cwd=str(PROJ), env=env,
        stdout=log_file, stderr=log_file,
        start_new_session=True,
    )
    (PROJ / "router.pid").write_text(str(proc.pid))

    log(f"(PID={proc.pid})", end=" ")
    for _ in range(20):
        time.sleep(1)
        print(".", end="", flush=True)
        status, _ = http_get(f"http://localhost:{PORT}/health")
        if status == 200:
            log(" ✅")
            return True

    log(f"\n❌ 启动超时！查看: tail -50 {PROJ}/server.log")
    return False

# ─────────────────────── step 3: tunnel ────────────────────────

def start_tunnel():
    if not CLOUDFLARED.exists():
        log("⚠️  cloudflared 二进制不存在，跳过隧道")
        return None

    CLOUDFLARED.chmod(CLOUDFLARED.stat().st_mode | stat.S_IXUSR)

    # Already running?
    r = subprocess.run(["pgrep", "-fa", "cloudflared"], capture_output=True, text=True)
    for line in r.stdout.splitlines():
        if "tunnel" in line and "run" in line:
            pid = line.split()[0]
            log(f"🌐 Cloudflare 隧道已在运行 (PID={pid}) ✅")
            return "https://llm.highgogo.uk" if CF_CONFIG.exists() else None

    log_file = open(PROJ / "tunnel.log", "w")

    if CF_CONFIG.exists():
        # ── Named tunnel → fixed domain ──
        log("🌐 启动命名隧道 → https://llm.highgogo.uk ...", end=" ")
        proc = subprocess.Popen(
            [str(CLOUDFLARED), "tunnel", "--config", str(CF_CONFIG), "run"],
            stdout=log_file, stderr=log_file,
            start_new_session=True,
        )
        (PROJ / "tunnel.pid").write_text(str(proc.pid))
        time.sleep(5)
        if proc.poll() is None:
            log(f"✅ (PID={proc.pid})")
            return "https://llm.highgogo.uk"
        log(f"\n❌ 启动失败，查看: tail -30 {PROJ}/tunnel.log")
        return None
    else:
        # ── Temp tunnel → random URL ──
        log("🌐 启动临时隧道 (获取公网 URL)...", end=" ")
        proc = subprocess.Popen(
            [str(CLOUDFLARED), "tunnel", "--url", f"http://localhost:{PORT}"],
            stdout=log_file, stderr=log_file,
            start_new_session=True,
        )
        (PROJ / "tunnel.pid").write_text(str(proc.pid))
        for _ in range(25):
            time.sleep(1)
            print(".", end="", flush=True)
            content = (PROJ / "tunnel.log").read_text()
            m = re.search(r'https://[a-z0-9-]+\.trycloudflare\.com', content)
            if m:
                log(f" ✅")
                return m.group()
        log(f"\n❌ 获取 URL 超时，查看: tail -30 {PROJ}/tunnel.log")
        return None

# ─────────────────────── step 4: test all models ───────────────

def test_model():
    log("\n🧪 测试所有模型（仅本地 Antigravity，通过 localhost）...")

    # 从本地接口获取模型列表
    status, body = http_get(
        f"http://localhost:{PORT}/v1/models",
        headers={"Authorization": f"Bearer {API_KEY}"}
    )
    if status != 200:
        log(f"   ❌ /v1/models 返回 HTTP {status}")
        return False

    try:
        all_models = [m["id"] for m in json.loads(body).get("data", [])]
    except Exception:
        all_models = []

    if not all_models:
        log("   ❌ 没有可用模型")
        return False

    # 只测试 Antigravity 本地模型（排除来自第三方代理的 claude-3-x、gpt- 等）
    external_prefixes = ("claude-3", "gpt-", "o1-", "o3-", "deepseek-")
    local_models = [
        m for m in all_models
        if not any(m.startswith(p) for p in external_prefixes)
    ]
    external_models = [m for m in all_models if m not in local_models]

    log(f"   本地模型 {len(local_models)} 个 / 外部代理模型 {len(external_models)} 个（不测试）")
    log(f"   逐一测试本地模型...\n")

    results = []
    for model_id in local_models:
        print(f"   {'─'*52}")
        print(f"   🔍 {model_id}", end=" ... ", flush=True)

        status, body = http_post(
            f"http://localhost:{PORT}/v1/chat/completions",
            payload={
                "model": model_id,
                "messages": [{"role": "user", "content": "Reply with exactly one word: OK"}],
                "max_tokens": 10,
            },
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=20,
        )

        if status == 200:
            try:
                reply = json.loads(body)["choices"][0]["message"]["content"].strip()
                print(f"✅  回复: {reply!r}")
                results.append((model_id, True, reply))
            except Exception as e:
                print(f"✅  (响应解析异常: {e})")
                results.append((model_id, True, "?"))
        else:
            try:
                err = json.loads(body).get("error", {}).get("message", body)[:80]
            except Exception:
                err = body[:80]
            print(f"❌  HTTP {status}: {err}")
            results.append((model_id, False, err))

    # 汇总
    ok  = [r for r in results if r[1]]
    bad = [r for r in results if not r[1]]

    print(f"\n   {'═'*52}")
    print(f"   📊 本地模型测试: ✅ {len(ok)} 个通过 / ❌ {len(bad)} 个失败")
    print(f"   {'═'*52}")
    if ok:
        print("   ✅ 可用:")
        for model_id, _, _ in ok:
            print(f"      • {model_id}")
    if bad:
        print("   ❌ 不可用:")
        for model_id, _, err in bad:
            print(f"      • {model_id}  ({err})")
    if external_models:
        print(f"\n   ℹ️  外部代理模型 (未测试): {', '.join(external_models[:5])}{'...' if len(external_models)>5 else ''}")

    return len(ok) > 0

# ─────────────────────── main ──────────────────────────────────

def self_cleanup():
    """首次运行时删除所有多余脚本"""
    obsolete = [
        "start.sh", "start_all.sh", "start_named_tunnel.sh",
        "start_tunnel.sh", "setup_named_tunnel.sh", "bypass_sandbox.py",
        "diagnose.sh", "install_services.sh", "check_status.py", "test.sh",
    ]
    removed = []
    for name in obsolete:
        f = PROJ / name
        if f.exists():
            f.unlink()
            removed.append(name)
    if removed:
        print(f"🧹 已清理旧脚本: {', '.join(removed)}")

    # 确保自身可执行
    me = PROJ / "start.py"
    me.chmod(me.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def main():
    print("=" * 55)
    print("🚀 Smart Agent Router 启动器")
    print("=" * 55)
    self_cleanup()

    setup_venv()
    if not start_router():
        sys.exit(1)

    tunnel_url = start_tunnel()
    model_ok   = test_model()

    print()
    print("=" * 55)
    print("📋 Cursor / Claude Code 配置:")
    print()
    if tunnel_url:
        print(f"   Base URL  :  {tunnel_url}/v1   ← 推荐（公网固定）")
    print(f"   本地 URL  :  http://localhost:{PORT}/v1")
    print(f"   API Key   :  {API_KEY}")
    print()
    print(f"   模型测试  :  {'✅ 通过' if model_ok else '❌ 失败（查看 server.log）'}")
    print()
    print(f"   日志      :  tail -f {PROJ}/server.log")
    print(f"              tail -f {PROJ}/tunnel.log")
    print("=" * 55)

if __name__ == "__main__":
    main()
