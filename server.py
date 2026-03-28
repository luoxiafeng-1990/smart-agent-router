#!/usr/bin/env python3
"""
Antigravity → OpenAI-compatible reverse proxy for Cursor
v5.0 - Full tool/function-calling support via prompt engineering

架构：
  Cursor (OpenAI format) → 本代理 → Antigravity language server → 模型

核心策略：
  language server 只支持纯文本 prompt/response，
  所以我们把 tools 定义嵌入 prompt 让模型学会调用，
  再从模型输出中解析 <tool_call> 标签转回 OpenAI 格式。
"""
import json
import logging
import os
import re
import subprocess
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager
import asyncio

from dotenv import load_dotenv
import dotenv
load_dotenv()

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse

# ─────────────────────────────── 代理跳过 ───────────────────────────────
# 移除所有代理相关环境变量，防止 httpx 误将本地 127.0.0.1 流量发往梯子导致超时或重置
for _key in ["http_proxy", "https_proxy", "all_proxy", "no_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"]:
    os.environ.pop(_key, None)

# ─────────────────────────────── 配置 ───────────────────────────────
PROXY_PORT    = int(os.environ.get("PORT", "8787"))
PROXY_HOST    = os.environ.get("PROXY_HOST", "0.0.0.0")
PROXY_API_KEY = os.environ.get("PROXY_API_KEY", "sk-cursor-proxy-key")
LOG_DIR       = Path(os.environ.get("LOG_DIR", "./logs"))
LOG_REQUESTS  = os.environ.get("LOG_REQUESTS", "true").lower() == "true"
today         = datetime.now().strftime("%Y-%m-%d")

# ================================
# 第三方代理配置 (作为 AI 路由中枢)
# ================================
EXTERNAL_ANTHROPIC_BASE_URL = os.environ.get("EXTERNAL_ANTHROPIC_BASE_URL", "https://api.anthropic.com").rstrip("/")
EXTERNAL_ANTHROPIC_API_KEY = os.environ.get("EXTERNAL_ANTHROPIC_API_KEY", "")

EXTERNAL_DEEPSEEK_BASE_URL = os.environ.get("EXTERNAL_DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/")
EXTERNAL_DEEPSEEK_API_KEY = os.environ.get("EXTERNAL_DEEPSEEK_API_KEY", "")

def _use_external_proxy(model_id: str) -> bool:
    """判断当前请求是否需要转发至第三方（而不是走本地免费的 Antigravity）"""
    normalized = model_id.lower().strip()
    
    is_deepseek = "deepseek" in normalized
    if is_deepseek and EXTERNAL_DEEPSEEK_API_KEY:
        return True
        
    if not is_deepseek and not EXTERNAL_ANTHROPIC_API_KEY:
        return False
        
    normalized = model_id.lower().strip()
    
    # 1. 带有 Antigravity 本源特征的，绝对走本地
    if "4.6" in normalized or "antigravity" in normalized or "gemini" in normalized:
        return False
        
    # 2. 外部主流商用标准的，绝对走外部
    external_prefixes = ("claude-3", "gpt-", "o1-", "o3-", "deepseek-")
    if any(normalized.startswith(p) for p in external_prefixes):
        return True
        
    # 3. 检查是否精确存在于本地可用模型缓存中
    try:
        models = _fetch_models()
        if normalized in models:
            return False
    except Exception:
        pass
        
    # 兜底：如果用户只写了 claude，那就送给外面
    return "claude" in normalized

LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / f"proxy-{today}.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("proxy")

_req_fh = logging.FileHandler(LOG_DIR / f"requests-{today}.jsonl", encoding="utf-8")
_req_fh.setFormatter(logging.Formatter("%(message)s"))
req_logger = logging.getLogger("req_raw")
req_logger.addHandler(_req_fh)
req_logger.propagate = False


def jlog(req_id: str, direction: str, data):
    if LOG_REQUESTS:
        req_logger.info(json.dumps(
            {"req_id": req_id, "ts": datetime.now().isoformat(), "dir": direction, "data": data},
            ensure_ascii=False, default=str,
        ))


# ─────────────────────── Language Server 发现 ───────────────────────
_ls_port: Optional[int] = None
_ls_csrf: Optional[str] = None
_model_cache: dict = {}
_model_cache_ts: float = 0


async def _discover_ls() -> tuple[int, str]:
    """扫描进程表，找到 Antigravity language server 的端口和 CSRF token。"""
    global _ls_port, _ls_csrf
    if _ls_port and _ls_csrf:
        return _ls_port, _ls_csrf

    cmdline = await asyncio.to_thread(subprocess.check_output, ["ps", "aux"], text=True)
    for line in cmdline.splitlines():
        if "language_server_linux_x64" not in line or "--csrf_token" not in line:
            continue
        m = re.search(r"--csrf_token\s+(\S+)", line)
        if not m:
            continue
        csrf = m.group(1)
        pid = int(line.split()[1])
        port = await _find_http_port(pid, csrf)
        if port:
            _ls_port = port
            _ls_csrf = csrf
            logger.info(f"✅ 发现 language server: port={port} csrf={csrf[:8]}...")
            return port, csrf

    raise RuntimeError(
        "找不到 Antigravity language server。请确认 Antigravity 正在运行，且你已登录会员账号。"
    )


def _get_pid_ports(pid: int) -> list[int]:
    """获取指定 PID 的所有监听端口，优先使用 lsof 以避免 ss 输出截断。"""
    ports = set()
    try:
        out = subprocess.check_output(["lsof", "-iTCP", "-sTCP:LISTEN", "-P", "-n"], text=True)
        for line in out.splitlines():
            if str(pid) in line.split():
                m = re.search(r":(\d+)\s+\(LISTEN\)", line)
                if m:
                    ports.add(int(m.group(1)))
        if ports:
            return sorted(list(ports))
    except Exception:
        pass

    try:
        out = subprocess.check_output(["ss", "-tlnp"], text=True)
        out_flat = out.replace("\n", " ")
        for m in re.finditer(rf":(\d+)\s+(?:[^\s]+\s+)*?users:\(\([^)]*?pid={pid},", out_flat):
            ports.add(int(m.group(1)))
    except Exception:
        pass
    return sorted(list(ports))


async def _find_http_port(pid: int, csrf: str) -> Optional[int]:
    """尝试各端口，找到能响应 Heartbeat 的 HTTP 端口。"""
    headers = {
        "Content-Type": "application/json",
        "x-codeium-csrf-token": csrf,
        "Connect-Protocol-Version": "1",
    }
    ports = await asyncio.to_thread(_get_pid_ports, pid)
    async with httpx.AsyncClient() as client:
        for port in ports:
            try:
                r = await client.post(
                    f"http://127.0.0.1:{port}/exa.language_server_pb.LanguageServerService/Heartbeat",
                    json={}, headers=headers, timeout=2,
                )
                if r.status_code == 200:
                    return port
            except Exception:
                continue
    return None


def _reset_ls():
    global _ls_port, _ls_csrf, _model_cache_ts
    _ls_port = None
    _ls_csrf = None
    _model_cache_ts = 0


async def _call_ls(method: str, body: dict, timeout: float = 120) -> dict:
    """向 language server 发起 RPC 调用。"""
    port, csrf = await _discover_ls()
    url = f"http://127.0.0.1:{port}/exa.language_server_pb.LanguageServerService/{method}"
    headers = {
        "Content-Type": "application/json",
        "x-codeium-csrf-token": csrf,
        "Connect-Protocol-Version": "1",
    }
    client = _http_client or httpx.AsyncClient()
    try:
        r = await client.post(url, json=body, headers=headers, timeout=timeout)
    except httpx.ConnectError:
        _reset_ls()
        raise RuntimeError("Antigravity language server 连接断开，请检查 Antigravity 是否运行")

    if r.status_code == 404:
        raise RuntimeError(f"方法 {method} 不存在")
    data = r.json()
    if "code" in data and data["code"] != "ok":
        raise RuntimeError(data.get("message", str(data)))
    return data


# ─────────────────────────── 模型管理 ───────────────────────────────
def _label_to_id(label: str) -> str:
    return label.lower().replace(" ", "-").replace("(", "").replace(")", "").strip("-")

async def _fetch_models() -> dict:
    """动态从 language server 获取可用模型列表，缓存 60 秒。"""
    global _model_cache, _model_cache_ts
    if time.time() - _model_cache_ts < 60 and _model_cache:
        return _model_cache

    models = {}
    for rpc in ["GetCascadeModelConfigData", "GetCommandModelConfigs"]:
        try:
            data = await _call_ls(rpc, {})
        except Exception as e:
            logger.warning(f"获取模型列表失败 ({rpc}): {e}")
            continue
        for cfg in data.get("clientModelConfigs", []):
            label = cfg.get("label", "")
            key = (
                cfg.get("modelOrAlias", {}).get("model")
                or cfg.get("modelOrAlias", {}).get("alias")
                or ""
            )
            if not key:
                continue
            model_id = _label_to_id(label)
            if model_id not in models:
                models[model_id] = {"internal_key": key, "label": label}

    # 额外的别名方便用户使用
    _ALIAS = {
        "claude-opus-4.6":           "claude-opus-4.6-thinking",
        "claude-sonnet-4.6":         "claude-sonnet-4.6-thinking",
        "antigravity-gemini-flash":  "gemini-3-flash",
        "antigravity-gemini-pro":    "gemini-3.1-pro-low",
        "antigravity-gemini-pro-high": "gemini-3.1-pro-high",
        "antigravity-gpt-oss":       "gpt-oss-120b-medium",
    }
    for alias, target in _ALIAS.items():
        if target in models and alias not in models:
            models[alias] = models[target]

    _model_cache = models
    _model_cache_ts = time.time()
    return models


async def _resolve_model(model_id: str) -> str:
    """将 Cursor 传来的模型名解析为 language server 内部 key。"""
    models = await _fetch_models()
    normalized = model_id.lower().strip()

    if normalized in models:
        return models[normalized]["internal_key"]

    # 增强版模式匹配：只要求对方名字包含核心关键词即可
    for mid, info in models.items():
        if "gemini" in normalized and "gemini" in info["label"].lower():
            return info["internal_key"]
        if "claude" in normalized and "claude" in info["label"].lower():
            return info["internal_key"]
            
    # 如果实在匹配不到对应的厂牌，尝试降级到最基本的模糊名称
    for mid, info in models.items():
        if normalized in mid or normalized in info["label"].lower():
            return info["internal_key"]
            
    # 默认第一个
    if models:
        first = next(iter(models.values()))
        logger.warning(f"模型 '{model_id}' 均未能精确匹配，随机使用默认存活: {first['label']}")
        return first["internal_key"]
        
    raise ValueError(f"无法解析模型 '{model_id}'，Language Server 内部模型表均为空")


# ───────────────────── Prompt 构建（核心改造） ──────────────────────
_TOOL_CALL_SYSTEM = """
## 工具调用规则

当你需要使用工具时，在回复中写入以下格式（每个工具调用单独一个块）：
<tool_call>
{"name": "工具名称", "arguments": {"参数名": "参数值"}}
</tool_call>

调用工具后立即停止，不要猜测工具的返回值，等待实际结果。
可以在一次回复中调用多个工具（每个写一个 <tool_call> 块）。
如果不需要调用任何工具，直接正常回复即可。
""".strip()


def _tools_to_text(tools: list[dict]) -> str:
    """将 OpenAI tools 数组转成自然语言描述，嵌入 system prompt。"""
    if not tools:
        return ""

    lines = ["\n## 可用工具\n"]
    for tool in tools:
        fn = tool.get("function") or tool  # 兼容有无 function 包装
        name = fn.get("name", "")
        desc = fn.get("description", "")
        params = fn.get("parameters", {})
        props = params.get("properties", {})
        required = params.get("required", [])

        lines.append(f"### {name}")
        if desc:
            lines.append(desc)
        if props:
            lines.append("参数：")
            for pname, pinfo in props.items():
                ptype = pinfo.get("type", "any")
                pdesc = pinfo.get("description", "")
                req_mark = "（必填）" if pname in required else "（选填）"
                lines.append(f"  - `{pname}` ({ptype}){req_mark}: {pdesc}")
        lines.append("")

    return "\n".join(lines)


def _tool_calls_to_text(tool_calls: list[dict]) -> str:
    """将 assistant 消息中的 tool_calls 转成文字（对话历史回放用）。"""
    parts = []
    for tc in tool_calls:
        fn = tc.get("function", {})
        name = fn.get("name", "")
        try:
            args = json.loads(fn.get("arguments", "{}"))
        except Exception:
            args = fn.get("arguments", {})
        parts.append(f'<tool_call>\n{json.dumps({"name": name, "arguments": args}, ensure_ascii=False)}\n</tool_call>')
    return "\n".join(parts)


def _build_prompt(messages: list[dict], tools: list[dict]) -> str:
    """
    将 OpenAI messages + tools 构建成 language server 接受的纯文本 prompt。

    格式：
      [System]
      <system内容>
      <工具描述>
      <工具调用规则>

      [User]
      <用户消息>

      [Tool Result: tool_name(args)]
      <工具执行结果>

      [Assistant]
      <AI回复 或 tool_call块>
    """
    has_tools = bool(tools)

    # 分离 system 消息和对话消息
    system_parts = []
    conv_messages = []
    for msg in messages:
        if msg.get("role") == "system":
            content = msg.get("content", "")
            if isinstance(content, list):
                content = "\n".join(p.get("text", "") for p in content if p.get("type") == "text")
            system_parts.append(content)
        else:
            conv_messages.append(msg)

    # 构建 system 块
    system_text = "\n\n".join(system_parts) if system_parts else "你是一个强大的 AI 编程助手。"
    if has_tools:
        system_text += "\n\n" + _tools_to_text(tools)
        system_text += "\n\n" + _TOOL_CALL_SYSTEM

    parts = [f"[System]\n{system_text}"]

    # 构建对话块
    for msg in conv_messages:
        role = msg.get("role", "user")
        content = msg.get("content") or ""
        tool_calls = msg.get("tool_calls", [])
        tool_call_id = msg.get("tool_call_id")
        name = msg.get("name", "")

        # 提取文本内容（支持 content 为 list 的格式）
        if isinstance(content, list):
            text_parts = []
            for part in content:
                if part.get("type") == "text":
                    text_parts.append(part.get("text", ""))
                elif part.get("type") == "image_url":
                    text_parts.append("[图片]")
            content = "\n".join(text_parts)

        if role == "user":
            parts.append(f"[User]\n{content}")
        elif role == "assistant":
            if tool_calls:
                # 历史中的工具调用：转成 <tool_call> 文本
                tc_text = _tool_calls_to_text(tool_calls)
                combined = (content + "\n" + tc_text).strip() if content else tc_text
                parts.append(f"[Assistant]\n{combined}")
            else:
                parts.append(f"[Assistant]\n{content}")
        elif role == "tool":
            # 工具执行结果
            label = f"[Tool Result: {name or tool_call_id or 'tool'}]"
            parts.append(f"{label}\n{content}")

    return "\n\n".join(parts)


# ─────────────────────── 解析工具调用输出 ───────────────────────────
_TOOL_CALL_RE = re.compile(
    r"<tool_call>\s*(\{.*?\})\s*</tool_call>",
    re.DOTALL,
)


def _parse_tool_calls(text: str) -> tuple[Optional[str], list[dict]]:
    """
    从模型输出中扫描 <tool_call>{...}</tool_call> 块。
    返回：(普通文字内容, tool_calls列表)
    """
    tool_calls = []
    matches = list(_TOOL_CALL_RE.finditer(text))
    if not matches:
        return text, []

    for i, m in enumerate(matches):
        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError:
            logger.warning(f"解析 tool_call JSON 失败: {m.group(1)[:100]}")
            continue
        tool_calls.append({
            "id": f"call_{uuid.uuid4().hex[:10]}",
            "type": "function",
            "function": {
                "name": data.get("name", ""),
                "arguments": json.dumps(data.get("arguments", {}), ensure_ascii=False),
            },
        })

    # 把 tool_call 块从文本中移除，剩余部分作为 content
    clean_text = _TOOL_CALL_RE.sub("", text).strip()
    # 如果有 tool_calls，content 通常为 null（符合 OpenAI 规范）
    content = clean_text if clean_text else None

    return content, tool_calls


# ───────────────────────────── FastAPI App ──────────────────────────
_http_client: Optional[httpx.AsyncClient] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _http_client
    _http_client = httpx.AsyncClient(timeout=120, limits=httpx.Limits(max_keepalive_connections=100))
    yield
    await _http_client.aclose()

app = FastAPI(title="Antigravity Proxy", version="5.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def _check_auth(request: Request) -> bool:
    if not PROXY_API_KEY:
        return True
    auth = request.headers.get("Authorization", "")
    return auth == f"Bearer {PROXY_API_KEY}"


@app.get("/")
async def root():
    try:
        with open("templates/index.html", "r", encoding="utf-8") as f:
            html = f.read()
        return HTMLResponse(content=html)
    except Exception as e:
        return JSONResponse({"status": "error", "detail": "Web Dashboard not found. Create templates/index.html first."}, status_code=500)

@app.get("/api/status")
async def get_status():
    try:
        port, _ = await _discover_ls()
        models = await _fetch_models()
    except Exception:
        port, models = None, {}
    return {
        "service": "Smart Agent Router",
        "ls_port": port,
        "local_models_count": len(models),
        "local_models": list(models.keys()),
        "third_party": {
            "enabled": bool(EXTERNAL_ANTHROPIC_API_KEY),
            "base_url": EXTERNAL_ANTHROPIC_BASE_URL,
            "api_key": "***" + EXTERNAL_ANTHROPIC_API_KEY[-4:] if EXTERNAL_ANTHROPIC_API_KEY else ""
        },
        "deepseek": {
            "enabled": bool(EXTERNAL_DEEPSEEK_API_KEY),
            "base_url": EXTERNAL_DEEPSEEK_BASE_URL,
            "api_key": "***" + EXTERNAL_DEEPSEEK_API_KEY[-4:] if EXTERNAL_DEEPSEEK_API_KEY else ""
        }
    }

@app.post("/api/config/update")
async def update_config(request: Request):
    global EXTERNAL_ANTHROPIC_BASE_URL, EXTERNAL_ANTHROPIC_API_KEY
    global EXTERNAL_DEEPSEEK_BASE_URL, EXTERNAL_DEEPSEEK_API_KEY
    data = await request.json()
    new_url = data.get("base_url", "").strip().rstrip("/")
    new_key = data.get("api_key", "").strip()
    ds_url = data.get("ds_base_url", "").strip().rstrip("/")
    ds_key = data.get("ds_api_key", "").strip()
    
    env_file = ".env"
    if not os.path.exists(env_file):
        with open(env_file, "w") as f: pass
    
    if new_url or new_key:
        if new_url: dotenv.set_key(env_file, "EXTERNAL_ANTHROPIC_BASE_URL", new_url)
        if new_key: dotenv.set_key(env_file, "EXTERNAL_ANTHROPIC_API_KEY", new_key)
        EXTERNAL_ANTHROPIC_BASE_URL = new_url or EXTERNAL_ANTHROPIC_BASE_URL
        EXTERNAL_ANTHROPIC_API_KEY = new_key or EXTERNAL_ANTHROPIC_API_KEY
        
    if ds_url or ds_key:
        if ds_url: dotenv.set_key(env_file, "EXTERNAL_DEEPSEEK_BASE_URL", ds_url)
        if ds_key: dotenv.set_key(env_file, "EXTERNAL_DEEPSEEK_API_KEY", ds_key)
        EXTERNAL_DEEPSEEK_BASE_URL = ds_url or EXTERNAL_DEEPSEEK_BASE_URL
        EXTERNAL_DEEPSEEK_API_KEY = ds_key or EXTERNAL_DEEPSEEK_API_KEY
    
    return {"status": "ok", "message": "代理配置已保存并热生效！"}

@app.post("/api/models/discover")
async def discover_models(request: Request):
    data = await request.json()
    base_url = data.get("base_url", "").strip().rstrip("/")
    api_key = data.get("api_key", "").strip()
    
    if not base_url or not api_key:
        return JSONResponse({"error": "缺少 Base URL 或 API Key"}, status_code=400)
        
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{base_url}/v1/models", headers={"Authorization": f"Bearer {api_key}"})
            if resp.status_code != 200:
                err = resp.text[:100]
                return JSONResponse({"error": f"目标端异常 {resp.status_code}: {err}"}, status_code=502)
                
            remote_models = [m.get("id") for m in resp.json().get("data", [])]
            
            # Smart Probing on top representative Agentic models
            top_models = [m for m in remote_models if "sonnet" in m or "gpt-4o" in m]
            top_models = top_models[:2] if top_models else remote_models[:2]
            
            working_models = []
            for tm in top_models:
                payload = {"model": tm, "messages": [{"role": "user", "content": "1"}], "max_tokens": 2}
                try:
                    probe = await client.post(f"{base_url}/v1/chat/completions", json=payload, headers={"Authorization": f"Bearer {api_key}"}, timeout=8)
                    if probe.status_code == 200:
                        working_models.append(tm)
                except Exception:
                    pass
                    
            return {"status": "ok", "models": remote_models, "probed_success": working_models}
    except Exception as e:
        return JSONResponse({"error": f"检测失败: {str(e)}"}, status_code=500)

@app.get("/health")
async def health():
    try:
        port, _ = await _discover_ls()
        return {"status": "ok", "ls_port": port}
    except Exception as e:
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=503)


@app.get("/v1/models")
async def list_models(request: Request):
    if not _check_auth(request):
        return JSONResponse({"error": {"message": "Unauthorized"}}, status_code=401)
    try:
        models = await _fetch_models()
    except Exception as e:
        return JSONResponse({"error": {"message": str(e)}}, status_code=502)
        
    combined_data = [
        {"id": mid, "object": "model", "created": 1700000000, "owned_by": "antigravity (local)"}
        for mid in models
    ]
    
    if EXTERNAL_ANTHROPIC_API_KEY and EXTERNAL_ANTHROPIC_BASE_URL:
        try:
            async with httpx.AsyncClient(timeout=4) as client:
                r = await client.get(f"{EXTERNAL_ANTHROPIC_BASE_URL}/v1/models", headers={"Authorization": f"Bearer {EXTERNAL_ANTHROPIC_API_KEY}"})
                if r.status_code == 200:
                    ext_data = r.json().get("data", [])
                    for m in ext_data:
                        if not m.get("owned_by"): m["owned_by"] = "third-party proxy"
                        # De-duplicate
                        if not any(cd["id"] == m["id"] for cd in combined_data):
                            combined_data.append(m)
        except Exception as e:
            logger.warning(f"获取第三方模型列表失败: {e}")

    return {
        "object": "list",
        "data": combined_data
    }


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    if not _check_auth(request):
        return JSONResponse({"error": {"message": "Unauthorized"}}, status_code=401)

    req_id = uuid.uuid4().hex[:10]

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": {"message": "Invalid JSON"}}, status_code=400)

    jlog(req_id, "cursor→proxy", body)

    model_id   = body.get("model", "")
    messages   = body.get("messages", [])
    tools      = body.get("tools") or []
    stream     = bool(body.get("stream", False))

    logger.info(
        f"[{req_id}] model={model_id} msgs={len(messages)} "
        f"tools={len(tools)} stream={stream}"
    )

    # =============== 路由分支：如果是第三方模型，走直接转发 ===============
    if _use_external_proxy(model_id):
        logger.info(f"[{req_id}] 🌐 击中第三方模型路由: 转发至外部 OpenAI 兼容节点 ({EXTERNAL_ANTHROPIC_BASE_URL})")
        return await _forward_openai_to_external_openai(req_id, body, stream)
    # =========================================================================

    # 解析模型
    try:
        internal_key = await _resolve_model(model_id)
    except ValueError as e:
        return JSONResponse({"error": {"message": str(e)}}, status_code=400)

    # 构建 prompt
    prompt = _build_prompt(messages, tools)
    jlog(req_id, "proxy→ls", {"model": internal_key, "prompt_chars": len(prompt)})

    # 调用 language server
    try:
        result = await _call_ls("GetModelResponse", {"model": internal_key, "prompt": prompt})
    except RuntimeError as e:
        logger.error(f"[{req_id}] language server 错误: {e}")
        err_msg = f"\n\n🚨 [Antigravity Proxy 网关截获报错]\n底层引擎拒绝服务: {e}\n\n💡 诊断：通常由于 Antigravity 该模型当前并发超载或账号额度被抽空。\n建议：请转到 Web 控制台添加 [第三方代理 API] 并改用外部商业模型！"
        if stream:
            return StreamingResponse(
                _stream_response(f"err-{uuid.uuid4().hex[:8]}", int(time.time()), model_id, err_msg, []),
                media_type="text/event-stream"
            )
        return JSONResponse({"error": {"message": str(e)}}, status_code=502)

    raw_text = result.get("response", "")
    jlog(req_id, "ls→proxy", {"response_chars": len(raw_text), "preview": raw_text[:200]})

    # 解析工具调用
    content, tool_calls = _parse_tool_calls(raw_text)
    finish_reason = "tool_calls" if tool_calls else "stop"

    logger.info(
        f"[{req_id}] ✓ finish={finish_reason} "
        f"tool_calls={len(tool_calls)} content_len={len(content or '')}"
    )

    completion_id = f"chatcmpl-{uuid.uuid4().hex[:12]}"
    created = int(time.time())

    if stream:
        return StreamingResponse(
            _stream_response(completion_id, created, model_id, content, tool_calls),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # 构建 assistant message
    message: dict = {"role": "assistant", "content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls

    return {
        "id": completion_id,
        "object": "chat.completion",
        "created": created,
        "model": model_id,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason,
        }],
        "usage": {
            "prompt_tokens": len(prompt.split()),
            "completion_tokens": len(raw_text.split()),
            "total_tokens": len(prompt.split()) + len(raw_text.split()),
        },
    }


async def _stream_response(
    completion_id: str,
    created: int,
    model_id: str,
    content: Optional[str],
    tool_calls: list[dict],
):
    """生成符合 OpenAI 规范的 SSE 流，正确处理 tool_calls。"""

    def _chunk(delta: dict, finish_reason=None) -> str:
        return "data: " + json.dumps({
            "id": completion_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model_id,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
        }, ensure_ascii=False) + "\n\n"

    # 开始标记
    yield _chunk({"role": "assistant"})

    if tool_calls:
        # 先发 content（如果有）
        if content:
            yield _chunk({"content": content})

        # 逐个发送 tool_calls
        for i, tc in enumerate(tool_calls):
            # 第一个 chunk：初始化 tool_call
            yield _chunk({"tool_calls": [{
                "index": i,
                "id": tc["id"],
                "type": "function",
                "function": {"name": tc["function"]["name"], "arguments": ""},
            }]})
            # 发送 arguments
            yield _chunk({"tool_calls": [{
                "index": i,
                "function": {"arguments": tc["function"]["arguments"]},
            }]})

        yield _chunk({}, finish_reason="tool_calls")
    else:
        # 普通文字流式输出（按词分块）
        words = (content or "").split(" ")
        chunk_size = 6
        for i in range(0, len(words), chunk_size):
            fragment = " ".join(words[i:i + chunk_size])
            if i > 0:
                fragment = " " + fragment
            yield _chunk({"content": fragment})

        yield _chunk({}, finish_reason="stop")

    yield "data: [DONE]\n\n"


# ────────────────────────────────────────────────────────────────────────
# ─────────────────────── 路由枢纽与协议转换扩展区 ───────────────────────
# ────────────────────────────────────────────────────────────────────────

async def _forward_openai_to_external_openai(req_id: str, body: dict, stream: bool):
    """直接转发 OpenAI 格式请求给第三方代理，带破损 Claude 内部 tool tag 拦截修复机制"""
    model_id = body.get("model", "")
    is_ds = "deepseek" in model_id.lower()
    
    base_url = EXTERNAL_DEEPSEEK_BASE_URL if is_ds else EXTERNAL_ANTHROPIC_BASE_URL
    api_key = EXTERNAL_DEEPSEEK_API_KEY if is_ds else EXTERNAL_ANTHROPIC_API_KEY

    url = f"{base_url}/v1/chat/completions"
    forward_headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    model_id = body.get("model", "")
    completion_id = f"chatcmpl-{uuid.uuid4().hex[:12]}"
    created = int(time.time())

    # DeepSeek API 要求 content 严格为 string（不支持多模态数组块），而 Cursor 经常传入 [{"type":"text","text":"..."}]
    if is_ds and "messages" in body:
        for msg in body["messages"]:
            content = msg.get("content")
            if isinstance(content, list):
                text_buffer = []
                for b in content:
                    if isinstance(b, dict) and b.get("type") == "text":
                        text_buffer.append(b.get("text", ""))
                msg["content"] = "\n".join(text_buffer)
                
    async def _stream_proxy():
        client = _http_client or httpx.AsyncClient()
        # To avoid massive reindentation, we artificially scope this with 'if True:' or simply reindent:
        if client: # keep indent level of 'async with'
            try:
                # 记录请求
                logger.debug(f"[{req_id}] POST {url} (model={model_id})")
                async with client.stream("POST", url, json=body, headers=forward_headers) as resp:
                    if resp.status_code != 200:
                        err = await resp.aread()
                        logger.error(f"[{req_id}] Proxy Error: {resp.status_code} {err.decode('utf-8')[:200]}")
                        yield f"data: {json.dumps({'error': resp.status_code, 'message': err.decode('utf-8')})}\n\n"
                        return

                    def _chunk(delta: dict, finish_reason=None):
                        return "data: " + json.dumps({
                            "id": completion_id, "object": "chat.completion.chunk",
                            "created": created, "model": model_id,
                            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
                        }, ensure_ascii=False) + "\n\n"

                    buffer = ""
                    tool_index = -1
                    has_tool_emitted = False

                    async for line in resp.aiter_lines():
                        if not line:
                            continue
                        if line.startswith("data:"):
                            data_str = line[5:].strip()
                            if not data_str or data_str == "[DONE]":
                                if buffer:
                                    yield _chunk({"content": buffer})
                                    buffer = ""
                                yield "data: [DONE]\n\n"
                                continue
                                
                            try:
                                data = json.loads(data_str)
                            except Exception:
                                continue
                                
                            choices = data.get("choices", [])
                            if not choices:
                                continue
                            delta = choices[0].get("delta", {})
                            
                            # 1. 代理原本就正确返回的 tool_calls 走这
                            if "tool_calls" in delta:
                                yield line + "\n\n"
                                continue
                                
                            # 2. 对 content_delta 进行强大容错过滤 (修复 apiclaw 没有拦截原生 tool tag 或自定义 tag 的现象)
                            if "content" in delta and delta["content"]:
                                buffer += delta["content"]
                                
                                # A. 抽取完整的 Tool 块
                                while True:
                                    m1 = re.search(r"<tool_call>\s*(.*?)\s*</tool_call>", buffer, re.DOTALL)
                                    m2 = re.search(r"<\|start\|>assistant.*?<\|message\|>\s*(.*?)\s*<\|call\|>", buffer, re.DOTALL)
                                    
                                    match = None
                                    if m1 and m2: match = m1 if m1.start() < m2.start() else m2
                                    elif m1: match = m1
                                    elif m2: match = m2
                                    
                                    if not match: break
                                        
                                    try:
                                        t_data = json.loads(match.group(1))
                                        t_name = t_data.get("name", "")
                                        t_args = json.dumps(t_data.get("arguments", {}), ensure_ascii=False)
                                        tool_index += 1
                                        has_tool_emitted = True
                                        
                                        # 如果 tool 前面有正文，吐出去
                                        if match.start() > 0:
                                            yield _chunk({"content": buffer[:match.start()]})
                                            
                                        yield _chunk({"tool_calls": [{"index": tool_index, "id": f"call_{uuid.uuid4().hex[:10]}", "type": "function", "function": {"name": t_name, "arguments": ""}}]})
                                        yield _chunk({"tool_calls": [{"index": tool_index, "function": {"arguments": t_args}}]})
                                    except Exception as e:
                                        logger.error(f"Failed to parse regex matched tool tag: {e}")
                                        if match.start() > 0:
                                            yield _chunk({"content": buffer[:match.start()]})
                                        yield _chunk({"content": match.group(0)})
                                        
                                    buffer = buffer[match.end():]

                                # B. 幻觉保护：如果模型生成完 tool_call 没被 proxy 强行打断，开始幻想 tool_result 或对方回复了
                                if "<tool_result>" in buffer or "<|start|>user" in buffer:
                                    # 直接切断这一句话！因为后续都是幻觉
                                    yield _chunk({}, finish_reason="tool_calls" if has_tool_emitted else "stop")
                                    yield "data: [DONE]\n\n"
                                    return

                                # C. 滚动窗口判定：找出不可能成为上述标签头的部分，将其吐出（避免因长标签卡死正常流）
                                safe_point = len(buffer)
                                tag_roots = ["<tool_call>", "<|start|>assistant", "<tool_result>", "<|start|>user"]
                                
                                has_root_inside = False
                                for r in tag_roots:
                                    idx = buffer.find(r)
                                    if idx != -1:
                                        safe_point = min(safe_point, idx)
                                        has_root_inside = True

                                if not has_root_inside:
                                    # 检查是否有任何前缀匹配
                                    for r in tag_roots:
                                        for i in range(len(buffer)):
                                            if r.startswith(buffer[i:]):
                                                safe_point = min(safe_point, i)
                                                break
                                                
                                if safe_point > 0:
                                    yield _chunk({"content": buffer[:safe_point]})
                                    buffer = buffer[safe_point:]
                                    
                            # 3. 转发结束信号
                            finish_reason = choices[0].get("finish_reason")
                            if finish_reason:
                                yield _chunk({}, finish_reason=finish_reason)
            except Exception as e:
                logger.error(f"[{req_id}] 外部流转发中断: {e}")
                yield f"data: {json.dumps({'error': str(e)})}\n\n"

    if stream:
        return StreamingResponse(_stream_proxy(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})
    else:
        # Cursor 的默认是非流式降级？Cursor通常使用流式。(备用降级逻辑)
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(url, json=body, headers=forward_headers)
            return JSONResponse(resp.json(), status_code=resp.status_code)


@app.post("/v1/messages")
async def messages_endpoint(request: Request):
    """
    Claude Code (Anthropic 原生客户端) 接入网关。
    如果配置了第三方外部代理，且模型符合规则，透明转发。
    否则，翻译成内部格式，利用 Antigravity 模型实现“白嫖”。
    """
    if not _check_auth(request):
        return JSONResponse({"error": {"message": "Unauthorized"}}, status_code=401)
        
    req_id = uuid.uuid4().hex[:10]
    
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": {"message": "Invalid JSON"}}, status_code=400)

    model_id = body.get("model", "")
    stream = body.get("stream", False)
    
    # ---------------- 外部请求透明转发 ----------------
    if _use_external_proxy(model_id):
        logger.info(f"[{req_id}] 📡 Claude Code 击中第三方路由: 纯透明转发至外部 Anthropic 节点")
        headers = {
            "x-api-key": EXTERNAL_ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }
        url = f"{EXTERNAL_ANTHROPIC_BASE_URL}/v1/messages"
        
        async def _direct_stream():
            async with httpx.AsyncClient(timeout=120) as client:
                async with client.stream("POST", url, json=body, headers=headers) as resp:
                    if resp.status_code != 200:
                        err = await resp.aread()
                        yield err
                        return
                    async for chunk in resp.aiter_raw():
                        yield chunk
                        
        if stream:
            return StreamingResponse(_direct_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})
        else:
            async with httpx.AsyncClient() as client:
                resp = await client.post(url, json=body, headers=headers, timeout=120)
                return JSONResponse(resp.json(), status_code=resp.status_code)
                
    # ---------------- 白嫖本地模型 (协议逆向转换) ----------------
    logger.info(f"[{req_id}] 💡 Claude Code 使用 Antigravity: 本地处理 {model_id}")
    
    messages = body.get("messages", [])
    system = body.get("system", "")
    req_tools = body.get("tools", [])
    
    # 将 Anthropic 传来的 tools 转为内部统一的 OpenAI tools 格式
    tools = []
    for t in req_tools:
        tools.append({
            "type": "function",
            "function": {
                "name": t.get("name"),
                "description": t.get("description", ""),
                "parameters": t.get("input_schema", {})
            }
        })
        
    mapped_messages = []
    if system:
        mapped_messages.append({"role": "system", "content": system})
        
    for m in messages:
        role = m.get("role")
        content = m.get("content")
        if isinstance(content, str):
            mapped_messages.append({"role": role, "content": content})
        elif isinstance(content, list):
            if role == "user":
                text = "\n".join([c.get("text", "") for c in content if c.get("type") == "text"])
                tool_results = [c for c in content if c.get("type") == "tool_result"]
                if tool_results:
                    for tr in tool_results:
                        mapped_messages.append({
                            "role": "tool",
                            "tool_call_id": tr.get("tool_use_id"),
                            "content": tr.get("content")
                        })
                else:
                    if text:
                        mapped_messages.append({"role": "user", "content": text})
            elif role == "assistant":
                text = "\n".join([c.get("text", "") for c in content if c.get("type") == "text"])
                tool_uses = [c for c in content if c.get("type") == "tool_use"]
                msg = {"role": "assistant"}
                if text:
                    msg["content"] = text
                if tool_uses:
                    msg["tool_calls"] = []
                    for tu in tool_uses:
                        msg["tool_calls"].append({
                            "id": tu.get("id"),
                            "type": "function",
                            "function": {
                                "name": tu.get("name"),
                                "arguments": json.dumps(tu.get("input", {}), ensure_ascii=False)
                            }
                        })
                mapped_messages.append(msg)
                
    try:
        internal_key = _resolve_model(model_id)
        prompt = _build_prompt(mapped_messages, tools)
        jlog(req_id, "proxy→ls(claude-code)", {"model": internal_key, "prompt_chars": len(prompt)})
        
        result = _call_ls("GetModelResponse", {"model": internal_key, "prompt": prompt})
    except Exception as e:
        logger.error(f"[{req_id}] language server 错误: {e}")
        return JSONResponse({"error": {"message": str(e)}}, status_code=502)
        
    raw_text = result.get("response", "")
    content_extracted, tool_calls = _parse_tool_calls(raw_text)
    
    async def _anthropic_sse_stream():
        msg_id = f"msg_{uuid.uuid4().hex[:12]}"
        yield f"event: message_start\ndata: {json.dumps({'type': 'message_start', 'message': {'id': msg_id, 'type': 'message', 'role': 'assistant', 'model': model_id, 'content': []}})}\n\n"
        
        block_idx = 0
        if content_extracted:
            yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': block_idx, 'content_block': {'type': 'text', 'text': ''}})}\n\n"
            words = content_extracted.split(" ")
            chunk_size = 6
            for i in range(0, len(words), chunk_size):
                frag = " ".join(words[i:i+chunk_size])
                if i > 0: frag = " " + frag
                yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': block_idx, 'delta': {'type': 'text_delta', 'text': frag}})}\n\n"
            yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': block_idx})}\n\n"
            block_idx += 1
            
        if tool_calls:
            for tc in tool_calls:
                yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': block_idx, 'content_block': {'type': 'tool_use', 'id': tc['id'], 'name': tc['function']['name'], 'input': {}}})}\n\n"
                
                # 由于工具流不支持断块解析，所以一次性发射 JSON Delta
                yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': block_idx, 'delta': {'type': 'input_json_delta', 'partial_json': tc['function']['arguments']}})}\n\n"
                yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': block_idx})}\n\n"
                block_idx += 1
                
        stop_reason = "tool_use" if tool_calls else "end_turn"
        yield f"event: message_delta\ndata: {json.dumps({'type': 'message_delta', 'delta': {'stop_reason': stop_reason}})}\n\n"
        yield f"event: message_stop\ndata: {json.dumps({'type': 'message_stop'})}\n\n"
        
    if stream:
        return StreamingResponse(_anthropic_sse_stream(), media_type="text/event-stream")
    else:
        return JSONResponse({"error": "Internal fallback proxy requires stream=true for now"}, status_code=400)


if __name__ == "__main__":
    print(f"\n🚀 Antigravity Proxy v5.0  →  http://localhost:{PROXY_PORT}")
    print(f"   Cursor 配置 : Base URL = http://localhost:{PROXY_PORT}/v1")
    print(f"   API Key     : {PROXY_API_KEY}")
    print(f"   日志目录    : {LOG_DIR.resolve()}/")
    print(f"   功能        : 工具调用 ✅  流式输出 ✅  模型自动发现 ✅\n")
    uvicorn.run("server:app", host=PROXY_HOST, port=PROXY_PORT, reload=False)
