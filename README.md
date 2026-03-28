# Cursor × Google Gemini 反向代理

让 Cursor 通过 OpenAI 兼容接口调用 Google Gemini 模型，并支持 Cursor 所有工具功能（代码补全、文件操作、function calling 等）。

---

## 快速开始

### 1. 获取 Google API Key

访问 [Google AI Studio](https://aistudio.google.com/)，点击 **Get API key** → **Create API key**

### 2. 配置

编辑 `.env` 文件：

```env
GOOGLE_API_KEY=你的API Key（AIza开头）
PORT=8787
PROXY_API_KEY=sk-cursor-proxy-key   # Cursor 里填这个
DEFAULT_MODEL=gemini-2.5-pro-preview-03-25
```

### 3. 启动代理

如果你不需要绕过 Cursor 的 SSRF（本地 IP 屏蔽）限制：
```bash
chmod +x start.sh
./start.sh
```

**如果 Cursor 报错 `ssrf_blocked` (连接到私有IP被拒绝)**：
请改为运行隧道脚本，这会创建免费的公网隧道暴露本地代理：
```bash
chmod +x start_tunnel.sh
./start_tunnel.sh
```

或者手动：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python server.py
```

---

## Cursor 配置

在 Cursor → Settings → Models → OpenAI API Key:

| 设置项 | 值 |
|--------|-----|
| **Base URL** | `http://localhost:8787/v1` （如果通过隧道启动，填终端输出的 `trycloudflare.com/v1` 链接） |
| **API Key** | `sk-cursor-proxy-key` |

然后在模型列表中选择任一 Gemini 模型即可。

---

## 常见问题 (FAQ)

**Q: Cursor 报错 `ssrf_blocked` 或 `connection to private IP is blocked` 怎么办？**

**A**: Cursor 更新后增强了 SSRF 防护，阻止了直接请求 `localhost` 或 `127.0.0.1`。
解决方法是使用 `start_tunnel.sh` 启动，它会自动调用免费的 Cloudflare 隧道给你一个公网 URL（例如 `https://xxxx.trycloudflare.com`），把 Cursor 里面的 Base URL 更换为此地址即可完美绕过防护。

---

## 支持的模型

| 模型 ID | 说明 |
|---------|------|
| `gemini-2.5-pro` | 最强推理，适合复杂编程任务 |
| `gemini-2.5-flash` | 快速且强大 |
| `gemini-2.0-flash` | 速度与质量均衡 |
| `gemini-2.0-flash-lite` | 最快，适合代码补全 |
| `gemini-2.0-flash-thinking` | 思维链推理 |
| `gemini-1.5-pro` | 超长上下文（100万 token）|
| `gemini-1.5-flash` | 长上下文快速版 |
| `gpt-4o` | 别名 → gemini-2.0-flash |
| `gpt-4` | 别名 → gemini-2.5-pro |

---

## 架构

```
Cursor → http://localhost:8787/v1/chat/completions
             │
             ├─ 请求解析（OpenAI 格式）
             ├─ 格式转换（OpenAI → Gemini）
             ├─ Tools / Function Calling 透传
             ├─ 调用 Gemini API
             ├─ 格式转换（Gemini → OpenAI）
             └─ 流式/非流式响应返回 Cursor
```

## 特性

- ✅ OpenAI 兼容 `/v1/chat/completions` 接口
- ✅ 流式响应（SSE）支持
- ✅ Function Calling / Tools 完整透传
- ✅ 多模型映射
- ✅ 图片/多模态支持
- ✅ system prompt 正确处理
- ✅ 简单 API Key 鉴权
