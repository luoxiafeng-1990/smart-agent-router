#!/bin/bash
# Antigravity Proxy v5.0 Cloudflare Tunnel 启动脚本
# 用于解决 Cursor 更新后屏蔽本地 IP (localhost/127.0.0.1) 的 SSRF 错误

set -e
cd "$(dirname "$0")"

PORT=8787

# 1. 启动本地代理后台进程 (如果未运行)
if ! curl -s http://localhost:$PORT/health > /dev/null; then
    echo "📦 正在后台启动 Antigravity 代理..."
    ./start.sh > proxy.log 2>&1 &
    PROXY_PID=$!
    
    # 等待代理启动
    for i in {1..10}; do
        if curl -s http://localhost:$PORT/health > /dev/null; then
            break
        fi
        sleep 1
    done
else
    echo "✅ Antigravity 代理已在端口 $PORT 运行中"
fi

# 2. 检查并下载 cloudflared
CLOUDFLARED="./cloudflared"
if [ ! -f "$CLOUDFLARED" ]; then
    echo "⏬ 正在下载 cloudflared..."
    OS="linux-amd64"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        if [[ $(uname -m) == "arm64" ]]; then
            OS="darwin-arm64"
        else
            OS="darwin-amd64"
        fi
    fi
    curl -sL "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-$OS" -o "$CLOUDFLARED"
    chmod +x "$CLOUDFLARED"
fi

# 3. 启动 cloudflared
echo ""
echo "🌐 正在创建免费的 Cloudflare 隧道..."
rm -f tunnel.log

# 启动并在后台运行，将日志输出到文件
$CLOUDFLARED tunnel --url http://localhost:$PORT > tunnel.log 2>&1 &
TUNNEL_PID=$!

# 等待获取 URL
echo "⏳ 等待获取公网 URL..."
TUNNEL_URL=""
for i in {1..15}; do
    TUNNEL_URL=$(grep "https://.*trycloudflare.com" tunnel.log | awk '{print $4}' | head -1)
    if [ -n "$TUNNEL_URL" ]; then
        break
    fi
    sleep 1
done

if [ -z "$TUNNEL_URL" ]; then
    echo "❌ 无法获取 Cloudflare 隧道 URL，请检查网络或查看 tunnel.log"
    kill $TUNNEL_PID 2>/dev/null || true
    exit 1
fi

echo ""
echo "=========================================================="
echo "🚀 隧道启动成功！"
echo "请在 Cursor 中进行如下配置："
echo ""
echo "   Base URL : $TUNNEL_URL/v1"
echo "   API Key  : sk-cursor-proxy-key"
echo ""
echo "说明：这个 URL 是公网 URL，完美绕过 Cursor 的 SSRF 报错。"
echo "隧道运行中，请不要关闭此窗口..."
echo "按 Ctrl+C 退出隧道"
echo "=========================================================="

# 捕获退出信号，清理后台进程
trap "echo -e '\n正在关闭隧道...'; kill $TUNNEL_PID 2>/dev/null || true; exit 0" SIGINT SIGTERM

# 保持脚本运行，同时跟踪日志中的请求（可选）
wait $TUNNEL_PID
