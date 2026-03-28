#!/bin/bash
# Antigravity Proxy v5.0 启动脚本
# 不需要任何外部 API Key，直接使用 Antigravity 会员账号

set -e
cd "$(dirname "$0")"

# 检查虚拟环境
if [ ! -d ".venv" ]; then
    echo "📦 创建虚拟环境..."
    python3 -m venv .venv
fi

# 激活虚拟环境并安装依赖
source .venv/bin/activate
echo "📦 安装/检查依赖..."
pip install -q -r requirements.txt

echo ""
echo "🚀 启动 Antigravity 代理 v5.0..."
echo "   Cursor 配置: Base URL = http://localhost:8787/v1"
echo "   API Key    : sk-cursor-proxy-key"
echo ""
python server.py
