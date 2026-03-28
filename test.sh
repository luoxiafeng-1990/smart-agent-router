#!/bin/bash
cat << 'EOF' > test_results.txt
=== TEST 1: Cursor to apiclaw.ai (OpenAI format -> Anthropic route) ===
EOF
curl -N -s -X POST http://localhost:8787/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-cursor-proxy-key" \
  -d '{
    "model": "claude-3-5-sonnet-20241022",
    "messages": [{"role": "user", "content": "Just reply with exactly: Hello from Anthropic API!"}],
    "stream": true
  }' >> test_results.txt 2>&1

echo -e "\n\n=== TEST 2: Claude Code to apiclaw.ai (Anthropic format -> Anthropic route) ===" >> test_results.txt
curl -N -s -X POST http://localhost:8787/v1/messages \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-3-5-sonnet-20241022",
    "messages": [{"role": "user", "content": "Reply exactly: Hello from Claude Code Native API!"}],
    "stream": true
  }' >> test_results.txt 2>&1
