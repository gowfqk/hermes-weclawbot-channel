# Hermes WeClawBot Bridge Channel Adapter

将 WeClawBot-Bridge 的微信消息接入 Hermes Gateway。

> 依赖项目：[WeClawBot-Bridge](https://github.com/gowfqk/WeClawBot-Bridge)

## 架构

```text
微信 → WeClawBot-Bridge → 本插件 (WebSocket) → Hermes Gateway
```

## 安装

```bash
# 1. 安装运行时依赖（与 Hermes 当前使用的 websockets 版本一致）
cd hermes-weclawbot-channel
uv pip install "websockets==15.0.1"

# 2. 复制为 Hermes 平台插件
mkdir -p ~/.hermes/plugins/weclawbot
cp plugin.yaml ~/.hermes/plugins/weclawbot/
cp src/adapter.py ~/.hermes/plugins/weclawbot/__init__.py

# 3. 在 Hermes 中启用
hermes plugins enable weclawbot
```

开发/回归验证可直接在仓库运行：

```bash
uv run --with "websockets==15.0.1" --with "pytest>=8,<10" pytest
```

或安装为 Python 包（用于测试和工具，而非 Hermes 的插件发现路径）：

```bash
uv pip install -e ".[test]"
```

## 配置

### Bridge 端

在 Bridge 管理面板创建 Agent（或复用已有的 `h`）：

| 字段 | 值 |
|---|---|
| ID | `h`（或任意） |
| 名称 | Hermes |
| 命令 | `hermes` |
| 类型 | **WS Remote** |
| 超时 | `180000` |

复制 Token。

### Hermes 端

> **安全要求：必须显式配置 `WECLAWBOT_BRIDGE_URL` 或 `platforms.weclawbot.extra.bridge_url`，且必须是完整的 `ws://` 或 `wss://` 地址。** 插件没有公网默认地址；未配置或 URL 非法时不会发送 Token，也不会启动连接。

**方式一：环境变量（推荐生产）**

```bash
cat > /root/.config/weclawbot-adapter.env <<'ENV'
WECLAWBOT_TOKEN=*** Bridge Token ***
WECLAWBOT_BRIDGE_URL=wss://<your-bridge-url>/ws/agent
WECLAWBOT_AGENT_ID=h
ENV
chmod 600 /root/.config/weclawbot-adapter.env
```

```bash
mkdir -p /etc/systemd/system/hermes-gateway.service.d
cat > /etc/systemd/system/hermes-gateway.service.d/weclawbot-adapter.conf <<'EOF'
[Service]
EnvironmentFile=/root/.config/weclawbot-adapter.env
EOF
systemctl daemon-reload
systemctl restart hermes-gateway.service
```

**方式二：直接写入 config.yaml**

```yaml
# ~/.hermes/config.yaml
platforms:
  weclawbot:
    enabled: true
    extra:
      token: "*** Token ***"
      bridge_url: "wss://<your-bridge-url>/ws/agent"
      agent_id: "h"
```

> ⚠️ 如果 `Bridge` 管理面板已配置 Token，不要在聊天记录或提交中输出 Token 明文。

## 验证

```bash
journalctl -u hermes-gateway.service -f | grep -i weclawbot
```

预期日志：

```text
WeClawBot: authenticated to Bridge as agent h
```

微信发消息，确认 Hermes 回复。认证被拒绝（Token、Agent ID 不正确）会被标记为不可重试的配置错误；网络断连才会使用退避重连。长时间工具任务在独立处理任务中执行，监听循环仍会处理 Bridge `ping` 并及时返回 `pong`。

## 与 OpenClaw 并行

Hermes Adapter 使用 Agent `h`，OpenClaw Channel Plugin 使用 Agent `openclaw`。
微信内切换：

```text
#hermes    → Hermes
#openclaw  → OpenClaw
```

## 故障排查

**插件未发现**

```bash
hermes plugins list | grep weclawbot
```

**认证失败**

- 检查 Bridge Agent ID 与 `WECLAWBOT_AGENT_ID` 一致
- 确认没有其他客户端使用同一 Agent ID 连接
- 旧的 `weclaw-agent.service` 可能占用连接：

```bash
systemctl stop weclaw-agent.service
systemctl disable weclaw-agent.service
```
