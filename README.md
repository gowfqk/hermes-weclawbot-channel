# Hermes WeClawBot Bridge Channel Adapter

将 WeClawBot-Bridge 的微信消息接入 Hermes Gateway。

> 依赖项目：[WeClawBot-Bridge](https://github.com/gowfqk/WeClawBot-Bridge)

## 架构

```text
微信 → WeClawBot-Bridge → 本插件 (WebSocket) → Hermes Gateway
```

## 安装

```bash
# 1. 克隆插件到 Hermes 插件目录
mkdir -p ~/.hermes/plugins/weclawbot
cp plugin.yaml ~/.hermes/plugins/weclawbot/
cp src/adapter.py ~/.hermes/plugins/weclawbot/__init__.py

# 2. 在 Hermes 中启用
hermes plugins enable weclawbot
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

微信发消息，确认 Hermes 回复。

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
