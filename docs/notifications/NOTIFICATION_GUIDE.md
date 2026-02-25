# 多终端通知接入指南（短信 + 微信）

本文档面向 MyInvestment 项目，说明如何把当前告警能力扩展到手机等终端。

当前项目中已经有告警事件源：
- `agent_alerts.py`
- `state/alerts_events.jsonl`
- `runs/ops/alerts_latest.json`

建议目标：
- `warn` 默认走 App/IM 推送
- `critical` 同时走 App/IM + 短信


## 0. 个人用户推荐：先用 ntfy App

如果你没有短信资质或企业微信资质，最推荐先用 `ntfy`：
- 手机安装 `ntfy`（iOS/Android）
- 订阅一个随机 topic（例如 `myinv-9f3k2a-alerts`）
- 服务端 HTTP POST 到 `https://ntfy.sh/<topic>` 即可推送

快速测试：

```bash
curl -X POST "https://ntfy.sh/myinv-9f3k2a-alerts" \
  -H "Title: MyInvestment Alert" \
  -H "Priority: 5" \
  -H "Tags: warning" \
  -d "critical: oldest_pending_execution_hours=50.2"
```

本项目可直接用 `agent_notifier.py` + scheduler 集成 ntfy。

## 1. 方案总览

可选通道：
- 短信：阿里云短信 / 腾讯云短信 / Twilio
- 企业微信机器人（推荐，成本低）
- 个人微信中转（Server酱、WxPusher、PushPlus 等）

推荐优先级：
1. 企业微信机器人（主通道）
2. 短信（critical 兜底）
3. 个人微信中转（如果你不想使用企业微信）

## 2. 短信通道接入（适合 critical）

### 2.1 前置条件

无论使用哪家短信服务，通常都需要：
- 实名认证账号
- 申请短信签名
- 申请短信模板
- 获取 API 凭据（AccessKey/Secret）

注意：短信一般存在审核周期和费用，不适合高频告警全量发送。

### 2.2 官方文档入口（建议优先看）

- 阿里云短信服务：
  - 产品主页：https://help.aliyun.com/zh/sms/
  - 控制台发送快速开始：https://help.aliyun.com/zh/sms/getting-started/use-sms-console-1
- 腾讯云短信：
  - 产品文档入口：https://cloud.tencent.com/document/product/382

### 2.3 在本项目中的建议配置

建议在 `agent_config.json` 新增：

```json
{
  "notifications": {
    "enabled": true,
    "routing": {
      "warn": ["wecom"],
      "critical": ["wecom", "sms"],
      "resolved": ["wecom"]
    },
    "sms": {
      "provider": "aliyun",
      "enabled": true,
      "template_code": "SMS_xxx",
      "sign_name": "MyInvestment"
    }
  }
}
```

密钥建议走环境变量，不要写入仓库：

```bash
export MYINVEST_SMS_PROVIDER=aliyun
export MYINVEST_SMS_ACCESS_KEY_ID=xxxx
export MYINVEST_SMS_ACCESS_KEY_SECRET=xxxx
export MYINVEST_SMS_REGION=cn-hangzhou
```

### 2.4 短信发送策略建议

- 仅 `critical + opened/escalated` 发短信
- 相同 `check_id + level` 冷却 30 分钟
- 夜间只保留 critical 短信
- 发送结果写 `state/notify_delivery_log.jsonl`

## 3. 个体用户如何用微信机器人

先说明关键事实：
- “个人微信（微信聊天）”没有官方通用 Webhook 机器人接口。
- 对个人开发者最稳定的官方方式是“企业微信机器人”。

你可以选两条路径。

### 3.1 路径 A：企业微信机器人（推荐）

适用人群：
- 个人用户也可用（创建一个自己的企业微信组织/团队），用于给自己或小团队推送告警。

核心流程：
1. 安装企业微信（手机或桌面端）。
2. 创建企业/团队并创建内部群。
3. 在群里添加“群机器人”。
4. 复制机器人 Webhook 地址（形如 `https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...`）。
5. 项目通过 HTTP POST 调用该地址。

限制说明：
- 一般仅支持企业微信内部群通知，外部群场景受限。

Webhook 文本消息示例：

```bash
curl -X POST 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "msgtype": "text",
    "text": {
      "content": "[MyInvestment] critical: oldest_pending_execution_hours=50.2"
    }
  }'
```

Markdown 示例：

```bash
curl -X POST 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY' \
  -H 'Content-Type: application/json' \
  -d '{
    "msgtype": "markdown",
    "markdown": {
      "content": "## MyInvestment Alert\n> level: **critical**\n> check_id: `oldest_pending_execution_hours`\n> value: `50.2`"
    }
  }'
```

建议把 webhook 放环境变量：

```bash
export MYINVEST_WECOM_WEBHOOK='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...'
```

### 3.2 路径 B：个人微信中转服务（可选）

适用人群：
- 不想使用企业微信，只想把通知送到个人微信。

常见中转：
- Server酱（ServerChan Turbo）
- WxPusher
- PushPlus

说明：
- 这些服务通常通过“公众号/订阅”把消息送到个人微信。
- 接入更快，但稳定性和配额依赖第三方平台策略。

Server酱示例：

```bash
curl -X POST 'https://sctapi.ftqq.com/<SENDKEY>.send' \
  -d 'title=MyInvestment Critical Alert' \
  -d 'desp=check_id=oldest_pending_execution_hours\nvalue=50.2'
```

WxPusher示例（简化）：

```bash
curl -X POST 'https://wxpusher.zjiecode.com/api/send/message' \
  -H 'Content-Type: application/json' \
  -d '{
    "appToken": "AT_xxx",
    "content": "MyInvestment critical alert",
    "summary": "critical",
    "contentType": 1,
    "uids": ["UID_xxx"]
  }'
```

## 4. 接入到当前项目的最小落地步骤

项目已提供 `agent_notifier.py`：
- 输入：`state/alerts_events.jsonl`
- 逻辑：按 cursor 增量消费 -> 规则路由 -> 调用渠道适配器
- 输出：`state/notify_delivery_log.jsonl`、`state/notify_cursor.json`

建议调度链路：
- 在 `agent_scheduler.py` 的 `run_alerts` 之后执行 `run_notifier`

建议先做两种适配器：
- `WecomRobotAdapter`（主通道）
- `SmsAdapter`（critical 兜底）

## 5. 告警分级与路由模板（可直接用）

- `warn + opened` -> 企业微信
- `critical + opened` -> 企业微信 + 短信
- `critical + escalated` -> 企业微信 + 短信（可升级到备份联系人）
- `resolved` -> 企业微信
- `reminder` -> 企业微信（每 6h 一次）

## 6. 安全与治理

- 密钥只放环境变量，不入库
- webhook URL 视为密钥，禁止明文打印
- 对下游接口失败做重试和指数退避
- 保留完整发送审计日志

## 7. FAQ

Q: 我只有个人微信，能直接用机器人 Webhook 吗？
A: 不能直接用“个人微信机器人 Webhook”（官方无此通用接口）。建议用企业微信机器人，或用 Server酱/WxPusher 这类中转。

Q: 短信是否建议全量发送？
A: 不建议。短信更适合 critical 告警和兜底升级。

Q: 我只想自己手机收到，最省事方案是什么？
A: 企业微信机器人最快；如果坚持个人微信，选 Server酱/WxPusher。
