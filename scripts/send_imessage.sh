#!/bin/bash
# 通过 iMessage 发送图片到手机
# 用法: ./scripts/send_imessage.sh <手机号或AppleID> [图片路径] [消息文字]
#
# 示例:
#   ./scripts/send_imessage.sh +8613800138000
#   ./scripts/send_imessage.sh +8613800138000 screenshots/xks_panel.png
#   ./scripts/send_imessage.sh user@icloud.com screenshots/xks_panel.png "小卡叔选债截图"

set -e

RECIPIENT="${1:?用法: $0 <手机号或AppleID> [图片路径] [消息文字]}"
IMAGE_PATH="${2:-screenshots/xks_panel.png}"
MESSAGE="${3:-小卡叔选债 — 筛选结果截图}"

# 转绝对路径
if [[ ! "$IMAGE_PATH" = /* ]]; then
  IMAGE_PATH="$(cd "$(dirname "$IMAGE_PATH")" && pwd)/$(basename "$IMAGE_PATH")"
fi

if [[ ! -f "$IMAGE_PATH" ]]; then
  echo "❌ 图片不存在: $IMAGE_PATH"
  exit 1
fi

echo "📱 发送 iMessage..."
echo "   收件人: $RECIPIENT"
echo "   图片: $IMAGE_PATH"
echo "   消息: $MESSAGE"

# 先发文字消息
osascript <<EOF
tell application "Messages"
    set targetService to 1st account whose service type = iMessage
    set targetBuddy to participant "$RECIPIENT" of targetService
    send "$MESSAGE" to targetBuddy
end tell
EOF

# 再发图片附件
osascript <<EOF
tell application "Messages"
    set targetService to 1st account whose service type = iMessage
    set targetBuddy to participant "$RECIPIENT" of targetService
    send POSIX file "$IMAGE_PATH" to targetBuddy
end tell
EOF

echo "✅ 已发送!"
