#!/bin/bash
#
# 反检测浏览器启动脚本 (macOS)
#
# 使用 open 命令启动独立 Chrome 进程，完全脱离父进程。
# 你可以像普通浏览器一样日常使用，同时支持 Playwright CDP 连接。
#
# 用法:
#   ./start_browser.sh                              # 默认启动
#   ./start_browser.sh https://www.temu.com/        # 启动并访问URL
#   ./start_browser.sh --port 9222                  # 指定CDP端口
#   ./start_browser.sh --port 9222 https://temu.com # 指定端口+URL
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
PROFILE_DIR="$PROJECT_ROOT/data/nodriver-profile"
CDP_PORT=9222
URL=""

# 解析参数
while [[ $# -gt 0 ]]; do
    case $1 in
        --port)
            CDP_PORT="$2"
            shift 2
            ;;
        --profile)
            PROFILE_DIR="$2"
            shift 2
            ;;
        --help|-h)
            echo "用法: $0 [--port CDP端口] [--profile profile目录] [URL]"
            echo ""
            echo "选项:"
            echo "  --port PORT       CDP调试端口 (默认: 9222)"
            echo "  --profile DIR     Chrome用户数据目录 (默认: data/nodriver-profile)"
            echo "  URL               启动后访问的网址"
            echo ""
            echo "示例:"
            echo "  $0                                # 默认启动"
            echo "  $0 https://www.temu.com/          # 访问Temu"
            echo "  $0 --port 9333 https://1688.com/  # 自定义端口"
            exit 0
            ;;
        *)
            URL="$1"
            shift
            ;;
    esac
done

mkdir -p "$PROFILE_DIR"

# 检查是否已有使用该profile的Chrome在运行
if pgrep -f "user-data-dir=$PROFILE_DIR" > /dev/null 2>&1; then
    EXISTING_PORT=$(ps aux | grep "user-data-dir=$PROFILE_DIR" | grep -v grep | grep -oE 'remote-debugging-port=[0-9]+' | head -1 | cut -d= -f2)
    echo "⚠️  浏览器已在运行中！"
    echo "   CDP端口: ${EXISTING_PORT:-未知}"
    echo "   Playwright连接: http://127.0.0.1:${EXISTING_PORT:-$CDP_PORT}"
    if [[ -n "$URL" ]]; then
        echo "   正在打开URL: $URL"
        open -a "Google Chrome" "$URL"
    fi
    exit 0
fi

echo "🚀 正在启动反检测浏览器..."
echo "   Profile: $PROFILE_DIR"
echo "   CDP端口: $CDP_PORT"

# 核心参数说明:
# --remote-debugging-port    开启CDP端口供Playwright连接
# --remote-debugging-host    仅本地访问
# --remote-allow-origins=*   允许跨域CDP连接
# --user-data-dir            持久化用户数据（cookie/历史/登录状态）
# --no-first-run             跳过首次运行向导
# --no-default-browser-check 跳过默认浏览器提示
# --disable-infobars         禁用"Chrome正在被自动化软件控制"提示栏
# --disable-breakpad         禁用崩溃报告
# --disable-session-crashed-bubble  禁用"Chrome未正确关闭"提示
# --disable-search-engine-choice-screen  跳过搜索引擎选择
# --password-store=basic     使用基本密码存储（避免keychain弹窗）
# --no-pings                 禁用超链接ping追踪
# --disable-features=IsolateOrigins,site-per-process  禁用站点隔离（减少进程数）
#
# 【关键】不包含以下参数，因此不会被检测：
# ❌ --enable-automation       (Selenium/Playwright会加)
# ❌ --headless                (无头模式会被检测)
# ❌ --disable-gpu             (GPU禁用会导致Canvas指纹异常)

open -na "Google Chrome" --args \
    --remote-debugging-port="$CDP_PORT" \
    --remote-debugging-host=127.0.0.1 \
    --remote-allow-origins='*' \
    --user-data-dir="$PROFILE_DIR" \
    --no-first-run \
    --no-service-autorun \
    --no-default-browser-check \
    --homepage=about:blank \
    --no-pings \
    --password-store=basic \
    --disable-infobars \
    --disable-breakpad \
    --disable-dev-shm-usage \
    --disable-session-crashed-bubble \
    --disable-search-engine-choice-screen \
    --disable-features=IsolateOrigins,site-per-process \
    ${URL:+"$URL"}

# 等待Chrome启动并验证CDP端口
echo -n "   等待浏览器启动"
for i in $(seq 1 10); do
    sleep 1
    echo -n "."
    if curl -s "http://127.0.0.1:$CDP_PORT/json/version" > /dev/null 2>&1; then
        echo ""
        VERSION=$(curl -s "http://127.0.0.1:$CDP_PORT/json/version" | python3 -c "import sys,json; print(json.load(sys.stdin).get('Browser','unknown'))" 2>/dev/null || echo "unknown")
        echo ""
        echo "🟢 浏览器已启动！"
        echo "   Chrome: $VERSION"
        echo "   CDP端口: $CDP_PORT"
        echo "   Playwright连接: http://127.0.0.1:$CDP_PORT"
        echo ""
        echo "📝 Playwright连接代码:"
        echo "   browser = await p.chromium.connect_over_cdp('http://127.0.0.1:$CDP_PORT')"
        echo ""
        echo "💡 此浏览器完全独立运行，可以像普通Chrome一样日常使用。"
        echo "   关闭浏览器窗口或 Cmd+Q 退出。"
        exit 0
    fi
done

echo ""
echo "⚠️  CDP端口未就绪，但浏览器可能已启动。"
echo "   请手动检查: curl http://127.0.0.1:$CDP_PORT/json/version"
