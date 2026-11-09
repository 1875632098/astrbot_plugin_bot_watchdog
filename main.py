"""互相看护（watchdog）。

作用：持续监控另一个 bot 的健康状态；连续失败达到阈值时，自动触发对方重启。
- 优先走对方面板的重启接口（自动用账号密码登录拿 Token，无需手填长期 Token）
- 没有面板凭据时，退回「在群里发一条重启指令」的兜底方案
- 自身重启后插件会重新加载，看护自动继续，不会出现「一边重启完就没人管」的死锁
"""

import asyncio
import json
import time
import urllib.error
import urllib.request

from astrbot.api import logger
from astrbot.api.star import Context, Star, register


@register(
    "astrbot_plugin_bot_watchdog",
    "宁喵喵",
    "监控另一个 bot，掉线达到阈值后自动触发重启（面板接口优先，群指令兜底）",
    "1.1.0",
)
class BotWatchdog(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        cfg = config or {}
        self.target_name = cfg.get("target_name", "目标 bot")
        self.health_url = (cfg.get("health_url") or "").strip()
        self.interval = max(10, int(cfg.get("check_interval_seconds", 60) or 60))
        self.threshold = max(1, int(cfg.get("failure_threshold", 3) or 3))
        self.timeout = max(3, int(cfg.get("timeout_seconds", 8) or 8))

        # 面板重启
        self.panel_base_url = (cfg.get("panel_base_url") or "").strip().rstrip("/")
        self.username = (cfg.get("panel_username") or "").strip()
        self.password = (cfg.get("panel_password") or "").strip()
        self.restart_path = (cfg.get("restart_path") or "/api/v1/system/restart").strip()

        # 兜底
        self.notify_umo = (cfg.get("notify_umo") or "").strip()
        self.command_fallback = (cfg.get("command_fallback") or "").strip()
        self.restart_cooldown = max(60, int(cfg.get("restart_cooldown_seconds", 300) or 300))

        self._task: asyncio.Task | None = None
        self._fails = 0
        self._last_restart_ts = 0.0

    async def initialize(self):
        if not self.health_url:
            logger.warning("[bot_watchdog] 未配置 health_url，看护未启动")
            return
        self._task = asyncio.create_task(self._loop())
        logger.info(
            f"[bot_watchdog] 开始看护 {self.target_name}：{self.health_url}"
            f"（间隔 {self.interval}s / 阈值 {self.threshold}）"
        )

    async def terminate(self):
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None

    # ---------- 内部 ----------

    async def _loop(self):
        while True:
            try:
                await asyncio.sleep(self.interval)
                ok = await asyncio.get_running_loop().run_in_executor(None, self._health)
                if ok:
                    if self._fails:
                        logger.info(f"[bot_watchdog] {self.target_name} 已恢复")
                    self._fails = 0
                    continue

                self._fails += 1
                logger.warning(
                    f"[bot_watchdog] {self.target_name} 无响应（{self._fails}/{self.threshold}）"
                )
                if self._fails >= self.threshold:
                    await self._restart_target()
                    self._fails = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:  # 看护自身不能被打断
                logger.warning(f"[bot_watchdog] 检查异常: {e}")

    def _health(self) -> bool:
        try:
            req = urllib.request.Request(
                self.health_url, headers={"User-Agent": "bot-watchdog"}
            )
            with urllib.request.urlopen(req, timeout=self.timeout):
                return True
        except urllib.error.HTTPError:
            # 有 HTTP 响应就算活着（401/403 都说明服务在跑）
            return True
        except Exception:
            return False

    # ---------- 重启 ----------

    def _login(self) -> str | None:
        """用账号密码登录面板，返回 Token。"""
        if not (self.panel_base_url and self.username and self.password):
            return None
        try:
            body = json.dumps(
                {"username": self.username, "password": self.password}
            ).encode()
            req = urllib.request.Request(
                self.panel_base_url + "/api/auth/login",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
            return (data.get("data") or {}).get("token") or data.get("token")
        except Exception as e:
            logger.warning(f"[bot_watchdog] 面板登录失败: {e}")
            return None

    def _call_restart(self) -> bool:
        token = self._login()
        if not token:
            return False
        try:
            req = urllib.request.Request(
                self.panel_base_url + self.restart_path,
                data=b"{}",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {token}",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=20) as r:
                logger.info(
                    f"[bot_watchdog] 已请求 {self.target_name} 重启，HTTP {r.status}"
                )
            return True
        except Exception as e:
            logger.warning(f"[bot_watchdog] 面板重启失败: {e}")
            return False

    async def _restart_target(self):
        now = time.time()
        if now - self._last_restart_ts < self.restart_cooldown:
            logger.info("[bot_watchdog] 距离上次自动重启不足冷却时间，跳过")
            return
        self._last_restart_ts = now

        # 1) 面板接口（自动登录）
        if self.panel_base_url and self.username and self.password:
            ok = await asyncio.get_running_loop().run_in_executor(
                None, self._call_restart
            )
            if ok:
                return
            logger.warning("[bot_watchdog] 面板重启失败，改走群指令兜底")

        # 2) 群指令兜底
        if self.notify_umo and self.command_fallback:
            try:
                from astrbot.core.message.components import Plain
                from astrbot.core.message.message_result import MessageChain
            except Exception:
                from astrbot.core.message.components import Plain
                from astrbot.core.message.message_event_result import MessageChain
            try:
                await self.context.send_message(
                    session=self.notify_umo,
                    message_chain=MessageChain([Plain(self.command_fallback)]),
                )
                logger.info(f"[bot_watchdog] 已发送兜底重启指令：{self.command_fallback}")
            except Exception as e:
                logger.warning(f"[bot_watchdog] 发送兜底指令失败: {e}")
        else:
            logger.warning("[bot_watchdog] 没有可用的重启方式（缺少面板凭据或群 UMO）")
