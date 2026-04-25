"""服务层入口。

这里统一暴露新的 service registry 和 mode bundle。
新的调用方应该只从 app.service 及其子模块取依赖。
"""

from app.service.registry import ModeServiceBundle, ServiceRegistry

__all__ = ["ModeServiceBundle", "ServiceRegistry"]
