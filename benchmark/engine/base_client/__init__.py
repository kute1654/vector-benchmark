"""驱动式客户端基类包的统一导出。

注意这里的导入路径是顶层 ``engine.*`` 而不是 ``benchmark.engine.*``：
``run.py`` 会把 ``benchmark/`` 加入 ``sys.path``，因此整个工程同时存在
``benchmark.engine``（脚本式引擎）与 ``engine``（驱动式客户端）两套命名空间。

``BaseClient`` 组合 Configurator/Uploader/Searcher 三个阶段，
``IncompatibilityError`` 用于“配置与引擎不兼容”这种可跳过的实验
（``run.py`` 捕获后会跳过该 target，而不是让整轮实验失败）。
"""

from engine.base_client.client import BaseClient
from engine.base_client.configure import BaseConfigurator
from engine.base_client.search import BaseSearcher
from engine.base_client.upload import BaseUploader


class IncompatibilityError(Exception):
    """当前 配置/数据集/引擎 组合不受支持，应跳过而不是中止整轮实验。"""
