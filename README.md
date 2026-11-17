# 增量运营数据管道与质量检查

这是基于 [dlt-hub/dlt](https://github.com/dlt-hub/dlt) 的开源二次开发作品。保留上游历史和许可证；新增应用通过 AI 辅助实现，框架本身不属于个人原创。

运行依赖：dlt 1.32.0 + DuckDB。本地验证日期：2026-11-17。

## 新增内容

- Pydantic 严格数据契约，隔离字段漂移、金额类型错误与相同时间戳版本冲突。
- dlt updated_epoch 游标、24小时回看、event_id merge、旧版本保护和软删除。
- DuckDB SQL 按 user_id 与 session_id 计算严格时间顺序漏斗；金额使用整数分。
- 覆盖重放幂等、金额修正、迟到数据、失败后状态恢复、跨用户同会话号隔离；提供看板和 JSON 报告。

## 运行

Python 3.12，建议独立虚拟环境，从当前项目根目录执行：

~~~powershell
python -m pip install -r requirements-portfolio.txt
python -m portfolio_pipeline --root .data/demo
python -m streamlit run lakehouse_app.py --server.headless true --server.fileWatcherType none
python -m pytest portfolio_tests -q
~~~

## 验证边界

- 合成350条事件用于工程验证，非真实订单量、营收或业务增长。
- 单进程本地 DuckDB，不是云端分布式湖仓；未做生产吞吐与SLA验证。
- 24小时之外的未见旧记录需要显式回填，不能声称无限迟到覆盖。
- Windows Streamlit 文件监听可能干扰 dlt 原子写入，项目配置已关闭监听。

详见 [VALIDATION.md](VALIDATION.md) 与 [UPSTREAM.md](UPSTREAM.md)。原始说明保存在 portfolio_docs/UPSTREAM_README.md。
