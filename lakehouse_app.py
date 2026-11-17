import json
from pathlib import Path
import streamlit as st
from portfolio_pipeline.pipeline import EventPipeline
from portfolio_pipeline.__main__ import demo_batches

st.set_page_config(page_title='增量运营数据管道',layout='wide')
st.title('增量运营数据管道与质量检查')
st.caption('dlt · DuckDB · 数据契约 · 24小时回看 · 主键合并 · 质量隔离 · SQL指标')
st.info('人工构造事件用于工程验证，不能作为真实营收或增长成果。')
pipe=EventPipeline(Path('.data/browser'))
if st.button('运行示例：首次导入、重放与金额修正'):
    with st.spinner('执行提取、规范化和加载…'):
        first,correction=demo_batches()
        pipe.run(first)
        before=pipe.metrics()
        pipe.run(first)
        same=before==pipe.metrics()
        report=pipe.run(correction)
    st.success('重复导入指标一致：'+str(same))
    st.json(report['metrics'])
path=pipe.root/'latest_report.json'
if path.exists():
    report=json.loads(path.read_text(encoding='utf-8'))
    metrics=report['metrics']
    a,b,c=st.columns(3)
    a.metric('有效事件',metrics['event_count'])
    b.metric('顺序完成购买会话',metrics['purchase_sessions'])
    c.metric('购买金额（分）',metrics['revenue_cents'])
    import altair as alt
    import pandas as pd
    stages=['浏览','加购','购买']
    funnel=pd.DataFrame({'环节':stages,'会话数':[metrics[k] for k in ['view_sessions','cart_sessions','purchase_sessions']]})
    st.altair_chart(alt.Chart(funnel).mark_bar().encode(x=alt.X('环节:N',sort=stages,axis=alt.Axis(labelAngle=0)),y='会话数:Q'),use_container_width=True)
    st.dataframe(metrics['channels'],hide_index=True)
    with st.expander('质量检查、游标与运行结果'):
        st.json(report)
    st.download_button('下载运行报告',json.dumps(report,ensure_ascii=False,indent=2),file_name='pipeline-report.json')
