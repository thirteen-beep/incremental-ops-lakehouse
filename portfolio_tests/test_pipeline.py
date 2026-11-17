from copy import deepcopy
from datetime import datetime,timedelta,timezone
import json
import pytest
from portfolio_pipeline.pipeline import EventPipeline,validate_rows
from portfolio_pipeline.__main__ import demo_batches


def test_three_stage_load_replay_correction_and_restart(tmp_path):
    rows,correction=demo_batches()
    pipeline=EventPipeline(tmp_path)
    first=pipeline.run(rows)['metrics']
    assert first['event_count']==350
    assert [first[k] for k in ['view_sessions','cart_sessions','purchase_sessions']]==[200,100,50]
    replay=pipeline.run(rows)['metrics']
    assert replay==first
    corrected=EventPipeline(tmp_path).run(correction)['metrics']
    assert corrected['event_count']==350
    assert corrected['revenue_cents']==first['revenue_cents']-300


def test_old_version_cannot_overwrite_updated_record(tmp_path):
    rows,correction=demo_batches()
    pipeline=EventPipeline(tmp_path)
    pipeline.run(rows)
    after=pipeline.run(correction)['metrics']
    result=pipeline.run(rows)
    assert result['stale_versions_ignored']==1 and result['metrics']==after


def test_schema_and_amount_contract_quarantine(tmp_path):
    rows,_=demo_batches()
    bad1={**rows[0],'new_unapproved_field':'drift'}
    bad2={**rows[2],'amount_cents':'1299'}
    pipeline=EventPipeline(tmp_path)
    result=pipeline.run([rows[0],bad1,bad2])
    assert result['rejected_rows']==2 and result['metrics']['event_count']==1
    assert len((tmp_path/'quarantine.jsonl').read_text().splitlines())==2


def test_soft_delete_removed_from_metrics(tmp_path):
    rows,_=demo_batches()
    pipeline=EventPipeline(tmp_path)
    before=pipeline.run(rows)['metrics']
    deletion={**rows[0],'is_deleted':True,'updated_at':'2026-11-02T08:00:00+00:00'}
    after=pipeline.run([deletion])['metrics']
    assert after['event_count']==before['event_count']-1
    assert after['purchase_sessions']==before['purchase_sessions']-1


def test_failure_does_not_advance_checkpoint(tmp_path):
    rows,correction=demo_batches()
    pipe=EventPipeline(tmp_path)
    before=pipe.run(rows)['metrics']
    state_before=json.dumps(pipe.pipeline.state.get('sources',{}),sort_keys=True,default=str)
    with pytest.raises(Exception,match='injected extraction failure'):
        pipe.run(correction,simulate_source_failure=True)
    # Reopen using the persisted state, not an in-flight runtime reference.
    reopened=EventPipeline(tmp_path)
    assert reopened.metrics()==before
    assert json.dumps(reopened.pipeline.state.get('sources',{}),sort_keys=True,default=str)==state_before
    assert reopened.run(correction)['metrics']['revenue_cents']==before['revenue_cents']-300


@pytest.mark.parametrize('change',[{'updated_at':'2020-01-01T00:00:00Z'}, {'occurred_at':'2026-11-01T08:00:00'}, {'is_deleted':1}, {'event_type':'unknown'}])
def test_contract_rejects_invalid(change):
    rows,_=demo_batches()
    good,bad=validate_rows([{**rows[0],**change}])
    assert not good and len(bad)==1


def test_equal_timestamp_conflicting_versions_not_arbitrarily_selected():
    rows,_=demo_batches()
    good,bad=validate_rows([rows[0],{**rows[0],'channel':'other'}])
    assert not good and len(bad)==1


def test_24h_lookback_handles_late_arrivals(tmp_path):
    rows,_=demo_batches()
    pipe=EventPipeline(tmp_path)
    pipe.run(rows)
    late={**rows[0],'event_id':'late','session_id':'late','updated_at':'2026-11-01T08:00:00+00:00'}
    assert pipe.run([late])['metrics']['event_count']==351


def test_outside_lookback_needs_backfill(tmp_path):
    rows,correction=demo_batches()
    pipe=EventPipeline(tmp_path)
    pipe.run(rows)
    far={**correction[0],'updated_at':'2026-11-05T08:00:00+00:00'}
    pipe.run([far])
    late={**rows[0],'event_id':'too-old','session_id':'old'}
    assert pipe.run([late])['metrics']['event_count']==350


def test_reused_session_id_cannot_join_different_users(tmp_path):
    rows,_=demo_batches()
    split=[{**rows[i],'user_id':f'user-{i}','session_id':'shared'} for i in range(3)]
    result=EventPipeline(tmp_path).run(split)['metrics']
    assert result['view_sessions']==1
    assert result['cart_sessions']==0 and result['purchase_sessions']==0
