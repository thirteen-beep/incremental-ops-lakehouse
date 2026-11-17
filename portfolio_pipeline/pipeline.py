from __future__ import annotations
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Literal
import dlt
from dlt.destinations import duckdb
import duckdb as sql
from pydantic import BaseModel,ConfigDict,Field,ValidationError,model_validator

os.environ.setdefault('RUNTIME__DLTHUB_TELEMETRY','false')


class Event(BaseModel):
    model_config=ConfigDict(extra='forbid',strict=True)
    event_id:str=Field(min_length=1,max_length=100)
    user_id:str=Field(min_length=1,max_length=100)
    session_id:str=Field(min_length=1,max_length=100)
    channel:str=Field(min_length=1,max_length=50)
    event_type:Literal['view','cart','purchase']
    occurred_at:datetime
    updated_at:datetime
    amount_cents:int | None=Field(default=None,ge=0)
    is_deleted:bool=False

    @model_validator(mode='after')
    def rules(self):
        if self.occurred_at.tzinfo is None or self.updated_at.tzinfo is None:
            raise ValueError('Timestamps require an explicit UTC offset')
        if self.event_type=='purchase' and self.amount_cents is None:
            raise ValueError('Purchase requires integer amount_cents')
        if self.updated_at < self.occurred_at:
            raise ValueError('updated_at precedes occurred_at')
        return self


def validate_rows(rows):
    valid={}
    rejected=[]
    for number,row in enumerate(rows,1):
        try:
            # model_validate_json intentionally accepts ISO timestamps while keeping numeric fields strict.
            event=Event.model_validate_json(json.dumps(row))
            record=event.model_dump()
            record['updated_epoch']=int(event.updated_at.timestamp())
            record.pop('updated_at')
            previous=valid.get(event.event_id)
            if previous is None or record['updated_epoch']>previous['updated_epoch']:
                valid[event.event_id]=record
            elif record['updated_epoch']==previous['updated_epoch'] and record!=previous:
                raise ValueError('Conflicting event versions share an update timestamp')
        except (ValidationError,ValueError,TypeError) as exc:
            rejected.append({'row':number,'event_id':row.get('event_id') if isinstance(row,dict) else None,
                             'reason':str(exc)[:300]})
    # A conflict invalidates that key rather than arbitrarily choosing an equal-time version.
    conflicted={item['event_id'] for item in rejected if 'Conflicting event' in item['reason']}
    return [row for key,row in valid.items() if key not in conflicted],rejected


class EventPipeline:
    def __init__(self,root:Path):
        self.root=Path(root).resolve()
        self.root.mkdir(parents=True,exist_ok=True)
        self.database=self.root/'warehouse.duckdb'
        self.pipeline=dlt.pipeline(pipeline_name='ops_events',pipelines_dir=str(self.root/'state'),
                                   destination=duckdb(credentials=str(self.database)),dataset_name='ops')

    def _current(self):
        if not self.database.exists():
            return {}
        with sql.connect(str(self.database),read_only=True) as db:
            exists=db.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='ops' AND table_name='events'").fetchone()[0]
            if not exists:
                return {}
            return dict(db.execute('SELECT event_id,updated_epoch FROM ops.events').fetchall())

    def run(self,rows,*,simulate_source_failure=False):
        valid,rejected=validate_rows(rows)
        current=self._current()
        stale=0
        selected=[]
        for row in valid:
            if row['updated_epoch']<current.get(row['event_id'],-1):
                stale+=1
            else:
                selected.append(row)

        @dlt.resource(name='events',primary_key='event_id',write_disposition='merge',
                      columns={'event_id':{'data_type':'text','nullable':False},
                               'updated_epoch':{'data_type':'bigint','nullable':False},
                               'amount_cents':{'data_type':'bigint'},'occurred_at':{'data_type':'timestamp'}},
                      schema_contract={'tables':'evolve','columns':'freeze','data_type':'freeze'})
        def events(cursor=dlt.sources.incremental('updated_epoch',initial_value=0,lag=86400)):
            for row in selected:
                yield row
            if simulate_source_failure:
                raise RuntimeError('injected extraction failure')

        # On a new dataset, infer the full fixed schema from validated Pydantic fields.
        resource=events()
        resource.apply_hints(columns={
            'event_id':{'data_type':'text','nullable':False},'user_id':{'data_type':'text','nullable':False},
            'session_id':{'data_type':'text','nullable':False},'channel':{'data_type':'text','nullable':False},
            'event_type':{'data_type':'text','nullable':False},'occurred_at':{'data_type':'timestamp','nullable':False},
            'updated_epoch':{'data_type':'bigint','nullable':False},'amount_cents':{'data_type':'bigint','nullable':True},
            'is_deleted':{'data_type':'bool','nullable':False}})
        info=self.pipeline.run(resource)
        if rejected:
            with (self.root/'quarantine.jsonl').open('a',encoding='utf-8') as stream:
                for row in rejected:
                    stream.write(json.dumps(row,ensure_ascii=False)+'\n')
        metrics=self.metrics() if self._current() else {'event_count':0}
        report={'input_rows':len(rows),'valid_unique_versions':len(valid),'rejected_rows':len(rejected),
                'stale_versions_ignored':stale,'load_ids':info.loads_ids,'metrics':metrics,
                'cursor':self.pipeline.state.get('sources',{}),
                'definitions':{'lookback':'24 hours on updated_epoch; older unseen records require an explicit backfill',
                               'amount':'integer cents; purchase event revenue excludes soft-deleted rows',
                               'source':'synthetic events, not production outcomes'}}
        (self.root/'latest_report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
        return report

    def metrics(self):
        with sql.connect(str(self.database),read_only=True) as db:
            rows=db.execute("""
                WITH active AS (SELECT * FROM ops.events WHERE NOT is_deleted),
                views AS (SELECT user_id,session_id, min(occurred_at) ts FROM active WHERE event_type='view' GROUP BY user_id,session_id),
                carts AS (SELECT e.user_id,e.session_id,min(e.occurred_at) ts FROM active e JOIN views v USING(user_id,session_id)
                  WHERE e.event_type='cart' AND e.occurred_at>v.ts GROUP BY e.user_id,e.session_id),
                buys AS (SELECT e.user_id,e.session_id,min(e.occurred_at) ts FROM active e JOIN carts c USING(user_id,session_id)
                  WHERE e.event_type='purchase' AND e.occurred_at>c.ts GROUP BY e.user_id,e.session_id)
                SELECT (SELECT count(*) FROM active),(SELECT count(DISTINCT user_id) FROM active),
                  (SELECT count(*) FROM views),(SELECT count(*) FROM carts),(SELECT count(*) FROM buys),
                  (SELECT coalesce(sum(amount_cents),0) FROM active WHERE event_type='purchase')
            """).fetchone()
            channels=db.execute("SELECT channel,count(*) FILTER(WHERE event_type='purchase') purchases,coalesce(sum(amount_cents) FILTER(WHERE event_type='purchase'),0) revenue_cents FROM ops.events WHERE NOT is_deleted GROUP BY channel ORDER BY channel").fetchall()
            # Data quality invariants are executable release gates.
            duplicates=db.execute('SELECT count(*)-count(DISTINCT event_id) FROM ops.events').fetchone()[0]
            if duplicates or not rows[4]<=rows[3]<=rows[2]:
                raise ValueError('Warehouse quality invariant failed')
            return dict(zip(['event_count','users','view_sessions','cart_sessions','purchase_sessions','revenue_cents'],rows),
                        channels=[dict(channel=c,purchase_events=int(p),revenue_cents=int(r)) for c,p,r in channels],
                        unique_primary_key=True,ordered_funnel=True)
