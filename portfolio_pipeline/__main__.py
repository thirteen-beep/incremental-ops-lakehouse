import argparse
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
from .pipeline import EventPipeline


def demo_batches():
    rows=[]
    base=datetime(2026,11,1,8,tzinfo=timezone.utc)
    for i in range(200):
        for offset,event in [(0,'view'),(1,'cart'),(2,'purchase')]:
            if event=='cart' and i%2:continue
            if event=='purchase' and i%4:continue
            occurred=base+timedelta(minutes=i*3+offset)
            rows.append(dict(event_id=f'e-{i}-{event}',user_id=f'user-{i//2}',session_id=f's-{i}',
                             channel='paid' if i%3 else 'organic',event_type=event,
                             occurred_at=occurred.isoformat(),updated_at=occurred.isoformat(),
                             amount_cents=1299+i if event=='purchase' else None,is_deleted=False))
    purchase=next(r for r in rows if r['event_id']=='e-0-purchase')
    correction={**purchase,'amount_cents':999,'updated_at':(base+timedelta(days=1)).isoformat()}
    return rows,[correction]


def main():
    parser=argparse.ArgumentParser(description='Contracted incremental events into DuckDB using dlt')
    parser.add_argument('--root',type=Path,default=Path('.data/demo'))
    parser.add_argument('--input',type=Path)
    args=parser.parse_args()
    pipe=EventPipeline(args.root)
    if args.input:
        report=pipe.run(json.loads(args.input.read_text(encoding='utf-8')))
    else:
        first,correction=demo_batches()
        pipe.run(first)
        before=pipe.metrics()
        pipe.run(first)
        replay=pipe.metrics()
        report=pipe.run(correction)
        report['replay_idempotent']=before==replay
    print(json.dumps(report,ensure_ascii=False,indent=2,default=str))


if __name__=='__main__':main()
