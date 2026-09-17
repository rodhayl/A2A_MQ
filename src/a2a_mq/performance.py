"""Explicit, bounded model-free admission benchmark against a disposable A2A bridge."""
from __future__ import annotations

import argparse
import asyncio
import json
import math
from pathlib import Path
import ssl
import statistics
import time
from uuid import uuid4

import httpx

from .configuration import https_origin
from .domain import require
from .vault import FileToken


def distribution(samples):
    require(isinstance(samples,list) and samples and all(type(v) in {int,float} and math.isfinite(v) and v>=0 for v in samples),
            'INVALID_MEASUREMENTS')
    values=sorted(samples)
    return {'count':len(values),'medianMs':statistics.median(values),
            'p95Ms':values[math.ceil(.95*len(values))-1], 'maxMs':values[-1]}


async def measure(url,token_file,*,ca_file,count=100,concurrency=4):
    require(type(count) is int and 1<=count<=10000 and type(concurrency) is int and 1<=concurrency<=32,'INVALID_BENCHMARK_SIZE')
    url=https_origin(url)
    from google.protobuf.json_format import ParseDict
    from a2a.client.client import ClientConfig
    from a2a.client.client_factory import ClientFactory
    from a2a.types import a2a_pb2 as pb
    context=ssl.create_default_context(cafile=ca_file)
    latencies=[]
    ids=[]
    gate=asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(verify=context,timeout=30,trust_env=False,
                                 headers={'Authorization':'Bearer '+FileToken(token_file)()}) as http:
        response=await http.get(url+'/.well-known/agent-card.json')
        response.raise_for_status()
        card=ParseDict(response.json(),pb.AgentCard())
        client=ClientFactory(ClientConfig(streaming=False,polling=True,httpx_client=http)).create(card)
        async def one():
            async with gate:
                request=ParseDict({'message':{'messageId':str(uuid4()),'role':'ROLE_USER','parts':[{'text':'reference benchmark'}]},
                    'configuration':{'returnImmediately':True}},pb.SendMessageRequest())
                start=time.perf_counter()
                results=[value async for value in client.send_message(request)]
                elapsed=(time.perf_counter()-start)*1000
                require(len(results)==1 and results[0].HasField('task'),'ADMISSION_FAILED')
                ids.append(results[0].task.id)
                latencies.append(elapsed)
        # Bound outstanding tasks as well as requests: no unbounded gather.
        for start in range(0,count,concurrency):
            await asyncio.gather(*(one() for _ in range(min(concurrency,count-start))))
        for tid in ids:
            require((await client.get_task(pb.GetTaskRequest(id=tid))).id==tid,'ADMISSION_NOT_QUERYABLE')
    return {'metric':'durable-http-admission','milliseconds':distribution(latencies),
            'samplesMs':latencies,'concurrency':concurrency,'taskCount':len(ids),
            'businessCompletionMeasured':False,'productionSlaCertified':False}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--url',required=True)
    p.add_argument('--token-file',required=True)
    p.add_argument('--ca-file',required=True)
    p.add_argument('--count',type=int,default=100)
    p.add_argument('--concurrency',type=int,default=4)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--disposable-reference-target',action='store_true')
    args=p.parse_args()
    require(args.disposable_reference_target,'BENCHMARK_MUTATION_OPT_IN_REQUIRED')
    require(not args.output.exists(),'OUTPUT_ALREADY_EXISTS')
    report=asyncio.run(measure(args.url,args.token_file,ca_file=args.ca_file,count=args.count,concurrency=args.concurrency))
    with args.output.open('x') as stream:
        json.dump(report,stream,indent=2)
    print(json.dumps({k:v for k,v in report.items() if k!='samplesMs'},indent=2))


if __name__=='__main__':
    main()
