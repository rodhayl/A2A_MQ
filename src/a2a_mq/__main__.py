"""Publish a newly saved envelope, or retry the exact saved envelope unchanged."""
import argparse
import asyncio
import os
from pathlib import Path

from .domain import Envelope, command
from .transport import DaprPublisher


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--text")
    group.add_argument("--retry-file", type=Path)
    parser.add_argument("--save", type=Path, help="Required with --text; created exclusively before publishing")
    parser.add_argument("--dapr-url", default=os.getenv("DAPR_HTTP_ENDPOINT", "http://127.0.0.1:3500"))
    args = parser.parse_args()
    if args.text is not None:
        if args.save is None:
            parser.error("--text requires --save so ambiguous publication can be retried safely")
        envelope = command(args.text)
        with args.save.open("x", encoding="utf-8") as out:
            out.write(envelope.wire + "\n")
            out.flush()
            os.fsync(out.fileno())
    else:
        envelope = Envelope.parse(args.retry_file.read_bytes())

    async def publish():
        bus = DaprPublisher(args.dapr_url, api_token=os.getenv("DAPR_API_TOKEN", ""))
        try:
            await bus.publish(envelope)
        finally:
            await bus.close()

    asyncio.run(publish())
    print("Transport accepted messageId=" + envelope.as_dict()["messageId"])


if __name__ == "__main__":
    main()
