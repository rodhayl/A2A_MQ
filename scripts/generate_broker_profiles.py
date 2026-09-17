#!/usr/bin/env python3
"""Generate role-scoped Dapr component JSON files without embedding credentials."""
import argparse
import json
from pathlib import Path
from a2a_mq.broker_profiles import assert_portability_contract, production_profiles


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", choices=("jetstream", "rabbitmq"), required=True)
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--worker", action="append", required=True)
    parser.add_argument("--observer", action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    profiles = production_profiles(broker=args.broker, tenant=args.tenant,
                                   workers=tuple(args.worker), observers=tuple(args.observer))
    for profile in profiles:
        assert_portability_contract(profile)
        (args.output / f"{profile.component_name}.json").write_text(
            json.dumps(profile.component, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__": raise SystemExit(main())
