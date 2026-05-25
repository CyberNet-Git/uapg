#!/usr/bin/env python3
"""
Simple OPC UA client for reading UAPG HistoryMetrics nodes.

Example:
    python tools/opcua_metrics_client.py opc.tcp://localhost:4840/freeopcua/server/
    python tools/opcua_metrics_client.py opc.tcp://localhost:4840 -u user -p password
"""

import argparse
import asyncio
import getpass
from typing import Any, Dict, Iterable, Optional

from asyncua import Client


async def _get_child_or_none(parent: Any, browse_path: Iterable[str]) -> Optional[Any]:
    try:
        return await parent.get_child(list(browse_path))
    except Exception:
        return None


async def _find_metrics_object(client: Client, namespace_index: int) -> Any:
    server_node = getattr(client.nodes, "server", None)
    if server_node is None:
        server_node = await client.nodes.objects.get_child(["0:Server"])

    history = await _get_child_or_none(server_node, [f"{namespace_index}:History"])
    if history is None:
        raise RuntimeError(f"History object not found under Server for namespace {namespace_index}")

    metrics = await _get_child_or_none(history, [f"{namespace_index}:HistoryMetrics"])
    if metrics is None:
        raise RuntimeError(
            f"HistoryMetrics object not found under History for namespace {namespace_index}"
        )

    return metrics


async def _read_metric_nodes(metrics_obj: Any) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    children = await metrics_obj.get_children()
    for child in children:
        browse_name = await child.read_browse_name()
        name = browse_name.Name
        try:
            result[name] = await child.read_value()
        except Exception as exc:
            result[name] = f"<read error: {exc}>"
    return dict(sorted(result.items()))


def _print_metrics(metrics: Dict[str, Any]) -> None:
    for name, value in metrics.items():
        print(f"{name}: {value}")


async def read_metrics(
    endpoint: str,
    namespace_index: int,
    interval: float,
    username: Optional[str],
    password: Optional[str],
) -> None:
    client = Client(url=endpoint)
    if username:
        client.set_user(username)
        client.set_password(password or "")

    async with client:
        metrics_obj = await _find_metrics_object(client, namespace_index)

        while True:
            metrics = await _read_metric_nodes(metrics_obj)
            _print_metrics(metrics)

            if interval <= 0:
                return

            print()
            await asyncio.sleep(interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read UAPG HistoryMetrics from an OPC UA server."
    )
    parser.add_argument("endpoint", help="OPC UA endpoint URL, e.g. opc.tcp://localhost:4840")
    parser.add_argument(
        "--namespace-index",
        "-n",
        type=int,
        default=2,
        help="Namespace index where HistoryMetrics were exposed (default: 2).",
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=float,
        default=0.0,
        help="Polling interval in seconds. 0 reads once and exits (default: 0).",
    )
    parser.add_argument(
        "--username",
        "-u",
        help="OPC UA username. If omitted, the client uses anonymous auth.",
    )
    parser.add_argument(
        "--password",
        "-p",
        help="OPC UA password. If username is set and password is omitted, prompt securely.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    password = args.password
    if args.username and password is None:
        password = getpass.getpass("OPC UA password: ")
    asyncio.run(
        read_metrics(
            args.endpoint,
            args.namespace_index,
            args.interval,
            args.username,
            password,
        )
    )


if __name__ == "__main__":
    main()
