import sys
import math
import asyncio
import asyncpg
from asyncua import Server, ua
from uapg import HistoryPgSQL
import time
from datetime import datetime, timezone, UTC


async def get_postgres_version():
    try:
        conn = await asyncpg.connect(
            user='postgres', password='postmaster',
            database='opcua', host='127.0.0.1'
        )
        version = await conn.fetchval('SELECT version();')
        print(f"Postgres version: {version}")
    except Exception as e:
        print(f"Postgres connection error: {e}")
    finally:
        if 'conn' in locals() and not conn.is_closed():
            await conn.close()


async def run_opcua_server():
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")
    server.set_server_name("Sin OPC UA Server")

    server.set_security_policy(
        [
            ua.SecurityPolicyType.NoSecurity,
            ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
            ua.SecurityPolicyType.Basic256Sha256_Sign,
        ]
    )

    #S et up history storage with TimescaleDB
    history = HistoryPgSQL(
        user='postgres', password='postmaster',
        database='opcua', host='127.0.0.1'
    )
    await history.init()
    server.iserver.history_manager.set_storage(history)

    # Create address space and variable
    idx = await server.register_namespace("http://rts-iot.ru")
    dev = await server.nodes.base_object_type.add_object_type(idx, "MyDevice")
    vart = await dev.add_variable(idx, "SinVariable", 0.0)
    vart.set_modelling_rule(True)
 #   await server.nodes.objects.add_object(idx, "MyDeviceInstance", dev)
    var = await server.nodes.objects.add_variable(idx, "SinVariableInstance", 0.0)
    await var.set_writable()

    # Enable historizing for the variable
    await server.historize_node_data_change(var, period=None, count=0)
    async with server:
        print(f"OPC UA Server started. Python: {sys.version}")

        t = 0
        try:
            while True:
                value = math.sin(time.time()/10)
                # await server.write_attribute_value(
                #             var.nodeid, 
                #             ua.DataValue(
                #                 value, 
                #                 ua.VariantType.Double,
                #                 ua.StatusCode(ua.StatusCodes.Good),
                #                 datetime.now(UTC), 
                #                 datetime.now(UTC)
                #             )
                #     )
                await var.set_value(ua.Variant(value, ua.VariantType.Double))
                
                t += 1
                await asyncio.sleep(1)
        finally:
            await server.stop()
            await history.stop()


def main():
    asyncio.run(run_opcua_server())


if __name__ == "__main__":
    main()
