#!/usr/bin/env python

import asyncio
from nats.aio.client import Client as NATS
from stan.aio.client import Client as STAN


async def run(loop):

    nc = NATS()
    
    await nc.connect(["nats://134.209.85.215:4221", "nats://134.209.85.215:4222", "nats://134.209.85.215:4223"], loop=loop)

    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))

    # Simple publisher and async subscriber via coroutine.
    sid = await nc.subscribe("foo", cb=message_handler)

    # Stop receiving after 2 messages.
    await nc.auto_unsubscribe(sid, 2)
    await nc.publish("foo", b'Hello')
    await nc.publish("foo", b'World')
    await nc.publish("foo", b'!!!!!')

    # Terminate connection to NATS.
    await nc.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.close()
