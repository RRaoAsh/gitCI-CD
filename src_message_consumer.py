import os
import asyncio
from azure.servicebus.aio import ServiceBusClient


async def main():
    print("started receiving message")
    print("=========================")
    servicebus_client = ServiceBusClient.from_connection_string(conn_str="Endpoint=sb://kedabus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=G+LNTMK5l67/ckjIVw/k+SilZ/MNxxcpIZFzicyCh7c=")

    print("started receiving message") 
    async with servicebus_client:
        receiver = servicebus_client.get_queue_receiver(queue_name="kedaservicebus")
        async with receiver:
            received_msgs = await receiver.receive_messages(max_message_count=10, max_wait_time=5)
            for msg in received_msgs:
                print(str(msg))
                await receiver.complete_message(msg)
    print("=========================")
    print("completed receiving message")
