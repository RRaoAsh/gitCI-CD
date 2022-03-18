import os
import pathlib
from datetime import datetime

from azure.servicebus import AutoLockRenewer, ServiceBusClient
from loguru import logger

# NOTE: While building from my local `data` folder is getting attached to the docker image.
# NOTE: remove the data folder and build the image and push this to ACR and check with data folder mouned via deploy.yaml.
try:
    from runner_utils import (
        create_optimization_results,
        optimization_runner,
        run_model_runner_wrapper,
        update_job_result,
        update_job_status,
    )
except Exception as e:
    from app.runner_utils import (
        create_optimization_results,
        optimization_runner,
        update_job_result,
        update_job_status,
    )

# CONNECTION_STR = os.environ.get("CONNECTION_STR")
# QUEUE_NAME = os.environ.get("QUEUE_NAME")

# CONNECTION_STR = "Endpoint=sb://abi-mt-mroi-gb-devsb.servicebus.windows.net/;SharedAccessKeyName=abi-mt-mmm-gb-dev-q-policy;SharedAccessKey=w0Kxx6emu632m7cm2Pe/XFlnNQRydp/fLigPHDH4hVs=;EntityPath=abi-mt-mmm-gb-dev-q"
# QUEUE_NAME = "abi-mt-mmm-gb-dev-q"
CONNECTION_STR = os.getenv("CONNECTION_STR")
QUEUE_NAME = os.getenv("QUEUE_NAME")

logger.info(f"CONNECTION_STR: {CONNECTION_STR}")
logger.info(f"QUEUE_NAME: {QUEUE_NAME}")


def decode_msg(msg):
    msg_dict = eval(str(msg))
    return msg_dict


def job_runner():
    logger.info("Logging from job_runner.")
    servicebus_client = ServiceBusClient.from_connection_string(
        conn_str=CONNECTION_STR
    )
    with servicebus_client:
        renewer = AutoLockRenewer()
        with servicebus_client.get_queue_receiver(
            queue_name=QUEUE_NAME, prefetch_count=0
        ) as receiver:
            received_msgs = receiver.receive_messages(
                max_message_count=1, max_wait_time=30
            )
            if len(received_msgs) == 0:
                logger.warning("There are no messages in the QUEUE.")
                return
            logger.info("Received msgs.")
            logger.info(received_msgs)
            logger.info(type(received_msgs))
            for msg in received_msgs:
                # NOTE: max_lock_renewal_duration number should be same as activeDeadlineSeconds
                # NOTE: mountPath this is a new path.
                logger.info("Before Register")
                renewer.register(receiver, msg, max_lock_renewal_duration=21600)
                logger.info("Register messages into AutoLockRenewer done.")
                logger.info("Received msg from SB.")
                msg_dict = decode_msg(msg)
                logger.info("msg_dict.")
                logger.info(msg_dict["func"])
                logger.info(type(msg_dict["func"]))
                msg_payload = msg_dict["payload"]  # {"country":country}
                logger.info("Message")
                if msg_dict["func"] == "model":
                    model_id = msg_dict["model_id"]
                    msg_payload["model_id"] = model_id
                    logger.info("Running model training job.")
                    status_msg = {
                        "job_id": model_id,
                        "job_status": "running",
                        "job_data": "None",
                    }
                    update_job_status(**status_msg)

                    country = msg_payload["country"]
                    msg_payload["data_folder"] = "/usr/src/app/data/data"
                    msg_payload["data_dir"] = pathlib.Path("data")
                    msg_payload["raw_data_folder"] = "raw"
                    msg_payload["processed_data_folder"] = pathlib.Path(
                        f"{country}/processed"
                    )
                    msg_payload["cached_data_folder"] = pathlib.Path(
                        "__cached__"
                    )

                    (
                        model_id,
                        job_status,
                        job_data,
                        model_results_path,
                    ) = run_model_runner_wrapper(msg_payload)
                    status_msg = {
                        "job_id": model_id,
                        "job_status": job_status,
                        "job_data": "None",
                    }
                    update_job_status(**status_msg)
                    result_msg = {"job_id": model_id, "job_data": job_data}
                    update_job_result(**result_msg)
                    logger.info("Executed model training job.")
                elif msg_dict["func"] == "optimization":
                    logger.info("Running optimization job.")
                    optimization_id = msg_dict["optimization_id"]
                    status_msg = {
                        "job_id": optimization_id,
                        "job_status": "running",
                        "job_data": "None",
                    }
                    logger.info("Optimization status - Running. ")
                    logger.info("---------------------------------------------")
                    update_job_status(**status_msg)
                    logger.info(
                        "Optimization status - Running updated successfully. "
                    )
                    logger.info("---------------------------------------------")

                    try:
                        start_time = datetime.now()
                        fail_time = None
                        (
                            optimization_id,
                            job_status,
                            job_data,
                        ) = optimization_runner(msg_payload, optimization_id)
                        logger.info(
                            "Function optimization_runner()(optz_engine_wrapper()) Output. "
                        )
                        logger.info(
                            optimization_id,
                            job_status,
                            job_data,
                        )

                    except Exception as e:
                        fail_time = datetime.now()
                    end_time = datetime.now()
                    if fail_time:
                        end_time = None
                    optimization_results_payload = {
                        "optz_id": optimization_id,
                        "start_time": start_time,
                        "fail_time": fail_time,
                        "end_time": end_time,
                        "job_data": job_data,
                    }
                    logger.info(
                        "Function create_optimization_results() triggered."
                    )
                    logger.info("---------------------------------------------")
                    create_optimization_results(**optimization_results_payload)
                    logger.info("Optz updating successful status. ")
                    logger.info("---------------------------------------------")
                    logger.info(optimization_id, job_status, job_data)
                    logger.info("---------------------------------------")
                    status_msg = {
                        "job_id": optimization_id,
                        "job_status": job_status,
                        "job_data": job_data,
                    }
                    update_job_status(**status_msg)
                    logger.info("Optz updating successful status done. ")
                    result_msg = {
                        "job_id": optimization_id,
                        "job_data": job_data,
                    }
                    logger.info("optimization_runner updating job result")
                    update_job_result(**result_msg)
                    logger.info(
                        "optimization_runner completion update job result done"
                    )
                    logger.info("Executed optimization job.")
                receiver.complete_message(msg)
            logger.info("Complete messages.")
        renewer.close()


if __name__ == "__main__":
    job_runner()
