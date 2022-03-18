import json
import os
import pathlib
import pickle
import time
import traceback
from datetime import datetime

import pandas as pd
import requests
from loguru import logger

try:
    import pymmm
except ImportError:
    logger.error("pymmm library not found")
from pymmm.optimization import optz_engine_wrapper
from pymmm.utils import optimization_utils as veh_mapping

# Cloud production DB
API_GATEWAY_SERVER = os.getenv("API_GATEWAY_SERVER")
API_GATEWAY_PORT = os.getenv("API_GATEWAY_PORT")

logger.info(f"API_GATEWAY_SERVER: {API_GATEWAY_SERVER}")
logger.info(f"API_GATEWAY_PORT: {API_GATEWAY_PORT}")

import typer

app = typer.Typer()


@app.command(payload="")
def run_model_runner_wrapper(payload: typer.FileText = typer.Option(...)):
    payload = json.load(payload)
    # current_dir = pathlib.Path.cwd()
    # current_dir = pathlib.Path.cwd() / "app"
    current_dir = pathlib.Path.cwd() / "app" / "data"  # cwd -> /usr/src
    data_mount_path = "data"

    country = payload.get("country")
    model_id = payload.get("model_id")

    model_config_path = pathlib.Path(
        os.path.join(
            current_dir,
            data_mount_path,
            f"{country}",
            "model_config",
            f"{country}_model_config.json",
        )
    )
    with open(model_config_path) as json_file:
        payload.update(json.load(json_file))
    roi_curve_dict_df = pymmm.run_model_entry_point(payload)

    roi_curves_pkl_path = pathlib.Path(
        os.path.join(
            current_dir,
            data_mount_path,
            f"{country}",
            "model_results",
            f"{country}_{model_id}_roi_curves.pkl",
        )
    )
    # NOTE: Saving ROI curves file here as pkl file obj.
    with roi_curves_pkl_path.open("wb") as f:
        pickle.dump(roi_curve_dict_df, f)

    job_data = roi_curves_pkl_path.relative_to(
        pathlib.Path(current_dir) / pathlib.Path(data_mount_path)
    ).as_posix()
    job_status = "successful"

    return model_id, job_status, job_data, roi_curves_pkl_path


# http://web:5000/users/
# def model_runner(payload, model_id):
#     # Modelling notebook code should go here.
#     try:
#         # country = payload.get('country')
#         # with open("app/data/{country}/model_config/{country}_model_config.json") as json_file:
#         #     payload = json.load(json_file)
#         print("Final Payload for Model run")
#         print(payload)
#         job_data = pymmm.run_model(**payload)
#         job_status = "successful"
#         job_data = None  # this is dummy this will be replaced once the real response is in-place.
#     except Exception as e:
#         job_data = None
#         job_status = "failed"
#         logger.error(f"Model run failed: {e}")
# return model_id, job_status, job_data


def translate_vehicles(job_data):
    with open(job_data, "r") as f:
        output_json = json.load(f)
    logger.info("translate_vehicles() function triggered")
    with open(f"sampletest_{datetime.now()}.json", "w") as outfile:
        outfile.write(json.dumps(output_json))

    if isinstance(output_json, dict):
        logger.info("Multiple")
        country = output_json["sim_response"][0]["country"]
        brand = output_json["sim_response"][0]["brand"]
    else:
        logger.info("Single dict")
        country = output_json[0]["country"]
        brand = output_json[0]["brand"]

    current_dir = pathlib.Path.cwd() / "app" / "data"  # cwd -> /usr/src
    data_mount_path = "data"

    veh_master_list_mapper_full_path = pathlib.Path(
        os.path.join(
            current_dir,
            data_mount_path,
            f"{country}",
            "optimization_input",
            f"master_mapping.csv",
        )
    )

    veh_master_list_mapper_full_path = (
        veh_master_list_mapper_full_path.as_posix()
    )
    mapper_dict = veh_mapping.fetch_mapper_dict(
        country, brand, veh_master_list_mapper_full_path
    )
    if isinstance(output_json, dict):
        for module in output_json:
            unmapped_df = pd.DataFrame.from_dict(output_json[module])
            mapped_roi_df = veh_mapping.map_roi_veh(
                unmapped_df, mapper_dict, reverse_map=True
            )
            output_json[module] = mapped_roi_df.to_dict(orient="records")
    else:
        unmapped_df = pd.DataFrame.from_dict(output_json)
        mapped_roi_df = veh_mapping.map_roi_veh(
            unmapped_df, mapper_dict, reverse_map=True
        )
        output_json = mapped_roi_df.to_dict(orient="records")
    job_data = job_data.as_posix()
    file_name, extension = job_data.split(".", 1)
    job_data = file_name + "_translated." + extension

    with open(job_data, "w") as outfile:
        outfile.write(json.dumps(output_json))
    job_data = pathlib.Path(job_data)
    return job_data


# @app.command(payload="")
# def optimization_runner(payload: typer.FileText = typer.Option(...)):
def optimization_runner(payload, optimization_id):
    """
    _Func where it triggers main wrapper function optz_engine_wrapper().
    Before that it does some modification to the payload, removes non-required
    variables from the payload dictionary.
    """
    try:
        # NOTE: Remove non-required variables: From payload dictionary
        logger.info("Entered optimization_runner() function. ")
        logger.info("---------------------------------------------")
        optimization_run_dict = payload
        # -----

        # NOTE: Temporary fix: Discuss & remove this
        optimization_run_dict["brand_level_constraints"]["planning_year"] = 2019
        optimization_run_dict["brand_level_constraints"]["start_date"] = (
            optimization_run_dict["brand_level_constraints"]["start_date"]
            .replace("2021", "2019")
            .replace("2022", "2019")
        )
        optimization_run_dict["brand_level_constraints"]["end_date"] = (
            optimization_run_dict["brand_level_constraints"]["end_date"]
            .replace("2021", "2019")
            .replace("2022", "2019")
        )
        brand_level_financials_new = {}
        for k, v in optimization_run_dict["brand_level_financials"].items():
            brand_level_financials_new[k] = {}
            for k1, v1 in v.items():
                brand_level_financials_new[k]["2019"] = v1
        optimization_run_dict.pop("brand_level_financials")
        optimization_run_dict[
            "brand_level_financials"
        ] = brand_level_financials_new
        # -----

        # NOTE: Checks for local execution or prod execution
        if optimization_run_dict["local_exec_environment"]:
            logger.info(
                f"Local execution is turned {optimization_run_dict['local_exec_environment']}"
            )
            job_data = "local/execution/path"
        else:
            # current_dir = pathlib.Path.cwd()
            current_dir = pathlib.Path("/usr/src/app/data")
            data_mount_path = "data"
            file_type = "pkl"
            country = optimization_run_dict["brand_level_constraints"][
                "country"
            ].lower()
            output_dir = (
                current_dir
                / data_mount_path
                / f"{country}"
                / "optimization_results"
            )
            logger.info(
                f"Local execution is turned {optimization_run_dict['local_exec_environment']}"
            )
            logger.info("optz_engine_wrapper() function is triggered.")
            # logger.info(json.dumps(optimization_run_dict, indent=4))
            job_data = optz_engine_wrapper(optimization_run_dict, output_dir)
            logger.info("optz_engine_wrapper()  output")
            logger.info(job_data)
            if optimization_run_dict["translate_vehicles"]:
                job_data = translate_vehicles(job_data)
                logger.info(type(job_data))
                logger.info("The vehicles has been translated")
            job_data = job_data.relative_to(
                current_dir / data_mount_path
            ).as_posix()
        del optimization_run_dict["local_exec_environment"]
        job_status = "successful"
        # -----
    except Exception as e:
        job_data = None
        job_status = "failed"
        logger.error(f"Optimization run failed: {e}")
        logger.error(f"{traceback.format_exc()}")
    return optimization_id, job_status, job_data


def ping_pong_check():
    # session = requests.Session()
    # session.trust_env = False
    url = f"{API_GATEWAY_SERVER}:{API_GATEWAY_PORT}/ping/"
    logger.info(f"ping pong url: {url}")
    r = requests.get(url)
    return r


def update_job_status(job_id, job_status, job_data):
    payload = {
        "job_id": job_id,
        "job_status": job_status,
        "job_data": job_data,
    }
    url = f"{API_GATEWAY_SERVER}:{API_GATEWAY_PORT}/job_status/"
    logger.info(f"update_job_status url: {url}")
    r = requests.put(url, json=payload)
    return r


def create_optimization_results(
    optz_id, start_time, fail_time, end_time, job_data
):
    logger.info("create_optimization_results() function triggered")
    payload = {
        "start_time": start_time,
        "fail_time": fail_time,
        "end_time": end_time,
        "job_data": job_data,
        "optz_id": optz_id,
    }
    if payload["fail_time"] is None:
        payload["fail_time"] = "1990-11-30 01:01:01"
    elif not isinstance(payload["fail_time"], str):
        payload["fail_time"] = payload["fail_time"].strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    if payload["end_time"] is None:
        payload["end_time"] = "1990-11-30 01:01:01"
    elif not isinstance(payload["end_time"], str):
        payload["end_time"] = payload["end_time"].strftime("%Y-%m-%d %H:%M:%S")

    if not isinstance(payload["start_time"], str):
        payload["start_time"] = payload["start_time"].strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    payload = {"payload": payload}
    logger.info("Before")
    logger.info(payload)
    url = (
        f"{API_GATEWAY_SERVER}:{API_GATEWAY_PORT}/create_optimization_results/"
    )
    logger.info(f"create_optimization_results url: {url}")
    r = requests.post(url, json=payload)
    return r


def update_job_result(job_id, job_data):
    payload = {"job_id": job_id, "job_data": job_data}
    url = f"{API_GATEWAY_SERVER}:{API_GATEWAY_PORT}/job_results/"
    logger.info(f"update_job_result url: {url}")
    r = requests.put(url, json=payload)
    return r
