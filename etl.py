import os
import re
import sys
import time
import asyncclick as click
import asyncio
from benedict import benedict
from hydra import compose, initialize_config_dir
from icecream import ic
import yaml
from lib.etl_constants import TABLE_BCP_EXTRACTOR
from lib.etl_util import decrpyt_password, process_job_steps, process_template, read_batch_date_file, repl
from pathlib import Path
from loguru import logger
import pendulum
from lib.etl_context import context_dict
import inspect
import importlib

def current_function_name():
    return inspect.currentframe().f_back.f_code.co_name

class OverrideParams:
    def __init__(self):
        self.params = {}

    def set_param(self, module_name, variable_name, value):
        print(current_function_name())
        if module_name not in self.params:
            self.params[module_name] = {}
        self.params[module_name][variable_name] = value



def create_step_dicts(data):
    index_to_step = {}
    step_to_value = {}
    for i, step in enumerate(data['job_steps']):
        step_name = list(step.keys())[0]
        index_to_step[i] = step_name
        step_to_value[step_name] = step[step_name]
    return index_to_step, step_to_value






@click.command()
@click.option('--env', default='dev', help='Environment to run the job in')
@click.option('--job_id', default='fifth_job', help='Name of the job to run')
#@click.option('--override', default=["step5.output_file.quote_char='🤣'","step5.validation=False"], multiple=True, help='Override parameters in the format module_name.variable_name=value')
@click.option('--override', default=[], multiple=True, help='Override parameters in the format module_name.variable_name=value')
async def main(env, job_id, override):
    env = env.lower()
    current_path = Path.cwd()
    print(current_path)
    override_params = OverrideParams()


    #Override only works till one level , canbe enhanced
    # for param in override:
    #     module_name, variable_name_valuepair = param.split('.')
    #     variable_name = variable_name_valuepair.split('=')[1]
    #     value = variable_name_valuepair.split('=')[1]  # Extract the value after the '=' sign
    #     override_params.set_param(module_name, variable_name, value)



    #ic(override_params.params)
    # Join current_path and "config" using pathlib
    config_path = current_path.joinpath("config")
    initialize_config_dir(version_base=None, config_dir=str(config_path), job_name="demo")



    context_dict.app_cfg = compose(config_name="etl_common")
    logpath = Path(context_dict.app_cfg.logpath)
    context_dict.app_cfg.logpath = Path(context_dict.app_cfg.logpath)
    context_dict.job_id = job_id
    context_dict.env = env


    context_dict.BDATE = context_dict.BUSINESS_DATE = await read_batch_date_file(context_dict.app_cfg.batch_date_path, "YYYYMMDD")
    logfilename = f"etl_{job_id}_{context_dict.BUSINESS_DATE.format('YYYYMMDD')}.log"
    logger.add(logpath / logfilename, rotation="1 day", retention="7 days", level="DEBUG", backtrace=True, enqueue=True)

    # Example usage of loguru features
    # logger.debug("This is a debug message")
    # logger.info("This is an info message")
    # logger.warning("This is a warning message")
    # logger.error("This is an error message")
    # logger.critical("This is a critical message")


    logger.info('Reading job file')

    
    # Check folder config_path
    if config_path.exists() and config_path.is_dir():
        # Using job_id, check if there are 2 files with .yml and .yaml extensions
        yml_file = config_path / f"{job_id}.yml"
        yaml_file = config_path / f"{job_id}.yaml"
        if yml_file.exists() and yaml_file.exists():
            raise ValueError("Both .yml and .yaml files exist for the given job_id")
        elif yml_file.exists():
            job_cfg = f"{job_id}.yml"
        elif yaml_file.exists():
            job_cfg = f"{job_id}.yaml"
        else:
            raise ValueError("No .yml or .yaml file found for the given job_id")
    else:
        raise ValueError("Invalid config_path")






    job_file_path = config_path / job_cfg
    context_dict.job_cfg = benedict.from_yaml(str(job_file_path))
    context_dict.pattern = rf'{context_dict.job_cfg.group_seperator}([\w{context_dict.job_cfg.child_access_symbol}˩^><!.]+){context_dict.job_cfg.component_seperator}([\w{context_dict.job_cfg.child_access_symbol}˩^><!.]+){context_dict.job_cfg.component_seperator}([^{context_dict.job_cfg.group_seperator}]+){context_dict.job_cfg.group_seperator}'
    #ic(context_dict.pattern)
    # #ic(cfg.logpath)
    # #ic(cfg.remote.env)
    # #ic(cfg.remote.vendor.connection_string)
    # #ic(BDATE)

    #context_dict.job_index_dict,context_dict.job_steps_dict = create_step_dicts(context_dict.job_cfg)
    context_dict.job_index_dict,context_dict.job_steps_dict = process_job_steps(context_dict.job_cfg.job_steps)

    
    #context_dict.job_index_dict = job_index_dict
    for step in context_dict.job_steps_dict.values():
        step.env = env

    #ic(context_dict.job_index_dict)


    for param in override:
        config_key, override_value = param.split('=')
        if config_key in context_dict.job_cfg:
            logger.info(f"Overriding job level parameter {config_key=} with value {override_value=}")
            context_dict.job_cfg[config_key] = override_value 
        else:
            if config_key in context_dict.job_steps_dict :
                logger.info(f"Overriding job step parameter {config_key=} with value {override_value=}")
                context_dict.job_steps_dict[config_key] = override_value


    #ic(context_dict.job_steps_dict.keys())
    for job_step_index in range(len(context_dict.job_cfg.job_steps)):
        start_time = time.perf_counter()
        logger.info(f"Now processing job step {job_step_index} with step name {context_dict.job_index_dict[job_step_index]}")
        step_info = context_dict.job_steps_dict[context_dict.job_index_dict[job_step_index]]
        logger.info(f"Job Step type is {step_info['type']}")
        #table_bcp_extractor_runner(context_dict.job_index_dict[job_step_index])
        function_name = step_info['type']

        if '.' in function_name:
            module_name, function_name = function_name.split('.')
            module = importlib.import_module(f"lib.{module_name}")
        else:
            module = importlib.import_module(f"lib.etl_service")

        caller_function = getattr(module, function_name)
        await caller_function(context_dict.job_index_dict[job_step_index])
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        logger.info(f"Step {job_step_index} completed in {elapsed_time} seconds")






  

    #cfg.remote.vendor.connection_string = cfg.remote['vendor'].connection_string.replace("REPLACEME", decrpyt_password(cfg.remote.password_command))

if __name__ == "__main__":
    #asyncio.run(main())
    main()


#todo :- last char in file
#fixe lenght inside bcp
# get table info using template
# transform file