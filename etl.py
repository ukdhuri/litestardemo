import os
import re
import sys
import asyncclick as click
import asyncio
from benedict import benedict
from hydra import compose, initialize_config_dir
from icecream import ic
from lib.etl_constants import TABLE_BCP_EXTRACTOR
from lib.etl_service import table_bcp_extractor_runner
from lib.etl_util import decrpyt_password, process_job_steps, process_template, read_batch_date_file, repl
from pathlib import Path
from loguru import logger
import pendulum
from lib.etl_context import context_dict
import inspect

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

@logger.catch(onerror=lambda _: sys.exit(1))
@click.command()
@click.option('--env', default='dev', help='Environment to run the job in')
@click.option('--job_cfg', default='first_job.yml', help='Name of the job to run')
@click.option('--override', multiple=True, help='Override parameters in the format module_name.variable_name=value')
async def main(env, job_cfg, override):
    env = env.lower()
    current_path = Path.cwd()
    print(current_path)
    override_params = OverrideParams()


    #Override only works till one level , canbe enhanced
    for param in override:
        module_name, variable_name_valuepair = param.split('.')
        variable_name = variable_name_valuepair.split('=')[1]
        value = variable_name_valuepair.split('=')[1]  # Extract the value after the '=' sign
        override_params.set_param(module_name, variable_name, value)


    ic(override_params.params)
    # Join current_path and "config" using pathlib
    config_path = current_path.joinpath("config")
    initialize_config_dir(version_base=None, config_dir=str(config_path), job_name="demo")
    context_dict.app_cfg = compose(config_name="etl_common")
    logpath = Path(context_dict.app_cfg.logpath)
    context_dict.app_cfg.logpath = Path(context_dict.app_cfg.logpath)
    BDATE = BUSINESS_DATE = await read_batch_date_file(context_dict.app_cfg.batch_date_path, "YYYYMMDD")
    logfilename = f"etl_{job_cfg}_{BUSINESS_DATE.format('YYYYMMDD')}.log"
    logger.add(logpath / logfilename, rotation="1 day", retention="7 days", level="DEBUG", backtrace=True, enqueue=True)

    # Example usage of loguru features
    # logger.debug("This is a debug message")
    # logger.info("This is an info message")
    # logger.warning("This is a warning message")
    # logger.error("This is an error message")
    # logger.critical("This is a critical message")


    logger.info('Reading job file')
    job_file_path = config_path / job_cfg
    logger.info(ic(job_file_path))
    context_dict.job_config = benedict.from_yaml(str(job_file_path))
    context_dict.pattern = rf'{context_dict.job_config.group_seperator}(\w+){context_dict.job_config.component_seperator}(\w+){context_dict.job_config.component_seperator}([^{context_dict.job_config.group_seperator}]+){context_dict.job_config.group_seperator}'

    # ic(cfg.logpath)
    # ic(cfg.remote.env)
    # ic(cfg.remote.vendor.connection_string)
    # ic(BDATE)
 
    job_index_dict,job_steps_dict = process_job_steps(context_dict.job_config.job_steps)
    context_dict.job_index_dict = job_index_dict
    context_dict.job_steps_dict = job_steps_dict
    ic(job_index_dict)


    ic(job_steps_dict.keys())
    for job_step_index in range(len(context_dict.job_config.job_steps)):
        logger.info(f"Now processing job step {job_step_index} with step name {job_index_dict[job_step_index]}")
        step_info = job_steps_dict[job_index_dict[job_step_index]]
        if step_info['type'] == TABLE_BCP_EXTRACTOR:
            logger.info(f"Job Step type is {step_info['type']}")
            table_bcp_extractor_runner(job_index_dict[job_step_index],env)





  

    #cfg.remote.vendor.connection_string = cfg.remote['vendor'].connection_string.replace("REPLACEME", decrpyt_password(cfg.remote.password_command))

if __name__ == "__main__":
    asyncio.run(main())
