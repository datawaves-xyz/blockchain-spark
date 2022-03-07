import os
import logging
import subprocess as sub

from typing import AnyStr


def get_resource_path(groups: [str], file_name: str) -> str:
    test_root_dir = os.path.dirname(__file__)
    fixture_file_name = os.path.join(test_root_dir, 'resources', *groups, file_name)

    if not os.path.exists(fixture_file_name):
        raise ValueError('File does not exist: ' + fixture_file_name)

    return fixture_file_name


def read_resource(groups: [str], file_name: str) -> AnyStr:
    fixture_file_name = get_resource_path(groups, file_name)

    with open(fixture_file_name, encoding="utf-8") as file_handle:
        return file_handle.read()


def run_cmd(cmd):
    """Execute the command and return the output if successful. If
    unsuccessful, print the failed command and its output.
    """
    try:
        out = sub.check_output(cmd, shell=True, stderr=sub.STDOUT)
        return out
    except sub.CalledProcessError as err:
        logging.error('The failed test setup command was [%s].' % err.cmd)
        logging.error('The output of the command was [%s]' % err.output)
        raise


# Dynamically load project root dir and jars
project_root_dir = os.path.dirname(__file__) + '/..'
jars = run_cmd(f"ls {project_root_dir}/java/target/blockchain-spark*jar-with-dependencies.jar") \
    .decode('utf-8').split('\n')[0]

# Set environment variables for Spark submit command
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars %s pyspark-shell" % jars

os.environ["SPARK_CONF_DIR"] = f"{project_root_dir}/test/resources/conf"

