import subprocess
from time import sleep
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--TIMEOUT_SECOND',
    help='TIMEOUT_SECOND')

args = parser.parse_args()
TIMEOUT_SECOND = args.TIMEOUT_SECOND if args.TIMEOUT_SECOND else 20


# This is our shell command, executed in subprocess.
p = subprocess.Popen("docker compose build && docker compose --env-file ./.env.dev up", stdout=subprocess.PIPE, shell=True)

sleep(TIMEOUT_SECOND)
p = subprocess.Popen("docker compose stop", stdout=subprocess.PIPE, shell=True)
# print(p.communicate()) # communicate 流式的pipe不停下来是不行的
