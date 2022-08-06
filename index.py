import subprocess
from time import sleep
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--timeout_second',
    help='timeout_second')

args = parser.parse_args()
TIMEOUT_SECOND = int(args.timeout_second) if args.timeout_second else 20


# This is our shell command, executed in subprocess.
p = subprocess.Popen("docker compose build && docker compose --env-file ./.env.dev up", stdout=subprocess.PIPE, shell=True)

sleep(TIMEOUT_SECOND)
p = subprocess.Popen("docker compose stop", stdout=subprocess.PIPE, shell=True)
# print(p.communicate()) # communicate 流式的pipe不停下来是不行的
