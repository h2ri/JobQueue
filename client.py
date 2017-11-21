import time
import random
import logging
import redis_queue
import redis
import dill
import docker
import os
import job
import dill

NUM_TASKS = 3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def do_something(arg1, arg2):

    #container.stop()
    print("Inside mp_worker")
    print(os.getpid())
    logger.info("Performing task with arg1=%s and arg2=%s", arg1, arg2)
    time.sleep(5)
    print("Done")

def main_client():

    # r = redis.Redis(
    #     host='localhost',
    #     port=6379
    # )
    queue_name = 'tasaks' #replace with args.queue
    #redis_conn = redis_queue.get_redis_conn(host=args.host, port=args.port, db=args.db)
    redis_conn = redis_queue.get_redis_conn()
    redis_queue_q = redis_queue.RedisQueue(queue_name, redis_conn)

    logger.info("Generating %i tasks", NUM_TASKS)

    for i in range(NUM_TASKS):
        a = job.Job(name="hari21/hello:1.0", memory={"mem" : "50M"})
        #a = job.Job('hari21/hello:1.0')
        data = dill.dumps(a)
        redis_queue_q.add(data)

if __name__ == '__main__':
    main_client()
