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

def main_client():
    queue_name = 'tasaks' #replace with args.queue
    #redis_conn = redis_queue.get_redis_conn(host=args.host, port=args.port, db=args.db)
    redis_conn = redis_queue.get_redis_conn()
    redis_queue_q = redis_queue.RedisQueue(queue_name, redis_conn)
    logger.info("Generating %i tasks", NUM_TASKS)
    for i in range(NUM_TASKS):
        a = job.Job(name="hari21/hello:1.0", memory={"memory" : "8"})
        #a = job.Job('hari21/hello:1.0')
        data = dill.dumps(a)
        redis_queue_q.add(data)

if __name__ == '__main__':
    main_client()
