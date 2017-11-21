import multiprocessing
import os
import time
import redis
import docker
import args
import redis_queue
import job
import dill
import resource
import sys

the_queue = multiprocessing.Queue()


def worker_main(queue,mem_limit_mb):
    resource.setrlimit(resource.RLIMIT_STACK, ( mem_limit_mb * 1024 * 100 * 10,
     mem_limit_mb * 1024 * 100 * 10)) #TO set limit of in kb
    soft, hard = resource.getrlimit(resource.RLIMIT_STACK)
    alloted_memory = hard / 1024 / 1024
    print os.getpid(),"working"
    while True:
        item = queue.get(True)
        if item:
            time.sleep(5) # simulate a "long" operation
            job_data = dill.loads(item)
            client = docker.from_env()
            try:
                if int(job_data.memory_requirment["memory"]) <= alloted_memory:
                    container = client.containers.run(job_data.container_name, detach=True, mem_limit="50M")
                    while container.status != 'exited':
                        time.sleep(5)
                        container.reload()
                    print("Job Completed")
                else:
                    print("Memory not available to process the request")
            except docker.errors.NotFound:
                print("File Not Found")

def mp_handler(no_of_process, mem_limit):
    the_pool = multiprocessing.Pool(no_of_process, worker_main,(the_queue,mem_limit))


if __name__ == '__main__':
    queue_name = 'tasks' #replace with args.queue
    #redis_conn = redis_queue.get_redis_conn(host=args.host, port=args.port, db=args.db)
    redis_conn = redis_queue.get_redis_conn()
    redis_queue_q = redis_queue.RedisQueue(queue_name, redis_conn)
    mp_handler(int(sys.argv[1]),int(sys.argv[2]))
    while True:
        the_queue.put(redis_queue_q.remove())
