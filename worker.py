import multiprocessing
import os
import time
import redis
import docker
import args
import redis_queue
import job
import dill

the_queue = multiprocessing.Queue()

def worker_main(queue):
    print os.getpid(),"working"
    while True:
        item = queue.get(True)
        if item:
            print os.getpid(), "got", item
            time.sleep(5) # simulate a "long" operation
            job_data = dill.loads(item)
            print(job_data)
            client = docker.from_env()
            try:
                container = client.containers.run(job_data.container_name, detach=True, mem_limit="50M")
                while container.status != 'exited':
                    time.sleep(5)
                    container.reload()
                    print("Inside contianer")
                print("Done")
            except docker.errors.NotFound:
                print("File Not Found")

def mp_handler():
    the_pool = multiprocessing.Pool(3, worker_main,(the_queue,))


if __name__ == '__main__':
    queue_name = 'tasks' #replace with args.queue
    #redis_conn = redis_queue.get_redis_conn(host=args.host, port=args.port, db=args.db)
    redis_conn = redis_queue.get_redis_conn()
    redis_queue_q = redis_queue.RedisQueue(queue_name, redis_conn)
    mp_handler()
    while True:
        the_queue.put(redis_queue_q.remove())
