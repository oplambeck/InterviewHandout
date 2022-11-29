
import mock_db
import uuid
from worker import worker_main
from threading import Thread
import time

"""
Owen Plambeck Take Home Assignment for MaestroQA, Completed 11/29/2022. 

In order to solve the concurrency issue presented in the files, I have implemented a locking
mechanism using the provided database operations. My implementation uses a dictionary object 
that acts as a key, in which threads can only run their workers if they have inserted their 
instance of the key into the database. Each thread keeps trying to insert their version of 
the key every retry interval, until it reaches the threshold in which it gives up. Once it is
successfully inserted into the store, the worker runs its code, then deletes its lock object from the store, 
so another thread and insert and run. If the thread crashes while it holds the object, the object 
is also deleted so a different thread can insert and complete. 

"""

def lock_is_free(lock_object, db):
    """
        Checks to see if the Lock Object is Free and Grabs it if it is


        Args:
            lock_object: A dictionary that has _id "lock_object" and uuid of the worker_hash
            db: an instance of MockDB

     """
    # First check to see if lock object is already in the db
    # If it is, return False as the lock is not free.
    if db.find_one(lock_object):
        return False
    else:
        # If it is not present in the database attempt an insert of the lock object.
        # If successful return true, if fails with a DuplicateKeyError I return false
        # but catch the error.
        try:
            db.insert_one(lock_object)
            return True
        except Exception as e:
            if str(e) == "DuplicateKeyError":
                return False
            else:
                raise e

def attempt_run_worker(worker_hash, give_up_after, db, retry_interval):
    """
        CHANGE MY IMPLEMENTATION, BUT NOT FUNCTION SIGNATURE

        Run the worker from worker.py by calling worker_main

        Args:
            worker_hash: a random string we will use as an id for the running worker
            give_up_after: if the worker has not run after this many seconds, give up
            db: an instance of MockDB
            retry_interval: continually poll the locking system after this many seconds
                            until the lock is free, unless we have been trying for more
                            than give_up_after seconds
    """

    # Note the start time of this thread, so we can give up after the passed give_up_after.
    start_time = time.perf_counter()

    # A object that we will enter into the dict to implement a locking system, do not need to pass
    # the worker hash as uuid, but it would allow to track with thread is writing which line if needed.
    lock_object = {'_id': 'LOCK', 'uuid': worker_hash}

    # While the lock object exists in the store, we want to keep checking every retry_interval
    # Also check to see if the time that has elapsed from the start of the thread
    # has exceeded the gve up after, if it has, I chose to throw TimeOutError, but you could
    # also have the thread fail silently if you wanted.
    while not lock_is_free(lock_object, db):
        curr_time = time.perf_counter()
        if (curr_time - start_time) > give_up_after:
            raise Exception("TimeOutError")
        time.sleep(retry_interval)

    # If we are able to successfully enter the lock object into the store
    # we want to try and run the worker, if successful, we remove the lock_object
    # from the store. If the thread crashed, we want to catch the error and delete
    # the lock object so another thread can grab it. If the worker errors with
    # a different exception we want to raise it.
    try:
        worker_main(worker_hash, db)
        db.delete_one(lock_object)
    except Exception as e:
        if str(e) == "Crash":
            db.delete_one(lock_object)
        else:
            raise e


if __name__ == "__main__":
    """
        DO NOT MODIFY

        Main function that runs the worker five times, each on a new thread
        We have provided hard-coded values for how often the worker should retry
        grabbing lock and when it should give up. Use these as you see fit, but
        you should not need to change them
    """

    db = mock_db.DB()
    threads = []
    for _ in range(25):
        t = Thread(target=attempt_run_worker, args=(uuid.uuid1(), 2000, db, 0.1))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
