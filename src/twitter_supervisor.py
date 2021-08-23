import signal
import threading
import time
from multiprocessing import Process, Value
import logging

from twitter_worker import TwitterWorker


class Supervisor:
    """supervisor class
    """

    def __init__(self):
        """init"""
        self.running = True
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)  # need
        self.workers = list()
        self.tw = list()

    def stop(self, signum, els):
        """stop"""
        logging.warning("Supervisor stop")
        self.stop_workers()
        self.running = False

    def stop_workers(self):
        """stop worker"""
        logging.warning("Supervisor    : close threads.")

        for tw in self.tw:
            tw.stop()

        for work in self.workers:
            work.close()

    def main(self):
        """setup logging """
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO,
                            datefmt="%H:%M:%S")

# todo util
def throughput_statistics(v, time_delta):
    """show and setup in own thread repeatedly how many events are processed

    Arguments:
        v: the value
        time_delta: time delta we wan't to monitor
    """
    logging.warning("THROUGHPUT: %d / %d" % (v.value, time_delta))
    with v.get_lock():
        v.value = 0

    threading.Timer(counter_time, throughput_statistics, args=[v, time_delta]).start()


if __name__ == "__main__":
    # logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    # w = TwitterWorker(0, counter)
    # logging.warning('start twitter worker ... connect in %s' % w.kafka_boot_time)
    # for i in range(w.kafka_boot_time, 1, -1):
    #     time.sleep(1)
    #     logging.debug('%ss left' % i)
    # time.sleep(10)
    # logging.warning('start consuming')
    # e.get_publication_info("10.1109/5.7710731")

    # w.consume()

    supervisor = Supervisor()
    supervisor.main()
    number_worker = 1  # same as partitions
    total_workers = 0

    max_workers = 50  # ??
    # todo stop after issues not on close (classes)

    counter = Value('i', 0)
    counter_time = 10
    threading.Timer(counter_time, throughput_statistics, args=[counter, counter_time]).start()

    while supervisor.running:
        while len([w for w in supervisor.workers if w.is_alive()]) <= number_worker and max_workers > total_workers:
            total_workers += 1
            logging.warning("Main    : create and start thread %d.", total_workers)
            t = TwitterWorker(total_workers, counter)
            supervisor.tw.append(t)
            worker = Process(target=t.consume)
            # worker = TwitterWorker(total_workers)
            worker.start()
            supervisor.workers.append(worker)
            time.sleep(0.5)
