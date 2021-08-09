import signal
import time
from multiprocessing import Process
import logging

from twitter_worker import TwitterWorker


class Supervisor:

    def __init__(self):
        self.running = True
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)  # need
        self.workers = list()
        self.tw = list()

    def stop(self, signum, els):
        logging.warning("Supervisor stop")
        self.stop_workers()
        self.running = False

    def stop_workers(self):
        logging.warning("Supervisor    : close threads.")

        for tw in self.tw:
            tw.stop()

        for work in self.workers:
            work.close()

    def main(self):
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO,
                            datefmt="%H:%M:%S")


if __name__ == "__main__":
    # logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    w = TwitterWorker(0)
    logging.warning('start twitter worker ... connect in %s' % w.kafka_boot_time)
    for i in range(w.kafka_boot_time, 1, -1):
        time.sleep(1)
        logging.debug('%ss left' % i)

    logging.warning('start consuming')
    # e.get_publication_info("10.1109/5.7710731")

    # w.consume()

    supervisor = Supervisor()
    supervisor.main()
    number_worker = 1  # same as partitions
    total_workers = 0

    max_workers = 50 # ??
    # todo stop after issues not on close (classes)

    while supervisor.running:
        while len([w for w in supervisor.workers if w.is_alive()]) <= number_worker and max_workers > total_workers:
            total_workers += 1
            logging.warning("Main    : create and start thread %d.", total_workers)
            t = TwitterWorker(total_workers)
            supervisor.tw.append(t)
            worker = Process(target=t.consume)
            # worker = TwitterWorker(total_workers)
            worker.start()
            supervisor.workers.append(worker)
