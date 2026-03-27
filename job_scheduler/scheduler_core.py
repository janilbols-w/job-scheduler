import multiprocessing
import time

class Scheduler:
    """Standalone scheduler process reading requests from per-GPU queues."""

    def __init__(self):
        self.gpu_queues = {}  # gpu_id -> multiprocessing.Queue
        self._shutdown_event = multiprocessing.Event()
        self.process = None

    def start(self):
        if self.process is not None and self.process.is_alive():
            return

        self.process = multiprocessing.Process(
            target=self._main_loop,
            args=(self.gpu_queues, self._shutdown_event),
            daemon=True,
        )
        self.process.start()
        print("[Scheduler] started process", self.process.pid)

    def enqueue(self, event_type: str, payload: dict):
        for gpu in payload.get('gpus', []):
            gpu_id = gpu['gpu_id']
            if gpu_id not in self.gpu_queues:
                self.gpu_queues[gpu_id] = multiprocessing.Queue()
            self.gpu_queues[gpu_id].put({
                'type': event_type,
                'job_id': payload['job_id'],
                'used_memory_mb': gpu['used_memory_mb']
            })

    def stop(self):
        if self.process is None:
            return

        for queue in self.gpu_queues.values():
            queue.put({'type': 'shutdown'})
        self._shutdown_event.set()
        self.process.join(timeout=5)

    @staticmethod
    def _main_loop(gpu_queues: dict, shutdown_event: multiprocessing.Event):
        print("[Scheduler] main loop running")

        while not shutdown_event.is_set():
            for gpu_id, queue in gpu_queues.items():
                try:
                    item = queue.get(timeout=0.1)
                    if item.get('type') == 'shutdown':
                        continue
                    print(f"[Scheduler] GPU {gpu_id}: processing {item['type']} event, job_id={item['job_id']}, used_memory_mb={item['used_memory_mb']}")
                    # TODO: Replace this with actual job scheduling, GPU allocation logic, etc.
                except Exception:
                    continue

        print("[Scheduler] main loop exiting")


def main():
    s = Scheduler()
    s.start()
    print("[Scheduler] running standalone scheduler (watching per-GPU queues)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[Scheduler] stopping via KeyboardInterrupt")
    finally:
        s.stop()


if __name__ == "__main__":
    main()
