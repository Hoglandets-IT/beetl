import concurrent.futures


class RequestThreader:
    threads: int = None
    executor: concurrent.futures.ThreadPoolExecutor = None

    def __init__(self, threads: int = 10):
        print("Starting threader...")
        self.threads = threads

    def __enter__(self):
        print("Threading the needle...")
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.threads)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        print("Waiting for threads to finish...")
        self.executor.shutdown(wait=True)

    def submit(self, func, *args, **kwargs):
        print("Submitting single thread...")
        return self.executor.submit(func, *args, **kwargs)

    def submitAndWait(self, func, kwarg_list: list):
        print("Submitting a list of threads...")
        for result in self.executor.map(lambda fn: func(**fn), kwarg_list):
            yield result
