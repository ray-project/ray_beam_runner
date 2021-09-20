from typing import TypeVar, Iterable, Any

import ray
from ray.data.impl.compute import ComputeStrategy
from ray.types import ObjectRef
from ray.data.block import Block
from ray.data.impl.block_list import BlockList
from ray.data.impl.progress_bar import ProgressBar

T = TypeVar("T")
U = TypeVar("U")

# A class type that implements __call__.
CallableClass = type


class CustomActorPool(ComputeStrategy):
    def __init__(self, worker_cls):
        self.workers = []
        self.worker_cls = worker_cls

    def __del__(self):
        for w in self.workers:
            w.__ray_terminate__.remote()

    def apply(self, fn: Any, remote_args: dict,
              blocks: Iterable[Block]) -> Iterable[ObjectRef[Block]]:

        map_bar = ProgressBar("Map Progress", total=len(blocks))

        if not remote_args:
            remote_args["num_cpus"] = 1
        BlockWorker = ray.remote(**remote_args)(self.worker_cls)

        self.workers = [BlockWorker.remote()]
        metadata_mapping = {}
        tasks = {w.ready.remote(): w for w in self.workers}
        ready_workers = set()
        blocks_in = [(b, m) for (b, m) in zip(blocks, blocks.get_metadata())]
        blocks_out = []

        while len(blocks_out) < len(blocks):
            ready, _ = ray.wait(
                list(tasks), timeout=0.01, num_returns=1, fetch_local=False)
            if not ready:
                if len(ready_workers) / len(self.workers) > 0.8:
                    w = BlockWorker.remote()
                    self.workers.append(w)
                    tasks[w.ready.remote()] = w
                    map_bar.set_description(
                        "Map Progress ({} actors {} pending)".format(
                            len(ready_workers),
                            len(self.workers) - len(ready_workers)))
                continue

            [obj_id] = ready
            worker = tasks[obj_id]
            del tasks[obj_id]

            # Process task result.
            if worker in ready_workers:
                blocks_out.append(obj_id)
                map_bar.update(1)
            else:
                ready_workers.add(worker)

            # Schedule a new task.
            if blocks_in:
                block_ref, meta_ref = worker.process_block.remote(
                    *blocks_in.pop())
                metadata_mapping[block_ref] = meta_ref
                tasks[block_ref] = worker

        new_metadata = ray.get([metadata_mapping[b] for b in blocks_out])
        map_bar.close()
        return BlockList(blocks_out, new_metadata)