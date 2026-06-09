import time
import os
import sys
from multiprocessing import get_context
from typing import Iterable, List, Optional, Tuple

import tqdm

from dataset_reader.base_reader import Record
from benchmark.cli_output import compact_kv, step, warn, set_live_line
from engine.base_client.utils import iter_batches, get_mem_available_bytes, format_bytes


class BaseUploader:
    client = None

    @staticmethod
    def _warn_memory(label: str, estimated_bytes: int, ratio: float = 0.8):
        available = get_mem_available_bytes()
        if available is None:
            return
        if estimated_bytes >= int(available * ratio):
            warn(
                f"{label} memory warning: estimated {format_bytes(estimated_bytes)} "
                f">= {int(ratio * 100)}% of available {format_bytes(available)}"
            )
            warn("Notice: the process may be killed.")
            warn(
                "If that happens, reduce upload_params.parallel and upload_params.batch_size in the test config (see README.md)."
            )

    def __init__(self, host, connection_params, upload_params):
        self.host = host
        self.connection_params = connection_params
        self.upload_params = upload_params

    @classmethod
    def get_mp_start_method(cls):
        return None

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params: dict, upload_params: dict,
                    extra_columns_name: list, extra_columns_type: list):
        raise NotImplementedError()

    def upload(
            self,
            distance,
            vector_count,
            records: Iterable[Record],
            extra_columns_name: list,
            extra_columns_type: list
    ) -> dict:
        start = time.perf_counter()
        parallel = self.upload_params.get("parallel", 16)
        batch_size = self.upload_params.get("batch_size", 256)

        compact_kv(
            "upload",
            parallel=parallel,
            batch_size=batch_size,
        )

        ctx = get_context(self.get_mp_start_method())
        with ctx.Pool(
                processes=int(parallel),
                initializer=self.__class__.init_client,
                initargs=(
                        self.host,
                        distance,
                        vector_count,
                        self.connection_params,
                        self.upload_params,
                        extra_columns_name,
                        extra_columns_type,
                ),
        ) as pool:
            uploaded = 0
            last_logged_percent = -1
            last_line_len = 0
            for batch_count in pool.imap(
                    self.__class__._upload_batch,
                    iter_batches(
                        records,
                        batch_size,
                    ),
            ):
                if batch_count is None:
                    continue
                uploaded += int(batch_count)
                if vector_count > 0:
                    percent = int(uploaded * 100 / vector_count)
                    if percent > last_logged_percent:
                        line = f" - upload progress: {uploaded}/{vector_count} ({percent}%)"
                        padding = " " * max(0, last_line_len - len(line))
                        sys.stdout.write("\r" + line + padding)
                        sys.stdout.flush()
                        set_live_line(True)
                        last_line_len = len(line)
                        last_logged_percent = percent
            if vector_count > 0 and last_logged_percent >= 0:
                sys.stdout.write("\n")
                sys.stdout.flush()
                set_live_line(False)
        upload_time = time.perf_counter() - start

        step(f"upload time: {upload_time:.3f}s")

        wait_index_begin = time.perf_counter()
        mp_method = "forkserver"
        if os.environ.get("NUITKA_ONEFILE_PARENT") or getattr(sys, "frozen", False):
            mp_method = "fork"

        try:
            ctx = get_context(mp_method)  # When use None, sometimes it will be blocked.
        except ValueError:
            ctx = get_context()
        with ctx.Pool(
                processes=1,
                initializer=self.__class__.init_client,
                initargs=(self.host,
                          distance,
                          vector_count,
                          self.connection_params,
                          self.upload_params,
                          extra_columns_name,
                          extra_columns_type,),
        ) as pool:
            post_upload_stats = pool.apply(func=self.post_upload, args=(distance,)) or {}

        total_time = time.perf_counter() - start
        post_time = time.perf_counter() - wait_index_begin

        return {
            "post_upload": post_time,
            "upload_time": upload_time,
            "total_time": total_time,
            **post_upload_stats,
        }

    # Upload data[ids, vectors, metadata] and return time consume
    @classmethod
    def _upload_batch(
            cls, batch: Tuple[List[int], List[list], List[Optional[dict]]]
    ) -> int:
        ids, vectors, metadata = batch
        cls.upload_batch(ids, vectors, metadata)
        return len(ids)

    @classmethod
    def post_upload(cls, distance):
        return {}

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]):
        raise NotImplementedError()
