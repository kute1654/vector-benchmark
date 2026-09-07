#!/usr/bin/env python3
"""Complete missing SIFT-128 rows and create the ClickHouse vector index."""

from __future__ import annotations

import time

import h5py
from clickhouse_driver import Client


PATH = "benchmark/datasets/downloads/sift-128-euclidean.hdf5"
TABLE = "benchmark_sift_128"
HOST = "127.0.0.1"
PORT = 9000
TOTAL = 10_000_000
BATCH = 10_000


def main() -> None:
    client = Client(host=HOST, port=PORT, database="default",
                    connect_timeout=10, send_receive_timeout=300,
                    sync_request_timeout=300)
    existing = client.execute(f"SELECT max(id), count() FROM {TABLE}")[0]
    max_id = int(existing[0]) if existing[0] is not None else -1
    count = int(existing[1])
    if count < TOTAL:
        start = max_id + 1
        with h5py.File(PATH, "r") as fp:
            train = fp["train"]
            for offset in range(start, TOTAL, BATCH):
                end = min(offset + BATCH, TOTAL)
                rows = [(i, train[i].astype("float32").tolist()) for i in range(offset, end)]
                client.execute(f"INSERT INTO {TABLE} (id, vector) VALUES", rows)
                if offset == start or end % 500_000 == 0:
                    print(f"uploaded {end}/{TOTAL}", flush=True)
    print("rows=", client.execute(f"SELECT count() FROM {TABLE}")[0][0], flush=True)

    # Merge parts before defining/materializing the vector index.
    client.execute(f"OPTIMIZE TABLE {TABLE} FINAL")
    while True:
        merges = client.execute(
            f"SELECT count() FROM system.merges WHERE database='default' AND table='{TABLE}'"
        )[0][0]
        if int(merges) == 0:
            break
        time.sleep(5)
    existing_indexes = client.execute(
        f"SELECT name FROM system.data_skipping_indices WHERE database='default' AND table='{TABLE}'"
    )
    if not existing_indexes:
        ddl = (
            f"ALTER TABLE {TABLE} ADD INDEX vector_index vector "
            f"TYPE vector_similarity('hnsw', 'L2Distance', 128, 'bf16', 32, 256) GRANULARITY 1"
        )
        client.execute(ddl)
        client.execute(f"ALTER TABLE {TABLE} MATERIALIZE INDEX vector_index")
    print("clickhouse setup complete", flush=True)


if __name__ == "__main__":
    main()
