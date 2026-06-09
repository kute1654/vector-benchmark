import ast
import math
from typing import Iterator, Optional, Tuple, List

import h5py
import numpy as np

from benchmark.dataset_config import DatasetConfig
from benchmark.cli_output import compact_kv, step
from dataset_reader.base_reader import BaseReader, Query, Record
from dataset_reader.utils import convert_H52py

HDF5_BATCH_PART_SIZE = 200000


def convert_bytes_to_str(text):
    if isinstance(text, bytes):
        text = text.decode('utf-8')
    if not isinstance(text, str):
        text = str(text)
    return text


class AnnH5Reader(BaseReader):
    def __init__(self, dataset_dir, dataset_config: DatasetConfig, normalize=False):
        self.dataset_dir = dataset_dir
        self.dataset_config = dataset_config
        self.normalize = normalize
        # 初始化 query_files
        if self.dataset_config.query_files is not None:
            query_files = [
                {
                    "path": self.dataset_dir / query_file_op["path"],
                    "meta": query_file_op["meta"],
                    "score_type": query_file_op.get("score_type", "default"),
                    "queries_pool_size": query_file_op["queries_pool_size"],
                } for
                query_file_op in self.dataset_config.query_files]
        else:
            query_files = [{"path": self.dataset_dir / self.dataset_config.path,
                            "meta": None,
                            "queries_pool_size": self.dataset_config.queries_pool_size}]
        self.query_files = query_files

    def read_queries(self, times: Optional[int] = 1000, query_meta: Optional[dict] = None) -> Iterator[Query]:
        for query_path in self.query_files:
            compact_kv(
                "queries",
                path=query_path.get("path"),
                meta=query_path.get("meta"),
                requested=times,
            )
            # skip mismatched query path
            if query_meta is not None and query_meta != query_path["meta"]:
                step("skip: meta not match")
                continue
            with h5py.File(query_path["path"], "r") as query_data:
                # Pre-load data into memory to avoid I/O during iteration
                # Load datasets
                test_vectors = query_data["test"][:]
                total_queries = len(test_vectors)
                
                if "filter" in query_data.keys():
                    raw_filters = query_data["filter"][:]
                else:
                    raw_filters = [None] * total_queries

                if "neighbors" in query_data.keys():
                    raw_neighbors = query_data["neighbors"][:]
                else:
                    raw_neighbors = [None] * total_queries

                if "distances" in query_data.keys():
                    raw_distances = query_data["distances"][:]
                else:
                    raw_distances = [None] * total_queries

                # Handle score type
                if query_path.get("score_type", None) is not None:
                    st = query_path["score_type"]
                elif self.dataset_config.score_type is not None:
                    st = self.dataset_config.score_type
                else:
                    st = "default"

                # Handle text columns
                query_columns_in_hdf5 = query_data.attrs.get("query_columns_in_hdf5", [])
                query_columns_in_table = query_data.attrs.get("query_columns_in_table", [])
                
                if len(query_columns_in_hdf5) != 0:
                    raw_query_texts = [convert_bytes_to_str(bytes_str) for bytes_str in query_data[query_columns_in_hdf5[0]][:]]
                else:
                    raw_query_texts = [None] * total_queries

                if len(query_columns_in_table) != 0:
                    query_text_column = query_columns_in_table[0]
                else:
                    query_text_column = None

                count = 0
                # Use a while loop to support cycling through the dataset if times > total_queries
                while count < times:
                    # Iterate over pre-loaded data
                    for i in range(total_queries):
                        if count >= times:
                            break
                            
                        vector = test_vectors[i]
                        if self.normalize:
                            vector = vector / np.linalg.norm(vector)
                        
                        filter_condition = raw_filters[i]
                        expected_result = raw_neighbors[i]
                        expected_scores = raw_distances[i]
                        query_text = raw_query_texts[i]
                        
                        # Process meta conditions (AST eval is slow, but doing it here avoids HDF5 overhead)
                        meta_conditions = None
                        if filter_condition is not None:
                             # Optimize: Only decode/eval if absolutely necessary and not None
                             # Assuming filter_condition is bytes
                             try:
                                 decoded_cond = filter_condition.decode("ascii", "ignore")
                                 meta_conditions = ast.literal_eval(decoded_cond).get("conditions", None)
                             except Exception:
                                 meta_conditions = None
    
                        count += 1
                        yield Query(
                            vector=vector.tolist(), # Still converting to list, but from memory
                            meta_conditions=meta_conditions,
                            expected_result=expected_result.tolist() if expected_result is not None else [],
                            expected_scores=expected_scores.tolist() if expected_scores is not None else [],
                            score_type=st,
                            query_text=query_text,
                            query_text_column=query_text_column,
                        )
                break  # Finished yielding required number of queries

    def read_data(self) -> Iterator[Record]:
        with h5py.File(self.dataset_dir / self.dataset_config.path, "r") as train_data:
            extra_columns = train_data.attrs.get("extra_columns",
                                                 []) if self.dataset_config.result_group == "hybrid_search" else []
            extra_columns_type = train_data.attrs.get("extra_columns_type",
                                                      []) if self.dataset_config.result_group == "hybrid_search" else []
            # get origin train datasets length
            data_size = train_data["train"].shape[0]
            # default use one batch_part
            batch_parts = 1

            if data_size > HDF5_BATCH_PART_SIZE:
                batch_parts = math.ceil(data_size / HDF5_BATCH_PART_SIZE)
            block_size = data_size // batch_parts

            global_idx = 0  # Add this line to initialize the global index.
            vectors_limit = -1
            vector_count = 0
            for i in range(batch_parts):
                start = i * block_size
                # To handle the case of uneven data sizes, we allow the last block to contain all the remaining data.
                if i == batch_parts - 1:
                    end = data_size
                else:
                    end = start + block_size
                step(f"read data block {i + 1}/{batch_parts}: rows {start}:{end} of {data_size}")
                # avoid mess memory consume
                data_block = train_data["train"][start:end]

                if 0 < vectors_limit <= vector_count:
                    break

                extra_columns_data = {col_name: train_data[col_name][start:end] for col_name in extra_columns}
                
                for idx, vector in enumerate(data_block):
                    # normalize the vector for some distance
                    if self.normalize:
                        vector /= np.linalg.norm(vector)

                    # read payload data
                    record_extra_data = {}
                    for col_name, col_type in zip(extra_columns, extra_columns_type):
                        value = extra_columns_data[col_name][idx]
                        if convert_H52py(col_type) == str:
                            value = convert_bytes_to_str(value)
                        record_extra_data[col_name] = value

                    if 0 < vectors_limit <= vector_count:
                        break
                    yield Record(id=global_idx,
                                 vector=vector.tolist()
                                 if (len(vector) == self.dataset_config.vector_size)
                                 else np.random.uniform(0, 1, self.dataset_config.vector_size).tolist(),
                                 metadata=None if len(record_extra_data.keys()) == 0 else record_extra_data)
                    global_idx += 1
                    vector_count += 1

    def read_column_name_type(self) -> Tuple[list, list]:
        """ Get the payloads data name and type """
        with h5py.File(self.dataset_dir / self.dataset_config.path, "r") as train_data:
            extra_columns = train_data.attrs.get("extra_columns", [])
            extra_columns_type = train_data.attrs.get("extra_columns_type", [])
            return extra_columns, extra_columns_type

    def get_query_files(self) -> List[dict]:
        return self.query_files
