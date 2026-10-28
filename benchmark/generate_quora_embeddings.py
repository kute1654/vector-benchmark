"""一次性数据转换脚本：为 quora 语料/查询生成句向量。

输入（gzip JSONL，每行一个文档/查询，至少含 ``_id`` 与 ``text``）：
    datasets/downloads/quora/corpus.jsonl.gz
    datasets/downloads/quora/queries.jsonl.gz

输出（gzip JSONL，每行 ``{"_id": ..., "vector": [...]}``）：
    datasets/downloads/quora/corpus_vectors.jsonl.gz
    datasets/downloads/quora/queries_vectors.jsonl.gz

这些输出与 ``datasets/datasets.json`` 中 quora-mini-hybrid-dev 的
``corpus_embedding_file`` / ``queries_embedding_file`` 对应，由
``GzTsvReader`` 在 hybrid_search 场景读取。

模型：sentence-transformers ``all-MiniLM-L6-v2``，输出 384 维，与配置中的
``vector_size: 384`` 一致。

注意：
- 路径是**相对当前工作目录**的硬编码值，需在项目根目录（存在 ``datasets/``
  的那一级）执行，例如 ``python benchmark/generate_quora_embeddings.py``。
- 脚本会覆盖输出文件；逐行编码，内存占用与文件大小无关。
- 无缓存/断点续跑：中途失败需整份重跑。
"""

import gzip
import json
from sentence_transformers import SentenceTransformer

# ------------------- 配置 -------------------
model_name = "all-MiniLM-L6-v2"  # 输出 384 维（对应你配置里的 vector_size:384）
corpus_input = "datasets/downloads/quora/corpus.jsonl.gz"
queries_input = "datasets/downloads/quora/queries.jsonl.gz"

corpus_output = "datasets/downloads/quora/corpus_vectors.jsonl.gz"
queries_output = "datasets/downloads/quora/queries_vectors.jsonl.gz"

# 加载模型
# 首次运行会从 Hugging Face 下载权重（需联网），之后使用本地缓存。
model = SentenceTransformer(model_name)

# ------------------- 生成 corpus 向量 -------------------
# 逐行读取 -> 编码 -> 逐行写出；gzip 文本模式保证 JSON 行以 UTF-8 存储。
with gzip.open(corpus_input, "rt", encoding="utf-8") as f_in, \
     gzip.open(corpus_output, "wt", encoding="utf-8") as f_out:

    for line in f_in:
        item = json.loads(line)
        text = item["text"]  # quora 格式固定
        # convert_to_numpy=False 返回框架张量（而非 ndarray），.tolist() 转成 Python 列表，
        # 以便 json.dumps 序列化。
        vec = model.encode(text, convert_to_numpy=False).tolist()
        # 只保留 _id 与向量；id 与 GzTsvReader 使用的语料 id 同源，保证能对齐。
        f_out.write(json.dumps({"_id": item["_id"], "vector": vec}) + "\n")

# ------------------- 生成 queries 向量 -------------------
# 与 corpus 完全相同的流程，输出供 hybrid 检索的查询向量使用。
with gzip.open(queries_input, "rt", encoding="utf-8") as f_in, \
     gzip.open(queries_output, "wt", encoding="utf-8") as f_out:

    for line in f_in:
        item = json.loads(line)
        text = item["text"]
        vec = model.encode(text, convert_to_numpy=False).tolist()
        f_out.write(json.dumps({"_id": item["_id"], "vector": vec}) + "\n")

# 成功结束时的提示（打印输出路径，便于确认写入位置）。
print("✅ 向量生成完成：")
print(corpus_output)
print(queries_output)