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
model = SentenceTransformer(model_name)

# ------------------- 生成 corpus 向量 -------------------
with gzip.open(corpus_input, "rt", encoding="utf-8") as f_in, \
     gzip.open(corpus_output, "wt", encoding="utf-8") as f_out:

    for line in f_in:
        item = json.loads(line)
        text = item["text"]  # quora 格式固定
        vec = model.encode(text, convert_to_numpy=False).tolist()
        f_out.write(json.dumps({"_id": item["_id"], "vector": vec}) + "\n")

# ------------------- 生成 queries 向量 -------------------
with gzip.open(queries_input, "rt", encoding="utf-8") as f_in, \
     gzip.open(queries_output, "wt", encoding="utf-8") as f_out:

    for line in f_in:
        item = json.loads(line)
        text = item["text"]
        vec = model.encode(text, convert_to_numpy=False).tolist()
        f_out.write(json.dumps({"_id": item["_id"], "vector": vec}) + "\n")

print("✅ 向量生成完成：")
print(corpus_output)
print(queries_output)