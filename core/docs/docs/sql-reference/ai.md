---
sidebar_position: 9
---

# ai — LLM Inference

The `ai` extension provides local LLM inference via llama.cpp with GPU acceleration (CUDA, Vulkan, Metal). Supports text generation, chat, embeddings, and batch processing.

## Model Management

### `trex_ai_list_models(path)`

List available model files in a directory.

| Parameter | Type | Description |
|-----------|------|-------------|
| path | VARCHAR | Directory containing GGUF model files |

**Returns:** VARCHAR — JSON list of models

```sql
SELECT trex_ai_list_models('/models');
```

### `trex_ai_download_model(repo, model_name, output_dir)`

Download a model from a Hugging Face repository.

| Parameter | Type | Description |
|-----------|------|-------------|
| repo | VARCHAR | Hugging Face repo ID |
| model_name | VARCHAR | Model filename |
| output_dir | VARCHAR | Local download directory |

**Returns:** VARCHAR

```sql
SELECT trex_ai_download_model('TheBloke/Llama-2-7B-GGUF', 'llama-2-7b.Q4_K_M.gguf', '/models');
```

### `trex_ai_load_model(model_name, model_path)`

Load a model into memory for text generation.

| Parameter | Type | Description |
|-----------|------|-------------|
| model_name | VARCHAR | Alias for the loaded model |
| model_path | VARCHAR | Path to the GGUF file |

**Returns:** VARCHAR

```sql
SELECT trex_ai_load_model('llama2', '/models/llama-2-7b.Q4_K_M.gguf');
```

### `trex_ai_load_model_for_embeddings(model_name, model_path)`

Load a model optimized for embedding generation.

| Parameter | Type | Description |
|-----------|------|-------------|
| model_name | VARCHAR | Alias for the loaded model |
| model_path | VARCHAR | Path to the GGUF file |

**Returns:** VARCHAR

```sql
SELECT trex_ai_load_model_for_embeddings('embed-model', '/models/nomic-embed-text.Q4_K_M.gguf');
```

### `trex_ai_unload_model(model_name)`

Unload a model from memory.

| Parameter | Type | Description |
|-----------|------|-------------|
| model_name | VARCHAR | Model alias to unload |

**Returns:** VARCHAR

```sql
SELECT trex_ai_unload_model('llama2');
```

### `trex_ai_list_loaded()`

List all currently loaded models.

**Returns:** VARCHAR — JSON

```sql
SELECT trex_ai_list_loaded();
```

### `trex_ai_model_info(model_name)`

Get detailed information about a loaded model.

| Parameter | Type | Description |
|-----------|------|-------------|
| model_name | VARCHAR | Model alias |

**Returns:** VARCHAR — JSON

```sql
SELECT trex_ai_model_info('llama2');
```

## Inference

### `trex_ai_generate(model_name, prompt, options)`

Generate text from a prompt.

| Parameter | Type | Description |
|-----------|------|-------------|
| model_name | VARCHAR | Model alias |
| prompt | VARCHAR | Input prompt |
| options | VARCHAR | JSON generation options (temperature, max_tokens, etc.) |

**Returns:** VARCHAR

```sql
SELECT trex_ai_generate('llama2', 'Explain SQL joins in one paragraph', '{"temperature": 0.7, "max_tokens": 256}');
```

### `trex_ai_chat(model_name, messages, options)`

Chat completion with message history.

| Parameter | Type | Description |
|-----------|------|-------------|
| model_name | VARCHAR | Model alias |
| messages | VARCHAR | JSON array of chat messages |
| options | VARCHAR | JSON generation options |

**Returns:** VARCHAR

```sql
SELECT trex_ai_chat('llama2',
  '[{"role": "user", "content": "What is trexsql?"}]',
  '{"temperature": 0.7}'
);
```

### `trex_ai_embed(model_name, text)`

Generate an embedding vector for input text.

| Parameter | Type | Description |
|-----------|------|-------------|
| model_name | VARCHAR | Embedding model alias |
| text | VARCHAR | Input text |

**Returns:** VARCHAR — JSON array of floats

```sql
SELECT trex_ai_embed('embed-model', 'patient diagnosis record');
```

### `trex_ai(query)`

Shorthand inference function.

| Parameter | Type | Description |
|-----------|------|-------------|
| query | VARCHAR | Query text |

**Returns:** VARCHAR

```sql
SELECT trex_ai('Summarize this SQL query: SELECT ...');
```

## Batch Processing

### `trex_ai_batch_process(batch_json)`

Submit a batch of inference requests for asynchronous processing.

| Parameter | Type | Description |
|-----------|------|-------------|
| batch_json | VARCHAR | JSON batch specification |

**Returns:** VARCHAR — batch ID

```sql
SELECT trex_ai_batch_process('[{"prompt": "Query 1"}, {"prompt": "Query 2"}]');
```

### `trex_ai_batch_result(batch_id)`

Retrieve results of a completed batch.

| Parameter | Type | Description |
|-----------|------|-------------|
| batch_id | VARCHAR | Batch identifier |

**Returns:** VARCHAR — JSON results

```sql
SELECT trex_ai_batch_result('batch-123');
```

## System Status

### `trex_ai_status()`

Get overall AI engine status.

**Returns:** VARCHAR — JSON

```sql
SELECT trex_ai_status();
```

### `trex_ai_gpu_info()`

Get GPU device information and acceleration status.

**Returns:** VARCHAR — JSON

```sql
SELECT trex_ai_gpu_info();
```

### `trex_ai_metrics()`

Get performance metrics (tokens/sec, latency, etc.).

**Returns:** VARCHAR — JSON

```sql
SELECT trex_ai_metrics();
```

### `trex_ai_memory_status()`

Get memory usage of loaded models.

**Returns:** VARCHAR — JSON

```sql
SELECT trex_ai_memory_status();
```

### `trex_ai_context_pool_status()`

Get inference context pool utilization.

**Returns:** VARCHAR — JSON

```sql
SELECT trex_ai_context_pool_status();
```

### `trex_ai_cleanup_contexts()`

Release unused inference contexts to free memory.

**Returns:** VARCHAR

```sql
SELECT trex_ai_cleanup_contexts();
```

### `trex_ai_openssl_version(version_type)`

Return the OpenSSL version linked by the extension.

| Parameter | Type | Description |
|-----------|------|-------------|
| version_type | VARCHAR | Version string type |

**Returns:** VARCHAR

```sql
SELECT trex_ai_openssl_version('full');
```
