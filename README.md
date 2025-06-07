# Kafka MCP Server

This project provides a **Model Context Protocol (MCP)** server that enables interaction with a Kafka cluster via standard tool-based interfaces. It exposes Kafka operations like producing messages, consuming, listing topics, and more — all accessible through Claude Desktop or any MCP-compatible client.

## 🛠 Features

* List Kafka topics
* Create and delete topics
* Produce messages

---

## 🚀 Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/joel-hanson/kafka-mcp-server.git
cd kafka-mcp-server
```

### 2. Setup Python environment (using `conda`)

```bash
conda create -n kafka-mcp python=3.10 -y
conda activate kafka-mcp
pip install -r requirements.txt
```

### 3. Start the MCP server (for local dev)

```bash
mcp dev
```

This starts the server and opens the **MCP Inspector** so you can test tool prompts and responses.

---

## Usage

### Running the Server

```bash
python main.py
```

### Available Tools

1. **kafka_initialize_connection** - Connect to Kafka using a properties file
2. **kafka_list_topics** - List all topics in the cluster
3. **kafka_create_topic** - Create a new topic
4. **kafka_delete_topic** - Delete an existing topic
5. **kafka_get_topic_info** - Get detailed information about a topic

### Example Usage Flow

1. First, connect to Kafka:

   ```text
   Use kafka_initialize_connection with your properties file <path>
   OR
   Initialize connection with kafka using the properties file <path>
   ```

2. List existing topics:

   ```text
   Use kafka_list_topics to see all topics
   OR
   List all the topics
   ```

3. Create a new topic:

   ```text
   Use kafka_create_topic with name "my-topic", partitions 3, replication_factor 1
   OR
   Create me a topic with name "my-topic"
   ```

4. Get topic details:

   ```text
   Use kafka_get_topic_info for "my-topic"
   ```

## Features

* **Configuration Management**: Loads Kafka connection details from standard properties files
* **Topic Management**: List, create, delete, and inspect topics
* **Error Handling**: Comprehensive error handling with informative messages
* **Connection Management**: Efficient connection pooling and cleanup
* **Extensible**: Easy to add more Kafka operations

## Supported Kafka Operations

### Current

* Topic listing
* Topic creation with custom partitions and replication factor
* Topic deletion
* Topic inspection (partitions, replicas, ISR)

### Planned Features

* Message production
* Message consumption
* Consumer group management
* Offset management
* Cluster information
* Performance metrics

## Troubleshooting

### Common Issues

1. **Connection Failed**: Verify your `bootstrap.servers` in the properties file
2. **Authentication Error**: Check your SASL/SSL configuration
3. **Topic Already Exists**: Use `kafka_get_topic_info` to check existing topics
4. **Permission Denied**: Ensure your Kafka user has the necessary permissions

### Logging

The server logs to stdout. You can adjust the logging level by modifying the `logging.basicConfig(level=logging.INFO)` line in the code.

## Security Notes

* Store sensitive credentials securely
* Use SSL/TLS for production environments
* Implement proper authentication and authorization
* Regularly rotate credentials

## 🧠 Using with Claude Desktop

> To integrate your Kafka MCP server with Claude Desktop:

Install it into Claude:

   ```bash
   mcp install <file_spec> -f <optional env file>
   ```

> Note: If you are seeing dependency-related issues, please configure the claud_desktop_config.json with the absolute path to the python and provide the file to run.

### MCP Client Configuration

Add this to your MCP client configuration (e.g., Claude Desktop):

```json
{
  "mcpServers": {
    "kafka": {
      "command": "python",
      "args": ["/path/to/server.py"]
    }
  }
}
```

   ```json
   {
   "mcpServers": {
         "Kafka MCP Server": {
            "command": "/opt/miniconda3/envs/kafka-mcp/bin/python",
            "args": [
            "/path/to/server.py"
            ]
         }
      }
   }
   ```

Claude will now detect and interact with the tools you've defined via `@mcp.prompt()` in your Python server.

## Configuration

Create a Kafka properties file (e.g., `kafka.properties`) with your Kafka connection details:

```properties
# Basic Kafka connection
bootstrap.servers=localhost:9092
client.id=kafka-mcp-client

# Security configuration (if needed)
# security.protocol=SASL_SSL
# sasl.mechanism=PLAIN
# sasl.username=your-username
# sasl.password=your-password

# SSL configuration (if needed)
# ssl.ca.location=/path/to/ca-cert
# ssl.certificate.location=/path/to/client-cert
# ssl.key.location=/path/to/client-key
```

---

## 📂 Project Structure

```text
kafka_mcp_server/
├── server.py               # Main FastMCP server defining tools
├── kafka_utils.py
├── requirements.txt
├── README.md
└── ...
```

---

## 🧩 Tool Debugging Tips

* Use `mcp dev` to inspect prompt behavior interactively
* Add `print()` or `logging.debug()` in your tool functions
* Use structured `@prompt` function signatures to ensure correct parsing

---

## 📦 Requirements

Add this to your `requirements.txt`:

```text
kafka-python
mcp[cli]
pydantic
uv
```

---

## Testing

Please run `docker compose up` to bring up kafka cluster with a single broker you can interact with using the bootstrap.server `localhost:9092`.

---

## 🔗 Resources

* [MCP Protocol Docs](https://modelcontextprotocol.io/)
* [Claude + MCP Guide](https://docs.unstructured.io/examplecode/tools/mcp)
* [mcp CLI Reference](https://github.com/modelcontextprotocol/python-sdk)
