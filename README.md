# Kafka MCP Server

A Model Context Protocol (MCP) server for interacting with Apache Kafka through a chatbot interface.

## Installation

1. Install Python dependencies:

```bash
pip install -r requirements.txt
```

1. Make the server executable:

```bash
chmod +x main.py
```

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

## Usage

### Running the Server

```bash
python main.py
```

### MCP Client Configuration

Add this to your MCP client configuration (e.g., Claude Desktop):

```json
{
  "mcpServers": {
    "kafka": {
      "command": "python",
      "args": ["/path/to/main.py"]
    }
  }
}
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
   Use kafka_initialize_connection with your properties file path
   ```

2. List existing topics:

   ```text
   Use kafka_list_topics to see all topics
   ```

3. Create a new topic:

   ```text
   Use kafka_create_topic with name "my-topic", partitions 3, replication_factor 1
   ```

4. Get topic details:

   ```text
   Use kafka_get_topic_info for "my-topic"
   ```

## Features

- **Configuration Management**: Loads Kafka connection details from standard properties files
- **Topic Management**: List, create, delete, and inspect topics
- **Error Handling**: Comprehensive error handling with informative messages
- **Connection Management**: Efficient connection pooling and cleanup
- **Extensible**: Easy to add more Kafka operations

## Supported Kafka Operations

### Current (v0.1.0)

- Topic listing
- Topic creation with custom partitions and replication factor
- Topic deletion
- Topic inspection (partitions, replicas, ISR)

### Planned Features

- Message production
- Message consumption
- Consumer group management
- Offset management
- Cluster information
- Performance metrics

## Troubleshooting

### Common Issues

1. **Connection Failed**: Verify your `bootstrap.servers` in the properties file
2. **Authentication Error**: Check your SASL/SSL configuration
3. **Topic Already Exists**: Use `kafka_get_topic_info` to check existing topics
4. **Permission Denied**: Ensure your Kafka user has the necessary permissions

### Logging

The server logs to stdout. You can adjust the logging level by modifying the `logging.basicConfig(level=logging.INFO)` line in the code.

## Security Notes

- Store sensitive credentials securely
- Use SSL/TLS for production environments
- Implement proper authentication and authorization
- Regularly rotate credentials

## Contributing

This is a foundational implementation. Future enhancements could include:

- Schema registry integration
- Kafka Streams operations
- Monitoring and alerting
- Batch operations
- Advanced consumer configurations
