#!/usr/bin/env python3
"""
Kafka MCP Server - A Model Context Protocol server for Kafka operations using FastMCP
"""

import json
import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (
    InvalidPartitionsError,
    InvalidReplicationFactorError,
    KafkaError,
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError,
)
from mcp.server.fastmcp import Context, FastMCP

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka-mcp-server")


class KafkaManager:
    """Manages Kafka connections and operations"""

    def __init__(self, config_file: str):
        self.config_file = Path(config_file)
        self.config = self._load_config()
        self.admin_client = None
        self.producer = None

    def _load_config(self) -> Dict[str, Any]:
        """Load Kafka configuration from properties file"""
        config = {}
        try:
            with open(self.config_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        if "=" in line:
                            key, value = line.split("=", 1)
                            config[key.strip()] = value.strip()
            logger.info(f"Loaded Kafka config from {self.config_file}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config file {self.config_file}: {e}")
            raise

    def _get_admin_client(self) -> KafkaAdminClient:
        """Get or create Kafka admin client"""
        if self.admin_client is None:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.config.get(
                    "bootstrap.servers", "localhost:9092"
                ),
                client_id=self.config.get("client.id", "kafka-mcp-server"),
                **{
                    k: v
                    for k, v in self.config.items()
                    if k not in ["bootstrap.servers", "client.id"]
                },
            )
        return self.admin_client

    def _get_producer(self) -> KafkaProducer:
        """Get or create Kafka producer"""
        if self.producer is None:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.get(
                    "bootstrap.servers", "localhost:9092"
                ),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                **{
                    k: v
                    for k, v in self.config.items()
                    if k not in ["bootstrap.servers"]
                },
            )
        return self.producer

    def list_topics(self) -> List[Dict[str, Any]]:
        """List all topics in the Kafka cluster"""
        try:
            admin = self._get_admin_client()
            metadata = admin.describe_topics()

            if metadata is None:
                return []
            topics = []
            for topic_metadata in metadata:
                topics.append(
                    {
                        "name": topic_metadata.get("topic"),
                        "partitions": len(topic_metadata.get("partitions")),
                        "replication_factor": (
                            len(topic_metadata.get("partitions")[0].get("replicas"))
                            if topic_metadata.get("partitions")
                            else 0
                        ),
                    }
                )

            return sorted(topics, key=lambda x: x["name"])
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            raise

    def create_topic(
        self,
        name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        config: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Create a new topic"""
        if num_partitions < 1:
            return {
                "status": "error",
                "message": "Number of partitions must be at least 1.",
            }

        if replication_factor < 1 and replication_factor != -1:
            return {
                "status": "error",
                "message": "Replication factor must be at least 1 or -1 for default.",
            }
        try:
            admin = self._get_admin_client()

            topic = NewTopic(
                name=name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=config or {},
            )

            response = admin.create_topics([topic])
            logger.info(f"Create Topics response: {response}")

            return {
                "status": "success",
                "message": f"Topic '{name}' created successfully",
                "topic": {
                    "name": name,
                    "partitions": num_partitions,
                    "replication_factor": replication_factor,
                },
            }

        except TopicAlreadyExistsError:
            return {
                "status": "error",
                "message": f"Topic '{name}' already exists",
            }
        except InvalidReplicationFactorError:
            return {
                "status": "error",
                "message": "Invalid replication factor. It must be at least 1 or -1 for default.",
            }
        except InvalidPartitionsError:
            return {
                "status": "error",
                "message": "Invalid number of partitions. It must be a positive integer.",
            }
        except KafkaError as e:
            logger.error(f"Kafka error while creating topic '{name}': {e}")
            return {
                "status": "error",
                "message": f"Kafka error: {str(e)}",
            }
        except Exception as e:
            logger.exception(f"Unexpected error while creating topic {name}")
            return {
                "status": "error",
                "message": f"Unexpected error: {str(e)}",
            }

    def delete_topic(self, name: str) -> Dict[str, Any]:
        """Delete a topic"""
        try:
            admin = self._get_admin_client()
            response = admin.delete_topics([name])
            logger.info(f"Delete Topic response: {response}")

            return {
                "status": "success",
                "message": f"Topic '{name}' deleted successfully",
            }

        except UnknownTopicOrPartitionError:
            return {
                "status": "error",
                "message": f"Topic '{name}' does not exist or already deleted.",
            }
        except KafkaError as e:
            logger.error(f"Kafka error while deleting topic '{name}': {e}")
            return {
                "status": "error",
                "message": f"Kafka error: {str(e)}",
            }

        except Exception as e:
            logger.error(f"Failed to start topic deletion for '{name}': {e}")
            return {
                "status": "error",
                "message": f"Failed to initiate topic deletion: {str(e)}",
            }

    def get_topic_info(self, name: str) -> Dict[str, Any]:
        """Get detailed information about a specific topic"""
        try:
            admin = self._get_admin_client()
            metadata = admin.describe_topics([name])

            logger.info(metadata)
            if metadata is None:
                return {"status": "error", "message": f"Topic '{name}' not found"}

            topic_metadata = metadata[0]
            partitions_info = []

            for partition in topic_metadata.get("partitions"):
                partitions_info.append(
                    {
                        "partition_id": partition.get("partition"),
                        "leader": partition.get("leader"),
                        "replicas": partition.get("replicas"),
                        "isr": partition.get("isr"),
                    }
                )

            return {
                "status": "success",
                "topic": {
                    "name": name,
                    "partitions": partitions_info,
                    "partition_count": len(partitions_info),
                    "replication_factor": (
                        len(partitions_info[0]["replicas"]) if partitions_info else 0
                    ),
                },
            }
        except Exception as e:
            logger.error(f"Failed to get topic info for {name}: {e}")
            return {
                "status": "error",
                "message": f"Failed to get topic info for '{name}': {str(e)}",
            }

    def close(self):
        """Close all connections"""
        if self.admin_client:
            self.admin_client.close()
        if self.producer:
            self.producer.close()


@dataclass
class KafkaContext:
    """Application context holding Kafka manager"""

    kafka_manager: Optional[KafkaManager] = None


# Create the FastMCP server with lifespan management
@asynccontextmanager
async def kafka_lifespan(server: FastMCP) -> AsyncIterator[KafkaContext]:
    """Manage Kafka connections lifecycle"""
    context = KafkaContext()
    try:
        yield context
    finally:
        if context.kafka_manager:
            context.kafka_manager.close()


# Initialize the MCP server
mcp = FastMCP("Kafka MCP Server", lifespan=kafka_lifespan)


@mcp.tool()
def kafka_initialize_connection(config_file: str, ctx: Context) -> str:
    """Connect to Kafka using a properties file"""
    try:
        kafka_manager = KafkaManager(config_file)
        # Store in the lifespan context
        ctx.request_context.lifespan_context.kafka_manager = kafka_manager
        return f"Successfully connected to Kafka using config file: {config_file}"
    except Exception as e:
        return f"Failed to connect to Kafka: {str(e)}"


@mcp.tool()
def kafka_list_topics(ctx: Context) -> str:
    """List all topics in the Kafka cluster"""
    kafka_manager = ctx.request_context.lifespan_context.kafka_manager
    if not kafka_manager:
        return "Error: Not connected to Kafka. Please use kafka_initialize_connection first."

    try:
        topics = kafka_manager.list_topics()
        if topics:
            topics_info = []
            for topic in topics:
                topics_info.append(
                    f"• {topic['name']} (partitions: {topic['partitions']}, replication: {topic['replication_factor']})"
                )
            return f"Topics in Kafka cluster:\n" + "\n".join(topics_info)
        else:
            return "No topics found in the Kafka cluster."
    except Exception as e:
        return f"Error listing topics: {str(e)}"


@mcp.tool()
def kafka_create_topic(
    name: str,
    partitions: int = 1,
    replication_factor: int = 1,
    config: Optional[Dict[str, str]] = None,
    ctx: Context = None,
) -> str:
    """Create a new Kafka topic"""
    kafka_manager = ctx.request_context.lifespan_context.kafka_manager
    if not kafka_manager:
        return "Error: Not connected to Kafka. Please use kafka_initialize_connection first."

    try:
        result = kafka_manager.create_topic(
            name, partitions, replication_factor, config
        )
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error creating topic: {str(e)}"


@mcp.tool()
def kafka_delete_topic(name: str, ctx: Context) -> str:
    """Delete a Kafka topic"""
    kafka_manager = ctx.request_context.lifespan_context.kafka_manager
    if not kafka_manager:
        return "Error: Not connected to Kafka. Please use kafka_initialize_connection first."

    try:
        result = kafka_manager.delete_topic(name)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error deleting topic: {str(e)}"


@mcp.tool()
def kafka_get_topic_info(name: str, ctx: Context) -> str:
    """Get detailed information about a specific topic"""
    kafka_manager = ctx.request_context.lifespan_context.kafka_manager
    if not kafka_manager:
        return "Error: Not connected to Kafka. Please use kafka_initialize_connection first."

    try:
        result = kafka_manager.get_topic_info(name)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error getting topic info: {str(e)}"


@mcp.resource("kafka://topics")
def get_all_topics() -> str:
    """Provide all Kafka topics as a resource"""
    ctx = mcp.get_context()
    kafka_manager = ctx.request_context.lifespan_context.kafka_manager
    if not kafka_manager:
        return "Not connected to Kafka cluster"

    try:
        topics = kafka_manager.list_topics()
        return json.dumps(topics, indent=2)
    except Exception as e:
        return f"Error fetching topics: {str(e)}"


@mcp.resource("kafka://topic/{topic_name}")
def get_topic_resource(topic_name: str) -> str:
    """Provide specific topic information as a resource"""
    ctx = mcp.get_context()
    kafka_manager = ctx.request_context.lifespan_context.kafka_manager
    if not kafka_manager:
        return "Not connected to Kafka cluster"

    try:
        result = kafka_manager.get_topic_info(topic_name)
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error fetching topic info: {str(e)}"


@mcp.prompt()
def kafka_troubleshoot(issue: str) -> str:
    """Get troubleshooting guidance for Kafka issues"""
    return f"""
Help me troubleshoot this Kafka issue: {issue}

Please provide:
1. Possible causes of this issue
2. Step-by-step debugging approach
3. Common solutions
4. How to prevent this issue in the future

Consider checking:
- Broker connectivity and health
- Topic configuration
- Consumer group status
- Network and security settings
- Resource utilization
"""


@mcp.prompt()
def kafka_best_practices() -> str:
    """Provide Kafka best practices guidance"""
    return """
Provide comprehensive Kafka best practices covering:

1. **Topic Design:**
   - Naming conventions
   - Partition strategy
   - Replication factor guidelines

2. **Performance Optimization:**
   - Producer configuration
   - Consumer configuration
   - Batch processing

3. **Security:**
   - Authentication setup
   - Authorization policies
   - Encryption in transit and at rest

4. **Monitoring:**
   - Key metrics to track
   - Alerting strategies
   - Log management

5. **Operational Excellence:**
   - Deployment strategies
   - Backup and recovery
   - Capacity planning
"""


if __name__ == "__main__":
    # Run the server
    mcp.run()
