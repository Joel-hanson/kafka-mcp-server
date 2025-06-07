#!/usr/bin/env python3
"""
Kafka Utilities - Core Kafka operations and management functionality
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import (
    InvalidPartitionsError,
    InvalidReplicationFactorError,
    KafkaError,
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError,
)

# Configure logging
logger = logging.getLogger("kafka-utils")


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
                    if k not in ["bootstrap.servers", "client.id"]
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

    def send_message(
        self, topic: str, message: Optional[Any], key: Optional[str] = None
    ) -> Dict[str, Any]:
        """Send a message to a Kafka topic"""
        try:
            producer = self._get_producer()
            future = producer.send(
                topic, value=message, key=key.encode("utf-8") if key else None
            )
            record_metadata = future.get(timeout=10)

            return {
                "status": "success",
                "message": f"Message sent to topic '{topic}'",
                "metadata": {
                    "topic": record_metadata.topic,
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset,
                },
            }
        except Exception as e:
            logger.error(f"Failed to send message to topic '{topic}': {e}")
            return {"status": "error", "message": f"Failed to send message: {str(e)}"}

    def close(self):
        """Close all connections"""
        if self.admin_client:
            self.admin_client.close()
        if self.producer:
            self.producer.close()


# Troubleshooting and best practices content
KAFKA_TROUBLESHOOTING_GUIDE = """
# Kafka Troubleshooting Guide

## Issue: {issue}

### Immediate Diagnostic Steps:
1. **Check Broker Connectivity**
   - Verify bootstrap servers are reachable
   - Test network connectivity: `telnet <broker-host> <broker-port>`
   - Check broker logs for errors

2. **Validate Configuration**
   - Review client configuration settings
   - Verify security settings (SASL, SSL)
   - Check topic-specific configurations

3. **Monitor Key Metrics**
   - Broker CPU and memory usage
   - Disk space and I/O performance  
   - Network latency and throughput
   - Topic partition distribution

### Common Solutions by Category:

**Connection Issues:**
- Verify bootstrap.servers configuration
- Check firewall rules and security groups
- Validate authentication credentials
- Test with simple console tools first

**Performance Issues:**
- Adjust batch.size and linger.ms for producers
- Tune fetch.min.bytes and fetch.max.wait.ms for consumers
- Monitor partition balance across brokers
- Check for hot partitions

**Data Loss/Consistency:**
- Verify acks=all for critical data
- Check min.insync.replicas setting
- Review retention policies
- Validate replication factor

**Consumer Lag:**
- Scale consumer instances
- Optimize consumer processing logic
- Check for slow consumers in group
- Consider increasing max.poll.records

### Prevention Strategies:
- Implement comprehensive monitoring
- Set up alerting for key metrics
- Regular configuration reviews
- Capacity planning and testing
- Documentation of operational procedures

Would you like me to elaborate on any specific aspect of this troubleshooting guide?
"""

KAFKA_BEST_PRACTICES = """
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

KAFKA_PERFORMANCE_TUNING = """
# Kafka Performance Tuning Guide

## Producer Optimization

### Throughput-Focused Settings:
```properties
# Batch processing
batch.size=16384                 # Increase for higher throughput
linger.ms=5                     # Small delay for batching
compression.type=lz4            # Fast compression

# Network and memory
buffer.memory=33554432          # 32MB buffer
send.buffer.bytes=131072        # 128KB send buffer
receive.buffer.bytes=65536      # 64KB receive buffer
```

### Reliability-Focused Settings:
```properties
acks=all                        # Wait for all replicas
retries=2147483647             # Max retries
max.in.flight.requests.per.connection=1  # Ordering guarantee
enable.idempotence=true         # Exactly-once semantics
```

## Consumer Optimization

### High Throughput Settings:
```properties
fetch.min.bytes=50000           # Larger fetch sizes
fetch.max.wait.ms=500          # Balance latency vs throughput
max.partition.fetch.bytes=1048576  # 1MB per partition
max.poll.records=500           # More records per poll
```

### Low Latency Settings:
```properties
fetch.min.bytes=1              # Immediate fetch
fetch.max.wait.ms=100         # Quick response
max.poll.interval.ms=300000    # Frequent polling
```

## Broker Configuration

### JVM Settings:
```bash
# Heap size (typically 1-6GB)
-Xms6g -Xmx6g

# G1 Garbage Collector
-XX:+UseG1GC
-XX:MaxGCPauseMillis=20
-XX:InitiatingHeapOccupancyPercent=35
```

### Key Broker Settings:
```properties
# Network threads
num.network.threads=8
num.io.threads=16

# Log settings
log.segment.bytes=1073741824    # 1GB segments
log.retention.hours=168         # 7 days retention
log.cleanup.policy=delete
```

## Topic Design Best Practices

### Partitioning Strategy:
- **Rule of thumb**: Start with (target throughput / partition throughput)
- **Consider**: Consumer parallelism requirements
- **Key distribution**: Ensure even distribution across partitions
- **Future scaling**: Plan for growth

### Replication Guidelines:
- **Production**: replication.factor=3 (minimum)
- **Development**: replication.factor=1 acceptable
- **min.insync.replicas=2** for durability

## Monitoring Key Metrics

### Producer Metrics:

- `record-send-rate` - records sent/sec
- `request-latency-avg` - average time to send
- `record-error-rate` - failed sends
- `buffer-available-bytes` - remaining producer buffer

### Consumer Metrics:

- `records-consumed-rate` - consumption throughput
- `fetch-latency-avg` - latency of fetches
- `rebalance-rate-per-hour` - frequent rebalances are a red flag
- `commit-latency-avg` - time to commit offsets

### Broker Metrics:

- `MessagesInPerSec` - ingestion rate
- `BytesInPerSec`, `BytesOutPerSec` - bandwidth
- `UnderReplicatedPartitions` - should be zero
- `RequestHandlerAvgIdlePercent` - idle CPU time, low values may signal thread starvation

"""
