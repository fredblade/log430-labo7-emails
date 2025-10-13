"""
Kafka and Logger configuration
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

"""Configuration settings for the consumer service"""
import os

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'user-events')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'user-event-consumer-group')
KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')

# Application Configuration
OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'output')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')