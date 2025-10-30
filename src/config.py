"""
Kafka and Logger configuration
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

"""Configuration settings for the consumer service"""
import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Configuration
KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET")

# Application Configuration
OUTPUT_DIR = os.getenv("OUTPUT_DIR")
LOG_LEVEL = os.getenv("LOG_LEVEL")
