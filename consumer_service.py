"""
Kafka Consumer service
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import json
from logger import Logger
from typing import Optional
from kafka import KafkaConsumer
from handler_registry import HandlerRegistry

logger = Logger.get_instance("Coolriel")

class EventConsumerService:
    """Main consumer service that processes Kafka events"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        registry: HandlerRegistry,
        auto_offset_reset: str = 'earliest'
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.registry = registry
        self.auto_offset_reset = auto_offset_reset
        self.consumer: Optional[KafkaConsumer] = None
    
    def start(self) -> None:
        """Start consuming messages from Kafka"""
        logger.debug(f"Starting consumer for topic: {self.topic}")
        logger.debug(f"Supported events: {self.registry.get_supported_events()}")
        
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True
        )
        
        try:
            for message in self.consumer:
                self._process_message(message.value)
        except KeyboardInterrupt:
            logger.debug("Shutting down consumer...")
        finally:
            self.stop()
    
    def _process_message(self, event_data: dict) -> None:
        """Process a single message"""
        event_type = event_data.get('event')
        
        if not event_type:
            logger.warning(f"Message missing 'event' field: {event_data}")
            return
        
        handler = self.registry.get_handler(event_type)
        
        if handler:
            try:
                logger.debug(f"Processing event: {event_type}")
                handler.handle(event_data)
            except Exception as e:
                logger.error(f"Error handling event {event_type}: {e}", exc_info=True)
        else:
            logger.debug(f"No handler registered for event type: {event_type}")
    
    def stop(self) -> None:
        """Stop the consumer gracefully"""
        if self.consumer:
            self.consumer.close()
            logger.debug("Consumer closed")