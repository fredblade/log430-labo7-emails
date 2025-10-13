"""
Coolriel: Event-Driven Email Sender
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

from logger import Logger
from consumer_service import EventConsumerService
from handler_registry import HandlerRegistry
from handlers.user_created_handler import UserCreatedHandler
from handlers.user_deleted_handler import UserDeletedHandler

logger = Logger.get_instance("Coolriel")

def main():
    """Main entry point for the consumer service"""
    
    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
    KAFKA_TOPIC = 'user-events'
    KAFKA_GROUP_ID = 'user-event-consumer-group'
    OUTPUT_DIR = 'output'
    
    # Create handler registry
    registry = HandlerRegistry()
    
    # Register handlers
    registry.register(UserCreatedHandler(output_dir=OUTPUT_DIR))
    registry.register(UserDeletedHandler(output_dir=OUTPUT_DIR))
    
    # Create and start consumer service
    consumer_service = EventConsumerService(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
        group_id=KAFKA_GROUP_ID,
        registry=registry
    )
    
    logger.debug("Starting User Event Consumer Service...")
    consumer_service.start()


if __name__ == "__main__":
    main()
