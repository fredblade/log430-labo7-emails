"""
Coolriel: Event-Driven Email Sender
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
import config
from consumers.user_event_history_consumer import UserEventHistoryConsumer
from logger import Logger
from consumers.user_event_consumer import UserEventConsumer
from handlers.handler_registry import HandlerRegistry
from handlers.user_created_handler import UserCreatedHandler
from handlers.user_deleted_handler import UserDeletedHandler

logger = Logger.get_instance("Coolriel")

def main():
    """Main entry point for the Coolriel service"""
    logger.info("Démarrage de Coolriel - Service de génération d'emails basé sur les événements")
    
    # Créer le registre des handlers
    registry = HandlerRegistry()
    registry.register(UserCreatedHandler(output_dir=config.OUTPUT_DIR))
    registry.register(UserDeletedHandler(output_dir=config.OUTPUT_DIR))
    logger.info("Handlers enregistrés avec succès")

    logger.info("Étape 1: Lecture de l'historique complet des événements...")
    consumer_service_history = UserEventHistoryConsumer(
        bootstrap_servers=config.KAFKA_HOST,
        topic=config.KAFKA_TOPIC,
        group_id=f"{config.KAFKA_GROUP_ID}-history",  # group_id distinct
        registry=registry,
    )
    logger.info("Démarrage du consommateur historique...")
    consumer_service_history.start() 
    logger.info("Lecture de l'historique terminée, passage au consommateur temps réel")

    logger.info("Étape 2: Démarrage de l'écoute des nouveaux événements...")
    consumer_service = UserEventConsumer(
        bootstrap_servers=config.KAFKA_HOST,
        topic=config.KAFKA_TOPIC,
        group_id=config.KAFKA_GROUP_ID,
        registry=registry,
    )
    logger.info("Démarrage du consommateur temps réel...")
    consumer_service.start()  

if __name__ == "__main__":
    main()
