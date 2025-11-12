"""
Kafka Historical User Event Consumer (Event Sourcing)
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import json
from logger import Logger
from typing import Optional
from kafka import KafkaConsumer
from handlers.handler_registry import HandlerRegistry

class UserEventHistoryConsumer:
    """A consumer that starts reading Kafka events from the earliest point from a given topic"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        registry: HandlerRegistry
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.registry = registry
        self.auto_offset_reset = "earliest"  # Pour lire depuis le début
        self.consumer: Optional[KafkaConsumer] = None
        self.logger = Logger.get_instance("UserEventHistoryConsumer")
        self.event_history = []  # Pour stocker l'historique des événements
    
    def start(self) -> None:
        """Start consuming messages from Kafka"""
        self.logger.info(f"Démarrer un consommateur historique : {self.group_id}")

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",  # Lire depuis le début (earliest), pas la fin (latest)
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True,
            consumer_timeout_ms=10000  # Timeout pour éviter d'attendre indéfiniment
        )
        
        try:
            self.logger.info("Lecture de l'historique des événements depuis le début...")
            for message in self.consumer:
                self._process_historical_message(message.value)
                
        except KeyboardInterrupt:
            self.logger.info("Arrêt demandé par l'utilisateur")
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture de l'historique: {e}", exc_info=True)
        finally:
            self._save_history_to_file()
            self.stop()

    def _process_historical_message(self, event_data: dict) -> None:
        """Process a single historical message"""
        event_type = event_data.get('event')
        
        if not event_type:
            self.logger.warning(f"Message historique manquant le champ 'event': {event_data}")
            return

        self.event_history.append(event_data)
        self.logger.debug(f"Événement historique traité : {event_type} (Total: {len(self.event_history)})")
        
        handler = self.registry.get_handler(event_type)
        
        if handler:
            try:
                handler.handle(event_data)
            except Exception as e:
                self.logger.error(f"Erreur lors du traitement de l'événement historique {event_type}: {e}", exc_info=True)
        else:
            self.logger.debug(f"Aucun handler enregistré pour le type historique : {event_type}")

    def _save_history_to_file(self) -> None:
        """Save the event history to a JSON file using json.dumps"""
        try:
            import os
            
            # Créer le répertoire output s'il n'existe pas
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            
            history_file = os.path.join(output_dir, "user_events_history.json")
            
            # Préparer les données à sauvegarder
            history_data = {
                "total_events": len(self.event_history),
                "consumer_group_id": self.group_id,
                "topic": self.topic,
                "events": self.event_history
            }
            
            # Utiliser json.dumps pour sérialiser les données en JSON
            json_content = json.dumps(history_data, indent=2, ensure_ascii=False)
            
            # Écrire le contenu JSON dans le fichier
            with open(history_file, 'w', encoding='utf-8') as f:
                f.write(json_content)
            
            self.logger.info(f"Historique de {len(self.event_history)} événements sauvegardé dans {history_file}")
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la sauvegarde de l'historique: {e}", exc_info=True)

    def stop(self) -> None:
        """Stop the consumer gracefully"""
        if self.consumer:
            self.consumer.close()
            self.logger.info("Consommateur historique arrêté!")