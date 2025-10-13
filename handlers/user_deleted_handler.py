"""
Handler: User Deleted
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import os
from datetime import datetime
from handlers.base import EventHandler
from typing import Dict, Any


class UserDeletedHandler(EventHandler):
    """Handles UserDeleted events"""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        super().__init__()
    
    def get_event_type(self) -> str:
        return "UserDeleted"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        user_id = event_data.get('user_id')
        name = event_data.get('name', 'User')
        
        html_content = f""""""
        
        filename = os.path.join(self.output_dir, f"goodbye_{user_id}.html")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        self.logger.debug(f"Created goodbye message for user {name} (ID: {user_id}) at {filename}")