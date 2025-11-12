"""
Handler: User Created
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import os
from pathlib import Path
from handlers.base import EventHandler
from typing import Dict, Any


class UserCreatedHandler(EventHandler):
    """Handles UserCreated events"""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        super().__init__()
    
    def get_event_type(self) -> str:
        """Return the event type this handler processes"""
        return "UserCreated"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Create an HTML email based on user creation data"""

        user_id = event_data.get('id')
        name = event_data.get('name')
        email = event_data.get('email')
        datetime = event_data.get('datetime')
        user_type_id = event_data.get('user_type_id', 1)

        current_file = Path(__file__)
        project_root = current_file.parent.parent
        
        # Choose template based on user type
        if user_type_id == 2 or user_type_id == 3:  # Employee (types 2 and 3)
            template_name = "welcome_employee_template.html"
            user_type = "employé"
        else:  # Client (user_type_id == 1 or default)
            template_name = "welcome_client_template.html"
            user_type = "client"
            
        with open(project_root / "templates" / template_name, 'r') as file:
            html_content = file.read()
            html_content = html_content.replace("{{user_id}}", str(user_id))
            html_content = html_content.replace("{{name}}", name)
            html_content = html_content.replace("{{email}}", email)
            html_content = html_content.replace("{{creation_date}}", datetime)
        
        filename = os.path.join(self.output_dir, f"welcome_{user_id}.html")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        self.logger.debug(f"Courriel HTML de bienvenue {user_type} généré à {name} (ID: {user_id}), {filename}")