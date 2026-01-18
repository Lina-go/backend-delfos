"""Greeting handler - responds to salutations without using the pipeline."""

from typing import Any


class GreetingHandler:
    """Handles greetings, farewells, and thank you messages."""

    RESPONSES = {
        "hola": "¡Hola! Soy Delfos, tu asistente de datos financieros de Colombia. ¿En qué te puedo ayudar?",
        "gracias": "¡Con gusto! Si necesitas más información sobre datos financieros, aquí estoy.",
        "chao": "¡Hasta luego! Que tengas un buen día.",
        "default": "¡Hola! ¿En qué te puedo ayudar con datos financieros?",
    }

    KEYWORDS = {
        "hola": ["hola", "buenos días", "buenas tardes", "buenas noches", "hey", "qué tal", "que tal"],
        "gracias": ["gracias", "thank", "te agradezco", "muchas gracias"],
        "chao": ["chao", "adiós", "adios", "bye", "hasta luego", "nos vemos"],
    }

    def handle(self, message: str) -> dict[str, Any]:
        """Handle greeting message and return response."""
        msg_lower = message.lower()

        response_key = "default"
        for key, keywords in self.KEYWORDS.items():
            if any(kw in msg_lower for kw in keywords):
                response_key = key
                break

        return {
            "patron": "greeting",
            "datos": [],
            "arquetipo": "NA",
            "visualizacion": "NO",
            "tipo_grafica": None,
            "imagen": None,
            "link_power_bi": None,
            "insight": self.RESPONSES[response_key],
        }