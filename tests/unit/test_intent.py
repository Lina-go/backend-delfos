"""Tests for intent service."""

import pytest
from src.services.intent.classifier import IntentClassifier


@pytest.mark.asyncio
async def test_intent_classifier_nivel_puntual(settings):
    """Test intent classifier with nivel_puntual question."""
    classifier = IntentClassifier(settings)
    result = await classifier.classify("Cuál es el total de cuentas?")
    assert result["intent"] in ["nivel_puntual", "requiere_viz"]


@pytest.mark.asyncio
async def test_intent_classifier_requiere_viz(settings):
    """Test intent classifier with requiere_viz question."""
    classifier = IntentClassifier(settings)
    result = await classifier.classify("Cómo ha evolucionado el número de clientes?")
    assert result["intent"] in ["nivel_puntual", "requiere_viz"]

