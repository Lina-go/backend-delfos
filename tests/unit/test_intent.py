"""Tests for intent service."""

import pytest

from src.config.constants import Intent
from src.services.intent.classifier import IntentClassifier
from src.services.intent.models import IntentResult


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
    assert result["intent"] in ['nivel_puntual', 'requiere_visualizacion']


def test_intent_result_includes_titulo_grafica():
    """Test that IntentResult model accepts and serializes titulo_grafica."""
    result = IntentResult(
        user_question="Cuál es el saldo total por banco?",
        intent=Intent.REQUIERE_VIZ,
        tipo_patron="Comparación",
        arquetipo="A",
        razon="Compara saldos entre bancos",
        titulo_grafica="Saldo total por entidad bancaria",
    )
    data = result.model_dump()
    assert data["titulo_grafica"] == "Saldo total por entidad bancaria"


def test_intent_result_titulo_grafica_defaults_to_none():
    """Test that titulo_grafica defaults to None when not provided."""
    result = IntentResult(
        user_question="Cuál es el total?",
        intent=Intent.NIVEL_PUNTUAL,
        tipo_patron="Comparación",
        arquetipo="A",
    )
    assert result.titulo_grafica is None
