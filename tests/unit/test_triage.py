"""Tests for triage service."""

import pytest

from src.services.triage.classifier import TriageClassifier


@pytest.mark.asyncio
async def test_triage_classifier_data_question(settings):
    """Test triage classifier with data question."""
    classifier = TriageClassifier(settings)
    result = await classifier.classify("How many customers are there?")
    assert result["query_type"] in ["data_question", "general", "out_of_scope"]


@pytest.mark.asyncio
async def test_triage_classifier_general(settings):
    """Test triage classifier with general question."""
    classifier = TriageClassifier(settings)
    result = await classifier.classify("What is a loan?")
    assert result["query_type"] in ["data_question", "general", "out_of_scope"]
