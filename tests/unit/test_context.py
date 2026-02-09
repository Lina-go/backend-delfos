"""Tests for conversation context management."""

from src.orchestrator.context import ConversationContext, ConversationStore


def test_conversation_context_has_last_title():
    """Test that ConversationContext includes last_title field."""
    ctx = ConversationContext()
    assert ctx.last_title is None


def test_conversation_store_persists_title():
    """Test that ConversationStore.update() saves titulo_grafica as last_title."""
    user_id = "test_user_titulo"

    ConversationStore.update(
        user_id=user_id,
        query="Cuál es el saldo por banco?",
        sql="SELECT banco, SUM(saldo) FROM cuentas GROUP BY banco",
        results=[{"banco": "A", "saldo": 100}],
        response={"insight": "Banco A tiene saldo 100"},
        chart_type="bar",
        title="Saldo total por entidad bancaria",
    )

    ctx = ConversationStore.get(user_id)
    assert ctx.last_title == "Saldo total por entidad bancaria"

    # Cleanup
    ConversationStore.clear(user_id)


def test_conversation_store_title_defaults_to_none():
    """Test that last_title is None when not passed to update()."""
    user_id = "test_user_no_titulo"

    ConversationStore.update(
        user_id=user_id,
        query="Cuál es el total?",
        sql="SELECT COUNT(*) FROM cuentas",
        results=[{"count": 50}],
        response={"insight": "Hay 50 cuentas"},
    )

    ctx = ConversationStore.get(user_id)
    assert ctx.last_title is None

    # Cleanup
    ConversationStore.clear(user_id)
