"""Tests for SubType enum, chart resolution, and backward compatibility."""

import pytest

from src.config.constants import ChartType, Intent
from src.config.subtypes import (
    BLOCKED_SUBTYPES,
    VIZ_SUBTYPES,
    SubType,
    get_chart_type_for_subtype,
    get_legacy_archetype,
    get_subtype_from_string,
    is_blocked,
)
from src.services.intent.models import IntentResult


# ---------------------------------------------------------------------------
# 1. SubType enum basics
# ---------------------------------------------------------------------------


class TestSubTypeEnum:
    """Validate SubType enum parsing and string identity."""

    def test_all_members_are_strings(self):
        for st in SubType:
            assert isinstance(st.value, str)

    def test_parse_lowercase(self):
        assert SubType("valor_puntual") == SubType.VALOR_PUNTUAL

    def test_parse_via_helper(self):
        assert get_subtype_from_string("ranking") == SubType.RANKING

    def test_parse_strips_whitespace(self):
        assert get_subtype_from_string("  ranking  ") == SubType.RANKING

    def test_parse_case_insensitive(self):
        assert get_subtype_from_string("RANKING") == SubType.RANKING

    def test_parse_invalid_returns_none(self):
        assert get_subtype_from_string("invalid_subtype") is None

    def test_parse_empty_returns_none(self):
        assert get_subtype_from_string("") is None

    def test_total_subtypes_is_16(self):
        assert len(SubType) == 16

    def test_blocked_count_is_6(self):
        assert len(BLOCKED_SUBTYPES) == 6

    def test_viz_count_is_9(self):
        assert len(VIZ_SUBTYPES) == 9


# ---------------------------------------------------------------------------
# 2. Deterministic chart resolution
# ---------------------------------------------------------------------------


class TestChartResolution:
    """Verify SubType -> ChartType mapping is deterministic and correct."""

    @pytest.mark.parametrize(
        "sub_type, expected_chart",
        [
            (SubType.VALOR_PUNTUAL, None),
            (SubType.COMPARACION_DIRECTA, ChartType.BAR),
            (SubType.RANKING, ChartType.BAR),
            (SubType.CONCENTRACION, ChartType.PIE),
            (SubType.COMPOSICION_SIMPLE, ChartType.PIE),
            (SubType.COMPOSICION_COMPARADA, ChartType.STACKED_BAR),
            (SubType.TENDENCIA_SIMPLE, ChartType.LINE),
            (SubType.TENDENCIA_COMPARADA, ChartType.LINE),
            (SubType.EVOLUCION_COMPOSICION, ChartType.STACKED_BAR),
            (SubType.EVOLUCION_CONCENTRACION, ChartType.STACKED_BAR),
        ],
    )
    def test_active_subtype_chart(self, sub_type: SubType, expected_chart: ChartType | None):
        assert get_chart_type_for_subtype(sub_type) == expected_chart

    @pytest.mark.parametrize(
        "sub_type",
        [
            SubType.RELACION,
            SubType.SENSIBILIDAD,
            SubType.DESCOMPOSICION_CAMBIO,
            SubType.WHAT_IF,
            SubType.CAPACIDAD,
            SubType.REQUERIMIENTO,
        ],
    )
    def test_blocked_subtype_raises(self, sub_type: SubType):
        assert is_blocked(sub_type)
        with pytest.raises(ValueError, match="blocked"):
            get_chart_type_for_subtype(sub_type)

    def test_all_chart_types_covered(self):
        """Every ChartType enum value is produced by at least one SubType."""
        produced = {
            get_chart_type_for_subtype(st)
            for st in SubType
            if not is_blocked(st) and get_chart_type_for_subtype(st) is not None
        }
        for ct in ChartType:
            assert ct in produced, f"ChartType.{ct.name} not produced by any SubType"


# ---------------------------------------------------------------------------
# 3. Backward compatibility: legacy archetype mapping
# ---------------------------------------------------------------------------


class TestLegacyArchetype:
    """Verify SubType -> legacy archetype letter mapping."""

    @pytest.mark.parametrize(
        "sub_type, expected_letter",
        [
            (SubType.VALOR_PUNTUAL, "A"),
            (SubType.COMPOSICION_SIMPLE, "B"),
            (SubType.COMPOSICION_COMPARADA, "B"),
            (SubType.COMPARACION_DIRECTA, "C"),
            (SubType.RANKING, "D"),
            (SubType.CONCENTRACION, "E"),
            (SubType.TENDENCIA_SIMPLE, "C"),
            (SubType.TENDENCIA_COMPARADA, "C"),
            (SubType.EVOLUCION_COMPOSICION, "B"),
            (SubType.EVOLUCION_CONCENTRACION, "E"),
            (SubType.RELACION, "F"),
            (SubType.SENSIBILIDAD, "G"),
            (SubType.DESCOMPOSICION_CAMBIO, "H"),
            (SubType.WHAT_IF, "I"),
            (SubType.CAPACIDAD, "J"),
            (SubType.REQUERIMIENTO, "K"),
        ],
    )
    def test_legacy_archetype(self, sub_type: SubType, expected_letter: str):
        assert get_legacy_archetype(sub_type) == expected_letter


# ---------------------------------------------------------------------------
# 4. IntentResult model with sub_type auto-population
# ---------------------------------------------------------------------------


class TestIntentResultModel:
    """Verify IntentResult auto-populates legacy fields from sub_type."""

    def test_valor_puntual_auto_populates(self):
        result = IntentResult(
            user_question="Cuál es el saldo total?",
            intent=Intent.NIVEL_PUNTUAL,
            sub_type="valor_puntual",
        )
        assert result.arquetipo == "A"
        assert result.temporality == "estatico"
        assert result.tipo_patron == "Comparacion"

    def test_tendencia_simple_auto_populates(self):
        result = IntentResult(
            user_question="Cómo ha evolucionado la cartera?",
            intent=Intent.REQUIERE_VIZ,
            sub_type="tendencia_simple",
        )
        assert result.arquetipo == "C"
        assert result.temporality == "temporal"
        assert result.tipo_patron == "Comparacion"

    def test_composicion_simple_auto_populates(self):
        result = IntentResult(
            user_question="Cuál es la distribución por tipo?",
            intent=Intent.REQUIERE_VIZ,
            sub_type="composicion_simple",
        )
        assert result.arquetipo == "B"
        assert result.temporality == "estatico"
        assert result.tipo_patron == "Comparacion"

    def test_relacion_auto_populates(self):
        result = IntentResult(
            user_question="Cómo se relaciona X con Y?",
            intent=Intent.REQUIERE_VIZ,
            sub_type="relacion",
        )
        assert result.arquetipo == "F"
        assert result.temporality == "estatico"
        assert result.tipo_patron == "Relacion"

    def test_sub_type_normalized_to_lowercase(self):
        result = IntentResult(
            user_question="Test",
            intent=Intent.NIVEL_PUNTUAL,
            sub_type="  VALOR_PUNTUAL  ",
        )
        assert result.sub_type == "valor_puntual"

    def test_explicit_legacy_fields_not_overridden(self):
        result = IntentResult(
            user_question="Test",
            intent=Intent.REQUIERE_VIZ,
            sub_type="ranking",
            tipo_patron="CustomPattern",
            arquetipo="Z",
            temporality="custom_temp",
        )
        assert result.tipo_patron == "CustomPattern"
        assert result.arquetipo == "Z"
        assert result.temporality == "custom_temp"

    def test_model_dump_includes_sub_type(self):
        result = IntentResult(
            user_question="Test",
            intent=Intent.NIVEL_PUNTUAL,
            sub_type="valor_puntual",
        )
        data = result.model_dump()
        assert "sub_type" in data
        assert data["sub_type"] == "valor_puntual"


# ---------------------------------------------------------------------------
# 5. Ground truth dataset -- 33 queries with expected sub_type and chart
# ---------------------------------------------------------------------------


_GROUND_TRUTH = [
    # valor_puntual (None)
    ("Cuál es el saldo total de la cartera del sistema?", "valor_puntual", None),
    ("Cuántos clientes PN activos tiene AV Villas?", "valor_puntual", None),
    ("Cuánto cambió la participación de mercado en el último periodo?", "valor_puntual", None),
    # comparacion_directa (BAR)
    ("Cómo se compara el saldo de ahorro entre BBOG, BPOP, BOCC y BAVV?", "comparacion_directa", ChartType.BAR),
    ("Cuántos clientes hay por rango de edad?", "comparacion_directa", ChartType.BAR),
    ("Compara los saldos hipotecarios del Grupo Aval con los de la competencia", "comparacion_directa", ChartType.BAR),
    # ranking (BAR)
    ("Cuáles son los top 5 bancos por saldo de cartera?", "ranking", ChartType.BAR),
    ("Qué banco tiene la mayor cantidad de clientes con calificación A?", "ranking", ChartType.BAR),
    ("Top 10 entidades por captación en CDT", "ranking", ChartType.BAR),
    # concentracion (PIE)
    ("Qué tan concentrado está el saldo de la cartera en las 5 principales entidades?", "concentracion", ChartType.PIE),
    ("Cuál es la participación de mercado en depósitos de los 10 principales bancos?", "concentracion", ChartType.PIE),
    # composicion_simple (PIE)
    ("Cuál es la distribución de clientes PN por rango de edad?", "composicion_simple", ChartType.PIE),
    ("Qué porcentaje de la cartera corresponde a cada tipo de producto?", "composicion_simple", ChartType.PIE),
    ("Cuál es la composición por género de los clientes?", "composicion_simple", ChartType.PIE),
    # composicion_comparada (STACKED_BAR)
    ("Cuál es la composición por calificación de riesgo para cada banco del Grupo Aval?", "composicion_comparada", ChartType.STACKED_BAR),
    ("Cómo se compara la composición por plazos de depósitos de los 10 principales bancos?", "composicion_comparada", ChartType.STACKED_BAR),
    # tendencia_simple (LINE)
    ("Cómo ha evolucionado la cartera de consumo en los últimos 6 meses?", "tendencia_simple", ChartType.LINE),
    ("Histórico de participación de mercado de AV Villas en depósitos PN", "tendencia_simple", ChartType.LINE),
    ("Cuál es la tendencia del saldo de cartera hipotecaria del BBOG?", "tendencia_simple", ChartType.LINE),
    # tendencia_comparada (LINE)
    ("Evolución de participación de mercado de los 10 principales bancos", "tendencia_comparada", ChartType.LINE),
    ("Histórico de tasa pasiva de los bancos del Grupo Aval", "tendencia_comparada", ChartType.LINE),
    ("Evolución del saldo de cartera y depósitos del Banco de Occidente", "tendencia_comparada", ChartType.LINE),
    # evolucion_composicion (STACKED_BAR)
    ("Cómo ha evolucionado la composición de la cartera por tipo de producto?", "evolucion_composicion", ChartType.STACKED_BAR),
    ("Composición porcentual de la cartera del BBOG por tipo de crédito cada mes", "evolucion_composicion", ChartType.STACKED_BAR),
    ("Evolución del saldo total y por tipo de depósito del Grupo Aval", "evolucion_composicion", ChartType.STACKED_BAR),
    # evolucion_concentracion (STACKED_BAR)
    ("Cómo ha evolucionado la concentración de mercado en las principales entidades?", "evolucion_concentracion", ChartType.STACKED_BAR),
    # Blocked subtypes
    ("Cómo se relaciona el saldo con la tasa de interés?", "relacion", None),
    ("Qué tan sensible es la cartera ante cambios en la tasa?", "sensibilidad", None),
    ("Qué parte del crecimiento se explica por consumo vs hipotecario?", "descomposicion_cambio", None),
    ("Si las tasas suben 2%, cuál sería el impacto en la cartera?", "what_if", None),
    ("Dado el capital actual, hasta qué nivel puede llegar la cartera?", "capacidad", None),
    ("Qué se requiere para alcanzar una participación del 20%?", "requerimiento", None),
]


class TestGroundTruthDataset:
    """Validate that each ground-truth query maps to the correct chart type."""

    @pytest.mark.parametrize(
        "question, expected_sub_type, expected_chart",
        _GROUND_TRUTH,
        ids=[f"{i:02d}_{row[1]}" for i, row in enumerate(_GROUND_TRUTH)],
    )
    def test_chart_resolution(
        self,
        question: str,
        expected_sub_type: str,
        expected_chart: ChartType | None,
    ):
        st = get_subtype_from_string(expected_sub_type)
        assert st is not None, f"Invalid sub_type: {expected_sub_type}"

        if is_blocked(st):
            # Blocked subtypes should raise
            with pytest.raises(ValueError):
                get_chart_type_for_subtype(st)
        else:
            assert get_chart_type_for_subtype(st) == expected_chart


# ---------------------------------------------------------------------------
# 6. MVP production dataset -- 30 queries
# ---------------------------------------------------------------------------


_MVP_QUERIES = [
    # valor_puntual
    ("Cuál es el saldo total de cartera del BBOG?", "valor_puntual", None),
    ("Cuántas cuentas de ahorro tiene el Banco Popular?", "valor_puntual", None),
    ("Cuál es la tasa promedio de CDT del sistema?", "valor_puntual", None),
    ("Cuántos clientes tiene Bancolombia?", "valor_puntual", None),
    ("Cuál es el índice de morosidad del Banco de Occidente?", "valor_puntual", None),
    # comparacion_directa
    ("Compara el saldo de cartera entre BBOG y Bancolombia", "comparacion_directa", ChartType.BAR),
    ("Cuántos clientes tienen los bancos del Grupo Aval vs competencia?", "comparacion_directa", ChartType.BAR),
    ("Cuál es el saldo por tipo de depósito de AV Villas?", "comparacion_directa", ChartType.BAR),
    # ranking
    ("Top 5 bancos por número de clientes", "ranking", ChartType.BAR),
    ("Cuáles son las 3 entidades con mayor cartera vencida?", "ranking", ChartType.BAR),
    ("Qué banco tiene la mayor tasa de captación?", "ranking", ChartType.BAR),
    # concentracion
    ("Qué tan concentrado está el mercado de CDT?", "concentracion", ChartType.PIE),
    ("Participación de mercado en ahorro de los top 5", "concentracion", ChartType.PIE),
    # composicion_simple
    ("Distribución de la cartera del BBOG por tipo de crédito", "composicion_simple", ChartType.PIE),
    ("Composición de los depósitos del sistema por tipo", "composicion_simple", ChartType.PIE),
    ("Qué porcentaje de clientes son PN vs PJ?", "composicion_simple", ChartType.PIE),
    # composicion_comparada
    ("Composición por calificación de cada banco del Grupo Aval", "composicion_comparada", ChartType.STACKED_BAR),
    ("Distribución por tipo de crédito para los top 5 bancos", "composicion_comparada", ChartType.STACKED_BAR),
    # tendencia_simple
    ("Evolución de la cartera de consumo del BBOG en los últimos 12 meses", "tendencia_simple", ChartType.LINE),
    ("Histórico del saldo de ahorro de AV Villas", "tendencia_simple", ChartType.LINE),
    ("Tendencia de la tasa DTF en los últimos 2 años", "tendencia_simple", ChartType.LINE),
    # tendencia_comparada
    ("Evolución de cartera de los bancos del Grupo Aval últimos 6 meses", "tendencia_comparada", ChartType.LINE),
    ("Histórico comparado de tasas de captación BBOG vs Bancolombia", "tendencia_comparada", ChartType.LINE),
    ("Cómo han evolucionado los depósitos de los top 5 bancos?", "tendencia_comparada", ChartType.LINE),
    # evolucion_composicion
    ("Evolución de la composición de cartera por tipo cada trimestre", "evolucion_composicion", ChartType.STACKED_BAR),
    ("Cómo ha cambiado la distribución de depósitos por tipo en el último año?", "evolucion_composicion", ChartType.STACKED_BAR),
    # evolucion_concentracion
    ("Evolución de la concentración de mercado en cartera mes a mes", "evolucion_concentracion", ChartType.STACKED_BAR),
    # Safe confusions (same chart type)
    ("Ranking de los 10 principales bancos por cartera", "ranking", ChartType.BAR),
    ("Comparación del saldo de los top 5 bancos", "comparacion_directa", ChartType.BAR),
    ("Tendencia de cartera del BBOG y Bancolombia", "tendencia_comparada", ChartType.LINE),
]


class TestMVPDataset:
    """Validate MVP production queries map to correct chart types."""

    @pytest.mark.parametrize(
        "question, expected_sub_type, expected_chart",
        _MVP_QUERIES,
        ids=[f"{i:02d}_{row[1]}" for i, row in enumerate(_MVP_QUERIES)],
    )
    def test_chart_resolution(
        self,
        question: str,
        expected_sub_type: str,
        expected_chart: ChartType | None,
    ):
        st = get_subtype_from_string(expected_sub_type)
        assert st is not None, f"Invalid sub_type: {expected_sub_type}"
        assert get_chart_type_for_subtype(st) == expected_chart
