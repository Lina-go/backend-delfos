"""Hierarchical classification: SubType to ChartType mapping."""

from enum import Enum

from src.config.constants import ChartType


class SubType(str, Enum):
    """Analytical sub-types -- each maps to exactly one ChartType."""

    # -- Static comparisons ------------------------------------------------
    VALOR_PUNTUAL = "valor_puntual"
    """Single numeric answer, no chart. Signals: 'cual es', 'cuanto', 'cuantos'."""

    COMPARACION_DIRECTA = "comparacion_directa"
    """Compare a metric across categories (absolute values)."""

    RANKING = "ranking"
    """Order entities by a metric (top-N, mayor/menor)."""

    CONCENTRACION = "concentracion"
    """How concentrated a metric is among top performers."""

    COMPOSICION_SIMPLE = "composicion_simple"
    """Parts of a whole for ONE entity/total (exhaustive, sum to 100%)."""

    COMPOSICION_COMPARADA = "composicion_comparada"
    """Parts of a whole for MULTIPLE entities (side-by-side breakdowns)."""

    # -- Temporal (evolution) -----------------------------------------------
    TENDENCIA_SIMPLE = "tendencia_simple"
    """One metric evolving over time (single series)."""

    TENDENCIA_COMPARADA = "tendencia_comparada"
    """Multiple entities/metrics evolving over time (multiple series)."""

    EVOLUCION_COMPOSICION = "evolucion_composicion"
    """Composition (parts of whole) changing over time."""

    EVOLUCION_CONCENTRACION = "evolucion_concentracion"
    """Concentration changing over time."""

    # -- Relationship --------------------------------------------------------
    RELACION = "relacion"
    """Correlation between two variables for the same subjects (scatter plot)."""

    COVARIACION = "covariacion"
    """Temporal evolution of the relationship between two variables."""

    # -- Blocked (not yet supported) ----------------------------------------
    SENSIBILIDAD = "sensibilidad"
    """Sensitivity/elasticity analysis."""

    DESCOMPOSICION_CAMBIO = "descomposicion_cambio"
    """Decomposing what caused a change (waterfall)."""

    WHAT_IF = "what_if"
    """Hypothetical scenario simulation."""

    CAPACIDAD = "capacidad"
    """Maximum achievable given a constraint."""

    REQUERIMIENTO = "requerimiento"
    """Reverse: what input is needed to reach an objective."""


# -------------------------------------------------------------------------
# Deterministic chart resolution: SubType -> ChartType | None
# -------------------------------------------------------------------------

BLOCKED_SUBTYPES: frozenset[SubType] = frozenset({
    SubType.SENSIBILIDAD,
    SubType.DESCOMPOSICION_CAMBIO,
    SubType.WHAT_IF,
    SubType.CAPACIDAD,
    SubType.REQUERIMIENTO,
})

_SUBTYPE_CHART_MAP: dict[SubType, ChartType | None] = {
    SubType.VALOR_PUNTUAL:            None,
    SubType.COMPARACION_DIRECTA:      ChartType.BAR,
    SubType.RANKING:                  ChartType.BAR,
    SubType.CONCENTRACION:            ChartType.PIE,
    SubType.COMPOSICION_SIMPLE:       ChartType.PIE,
    SubType.COMPOSICION_COMPARADA:    ChartType.STACKED_BAR,
    SubType.TENDENCIA_SIMPLE:         ChartType.LINE,
    SubType.TENDENCIA_COMPARADA:      ChartType.LINE,
    SubType.EVOLUCION_COMPOSICION:    ChartType.STACKED_BAR,
    SubType.EVOLUCION_CONCENTRACION:  ChartType.STACKED_BAR,
    SubType.RELACION:                 ChartType.SCATTER,
    SubType.COVARIACION:              ChartType.SCATTER,
}

VIZ_SUBTYPES: frozenset[SubType] = frozenset(
    st for st, chart in _SUBTYPE_CHART_MAP.items() if chart is not None
)


def is_blocked(sub_type: SubType) -> bool:
    """Return True if the sub-type is not yet supported."""
    return sub_type in BLOCKED_SUBTYPES


def get_chart_type_for_subtype(sub_type: SubType) -> ChartType | None:
    """Resolve chart type from a SubType.

    Raises:
        ValueError: If the sub_type is blocked.
    """
    if sub_type in BLOCKED_SUBTYPES:
        raise ValueError(
            f"SubType '{sub_type.value}' is blocked -- not yet supported. "
            f"Check is_blocked() before calling this function."
        )
    return _SUBTYPE_CHART_MAP.get(sub_type)


def get_subtype_from_string(value: str) -> SubType | None:
    """Parse a sub_type string into SubType enum. Returns None if invalid."""
    try:
        return SubType(value.lower().strip())
    except ValueError:
        return None


# -------------------------------------------------------------------------
# Backward compatibility: map SubType back to legacy Archetype letter
# -------------------------------------------------------------------------

_SUBTYPE_TO_LEGACY_ARCHETYPE: dict[SubType, str] = {
    SubType.VALOR_PUNTUAL:            "A",
    SubType.COMPOSICION_SIMPLE:       "B",
    SubType.COMPOSICION_COMPARADA:    "B",
    SubType.COMPARACION_DIRECTA:      "C",
    SubType.RANKING:                  "D",
    SubType.CONCENTRACION:            "E",
    SubType.TENDENCIA_SIMPLE:         "C",
    SubType.TENDENCIA_COMPARADA:      "C",
    SubType.EVOLUCION_COMPOSICION:    "B",
    SubType.EVOLUCION_CONCENTRACION:  "E",
    SubType.RELACION:                 "F",
    SubType.COVARIACION:              "F",
    SubType.SENSIBILIDAD:             "G",
    SubType.DESCOMPOSICION_CAMBIO:    "H",
    SubType.WHAT_IF:                  "I",
    SubType.CAPACIDAD:                "J",
    SubType.REQUERIMIENTO:            "K",
}


def get_legacy_archetype(sub_type: SubType) -> str:
    """Map SubType to legacy archetype letter for backward compatibility."""
    return _SUBTYPE_TO_LEGACY_ARCHETYPE.get(sub_type, "A")


# -------------------------------------------------------------------------
# Temporal classification and pattern type (single source of truth)
# -------------------------------------------------------------------------

TEMPORAL_SUBTYPES: frozenset[SubType] = frozenset({
    SubType.TENDENCIA_SIMPLE,
    SubType.TENDENCIA_COMPARADA,
    SubType.EVOLUCION_COMPOSICION,
    SubType.EVOLUCION_CONCENTRACION,
    SubType.COVARIACION,
})

_SUBTYPE_TO_PATTERN: dict[SubType, str] = {
    SubType.RELACION:              "Relacion",
    SubType.COVARIACION:           "Relacion",
    SubType.SENSIBILIDAD:          "Relacion",
    SubType.DESCOMPOSICION_CAMBIO: "Proyeccion",
    SubType.WHAT_IF:               "Proyeccion",
    SubType.CAPACIDAD:             "Simulacion",
    SubType.REQUERIMIENTO:         "Simulacion",
}


def get_temporality(sub_type: SubType) -> str:
    """Return 'temporal' or 'estatico' based on the sub-type."""
    return "temporal" if sub_type in TEMPORAL_SUBTYPES else "estatico"


def get_pattern_type(sub_type: SubType) -> str:
    """Return the pattern type for a sub-type (lowercase for PipelineState)."""
    return _SUBTYPE_TO_PATTERN.get(sub_type, "Comparacion").lower()


def get_legacy_pattern_type(sub_type: SubType) -> str:
    """Return the legacy pattern type for a sub-type (capitalized for IntentResult)."""
    return _SUBTYPE_TO_PATTERN.get(sub_type, "Comparacion")
