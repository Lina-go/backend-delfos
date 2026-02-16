"""
Archetype definitions (A-K) for intent classification.

Chart resolution is embedded per archetype via `chart_rules`:
    A dict mapping (Temporality, is_plural) → ChartType | None
    where is_plural = subject_cardinality > 1
"""

import logging
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

from src.config.constants import Archetype, ChartType, Intent, PatternType

logger = logging.getLogger(__name__)


class Temporality(str, Enum):
    """Whether the question asks for a snapshot or a time series."""

    STATIC = "estatico"
    TEMPORAL = "temporal"


@dataclass(frozen=True)
class ArchetypeInfo:
    """Definition of a query archetype."""

    archetype: Archetype
    name: str
    template: str
    description: str
    examples: list[str]
    intent: Intent
    pattern_type: PatternType
    chart_rules: dict[tuple[Temporality, bool], ChartType | None] = field(default_factory=dict)

    def resolve_chart(
        self,
        temporality: Temporality = Temporality.STATIC,
        subject_cardinality: int = 1,
    ) -> ChartType | None:
        """Resolve chart type based on temporality and subject cardinality.

        Args:
            temporality: Whether the query is static or temporal.
            subject_cardinality: Number of subjects (1 = single, >1 = multiple).

        Returns:
            ChartType or None.
        """
        is_plural = subject_cardinality > 1
        return self.chart_rules.get((temporality, is_plural))

    def to_dict(self) -> dict[str, Any]:
        """Get all information for the archetype as a dictionary."""
        return asdict(self)

    def get_all_info_as_string(self) -> str:
        """Get all information for the archetype as a string."""
        return (
            f"Archetype: {self.archetype}\n"
            f"Name: {self.name}\n"
            f"Template: {self.template}\n"
            f"Description: {self.description}\n"
            f"Examples: {self.examples}"
        )


S, T = Temporality.STATIC, Temporality.TEMPORAL

ARCHETYPES: dict[Archetype, ArchetypeInfo] = {
    # ==========================================================================
    # Comparison (A–E)
    # ==========================================================================
    Archetype.ARCHETYPE_A: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_A,
        name="nivel_puntual",
        template="¿Cuál es el nivel/valor/cantidad de {métrica} para {conjunto/dimensión} en {período}?",
        description=(
            "Asks for a specific metric value at a specific time point. "
            "Also covers delta/variation questions and single aggregate counts "
            "(e.g. '¿Cuántos clientes tiene X?')."
        ),
        intent=Intent.NIVEL_PUNTUAL,
        pattern_type=PatternType.COMPARACION,
        chart_rules={
            (S, False): None,
            (S, True):  None,
            (T, False): ChartType.LINE,
            (T, True):  ChartType.LINE,
        },
        examples=[
            "¿Cuál es el saldo total de la cartera del sistema a la fecha de corte?",
            "¿Cuál fue la tasa de captación reportada por una entidad específica en un mes determinado?",
            "¿Cuál ha sido el cambio en participación de crédito a persona jurídica de cada banco durante los últimos seis meses?",
            "¿Cuántos clientes persona natural activos tiene AV Villas en Bogotá?",
        ],
    ),
    Archetype.ARCHETYPE_B: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_B,
        name="composicion",
        template=(
            "¿Qué porcentaje/porción/participación/contribución de {conjunto} "
            "corresponde a {categoría/actor} en {período}?"
        ),
        description=(
            "Asks how a total breaks down into EXHAUSTIVE categorical parts that SUM TO 100% "
            "(composition, contribution breakdown). Requires an explicit breakdown dimension "
            "(e.g. 'by product type', 'by segment'). "
            "DISAMBIGUATION vs C: If the question asks for market share of SPECIFIC ENTITIES "
            "(banks, groups) vs the total market, use C — entity shares are independent metrics, "
            "not exhaustive slices. "
            "B: '¿Qué % de la cartera es consumo vs vivienda vs comercial?' (exhaustive segments). "
            "C: '¿Cuál es la participación de mercado de cada banco del Grupo Aval?' (entity comparison)."
        ),
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.COMPARACION,
        chart_rules={
            (S, False): ChartType.PIE,
            (S, True):  ChartType.STACKED_BAR,
            (T, False): ChartType.STACKED_BAR,
            (T, True):  ChartType.STACKED_BAR,
        },
        examples=[
            "¿Qué porcentaje del saldo total de la cartera corresponde a cada tipo de producto?",
            "¿Cuál es la composición de créditos a persona natural por tipo de producto para el mercado en general?",
            "¿Cuál es la composición de créditos a persona natural por tipo de producto para los diez principales bancos?",
            "¿Cómo se compara la composición por plazos de depósitos de los diez principales bancos?",
            "¿Cómo ha evolucionado la composición de la cartera por tipo de producto durante el último año?",
        ],
    ),
    Archetype.ARCHETYPE_C: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_C,
        name="comparacion_directa",
        template="¿Cómo se compara {métrica} entre {grupo A} y {grupo B} en {período}?",
        description=(
            "Asks to compare a single metric between two or more specific groups or categories."
        ),
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.COMPARACION,
        chart_rules={
            (S, False): ChartType.BAR,
            (S, True):  ChartType.BAR,
            (T, False): ChartType.LINE,
            (T, True):  ChartType.LINE,
        },
        examples=[
            "¿Cómo se compara el saldo de la cartera de consumo frente a la cartera de vivienda?",
            "¿Cómo se comparan las tasas de captación entre bancos y compañías de financiamiento?",
            "Muestre el cambio histórico en tasa activa de los seis principales bancos.",
            "Muestre el histórico de tasa pasiva de los bancos del Grupo Aval.",
            "¿Cuál ha sido la participación de mercado en cartera de cada banco del Grupo Aval los últimos meses?",
            "¿Cómo ha evolucionado la cantidad de clientes PN del Grupo Aval entre 2023 y 2025?",
        ],
    ),
    Archetype.ARCHETYPE_D: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_D,
        name="ranking",
        template="¿Cuáles {unidades} presentan el mayor/menor {métrica} dentro de {conjunto} en {período}?",
        description=(
            "Asks to rank or order entities by a single metric. "
            "When temporal, resolves to LINE."
        ),
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.COMPARACION,
        chart_rules={
            (S, False): ChartType.BAR,
            (S, True):  ChartType.BAR,
            (T, False): ChartType.LINE,
            (T, True):  ChartType.LINE,
        },
        examples=[
            "¿Cuáles son las entidades con mayor saldo de cartera en el sistema financiero?",
            "¿Cuáles son las entidades que reportan las tasas de captación más altas en un período determinado?",
            "¿Cómo ha evolucionado la participación de mercado en créditos de los diez principales bancos?",
            "¿Cómo ha evolucionado la participación de mercado en créditos de vivienda de los diez principales bancos?",
        ],
    ),
    Archetype.ARCHETYPE_E: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_E,
        name="concentracion",
        template="¿Qué tan concentrado está {recurso/métrica} en {top-N/contrapartes} durante {período}?",
        description="Asks how concentrated a resource or metric is among top performers.",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.COMPARACION,
        chart_rules={
            (S, False): ChartType.PIE,
            (S, True):  ChartType.STACKED_BAR,
            (T, False): ChartType.STACKED_BAR,
            (T, True):  ChartType.STACKED_BAR,
        },
        examples=[
            "¿Qué tan concentrado está el saldo de la cartera en las cinco principales entidades del sistema?",
            "¿Qué tan concentrado está el mercado de captación en las principales entidades financieras?",
            "¿Cuál es la participación de mercado en saldos de depósitos de los diez principales bancos?",
        ],
    ),
    # ==========================================================================
    # Relationship (F–G)
    # ==========================================================================
    Archetype.ARCHETYPE_F: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_F,
        name="relacion_entre_variables",
        template="¿Cómo se relaciona {métrica A} con {métrica B} para {conjunto} en {período}?",
        description="Asks about the relationship or correlation between two metrics.",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.RELACION,
        examples=[
            "¿Cómo se relacionan las tasas de captación con el volumen captado por las entidades?",
            "¿Cómo se relaciona el saldo total de la cartera con el nivel de cartera vigente por producto?",
        ],
    ),
    Archetype.ARCHETYPE_G: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_G,
        name="sensibilidad_derivada",
        template="¿Cuál es la sensibilidad de {resultado} ante cambios en {variable} durante {período}?",
        description="Asks about sensitivity or how much one metric changes when another changes.",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.RELACION,
        examples=[
            "¿Qué tan sensible es el volumen captado ante cambios en las tasas de interés del mercado?",
            "¿Qué tan sensible es la cartera vigente ante variaciones en el saldo total de la cartera?",
        ],
    ),
    # ==========================================================================
    # Projection (H–I)
    # ==========================================================================
    Archetype.ARCHETYPE_H: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_H,
        name="descomposicion_cambio",
        template="¿Qué porción del cambio en {métrica} se explica por {driver A} vs {driver B} en {período}?",
        description=(
            "Asks what portion of a change is explained by different drivers. "
            "IMPORTANT: '¿Cómo ha evolucionado X?' is NOT H — that is temporal comparison (C/D). "
            "H is ONLY for decomposing what CAUSED a change (e.g. '¿qué parte del crecimiento se explica por...')."
        ),
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.PROYECCION,
        examples=[
            "¿Qué parte del crecimiento de la cartera se explica por consumo frente a cartera comercial?",
            "¿Qué parte del cambio en el volumen del mercado se explica por bancos frente a otras entidades?",
        ],
    ),
    Archetype.ARCHETYPE_I: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_I,
        name="escenario_what_if",
        template="Si {supuesto/condición}, ¿cuál sería el impacto en {resultado} durante {horizonte}?",
        description="Asks about the impact of a hypothetical assumption on an outcome.",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.PROYECCION,
        examples=[
            "Si las tasas de captación aumentan, ¿cuál sería el impacto esperado en el volumen captado en los próximos meses?",
            "Si la cartera vigente crece por encima del promedio histórico, ¿cuál sería el impacto en el saldo total de la cartera?",
        ],
    ),
    # ==========================================================================
    # Simulation (J–K)
    # ==========================================================================
    Archetype.ARCHETYPE_J: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_J,
        name="capacidad_dado_un_recurso",
        template="Dado {restricción/recurso}, ¿hasta qué nivel puede llegar {resultado} durante {horizonte}?",
        description="Asks for the maximum achievable level given a constraint.",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.SIMULACION,
        examples=[
            "Dado un límite de crecimiento de cartera, ¿hasta qué nivel podría llegar el saldo total en el próximo año?",
            "Dado un tope de fondeo disponible, ¿hasta qué nivel podría crecer el volumen captado manteniendo las tasas actuales?",
        ],
    ),
    Archetype.ARCHETYPE_K: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_K,
        name="requerimiento_inverso",
        template="¿Qué nivel de {variable/palanca} se requiere para alcanzar {objetivo} en {horizonte}?",
        description="Asks what level of a variable is needed to reach a specific objective.",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.SIMULACION,
        examples=[
            "¿Qué crecimiento de la cartera vigente se requiere para alcanzar un saldo objetivo en un año?",
            "¿Qué nivel de tasas de captación sería necesario para alcanzar un volumen objetivo del mercado en el próximo trimestre?",
        ],
    ),
}


############################################################
# Helper functions
############################################################


def get_all_archetypes() -> list[ArchetypeInfo]:
    """Get all archetypes."""
    return list(ARCHETYPES.values())


def get_archetype_info(archetype: Archetype) -> ArchetypeInfo:
    """Get archetype info by archetype."""
    return ARCHETYPES[archetype]


def get_all_archetypes_for_prompt() -> str:
    """Get all archetypes formatted as a string for prompt injection."""
    return "\n\n".join(info.get_all_info_as_string() for info in ARCHETYPES.values())


def get_archetypes_by_pattern_type(pattern_type: PatternType) -> list[ArchetypeInfo]:
    """Get all archetypes matching a given pattern type."""
    return [a for a in ARCHETYPES.values() if a.pattern_type == pattern_type]


def get_archetypes_by_intent(intent: Intent) -> list[ArchetypeInfo]:
    """Get all archetypes matching a given intent."""
    return [a for a in ARCHETYPES.values() if a.intent == intent]


def get_archetype_name(archetype_letter: str) -> str:
    """Get archetype name by letter. Falls back to 'nivel_puntual' for invalid input."""
    try:
        archetype_enum = Archetype(archetype_letter)
        return ARCHETYPES[archetype_enum].name
    except (ValueError, KeyError):
        logger.warning(
            "Invalid archetype letter '%s', defaulting to 'A' (nivel_puntual)",
            archetype_letter,
        )
        return ARCHETYPES[Archetype.ARCHETYPE_A].name


def get_chart_type_for_archetype(
    archetype: Archetype,
    temporality: Temporality = Temporality.STATIC,
    subject_cardinality: int = 1,
) -> ChartType | None:
    """Get chart type using the archetype's embedded resolution rules.

    Args:
        archetype: Archetype enum.
        temporality: Whether the query is static or temporal.
        subject_cardinality: Number of subjects (1 = single, >1 = multiple).

    Returns:
        ChartType or None.
    """
    return ARCHETYPES[archetype].resolve_chart(temporality, subject_cardinality)


def get_archetype_letter_by_name(name: str) -> Archetype | None:
    """Get archetype enum by name. Returns None if not found."""
    for archetype, info in ARCHETYPES.items():
        if info.name == name:
            return archetype
    return None
