"""
Archetype definitions (A-N) for intent classification.
"""

from dataclasses import asdict, dataclass
from typing import Any

from src.config.constants import Archetype, ChartType, Intent, PatternType


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
    default_chart: ChartType | None = None

    def to_dict(self) -> dict[str, Any]:
        """Get all information for the archetype as a dictionary."""
        return asdict(self)

    def get_all_info_as_string(self) -> str:
        """Get all information for the archetype as a string."""
        return f"Archetype: {self.archetype}\nName: {self.name}\nTemplate: {self.template}\nDescription: {self.description}\nExamples: {self.examples}"


ARCHETYPES: dict[Archetype, ArchetypeInfo] = {
    # ==========================================================================
    # Comparison (A–H)
    # ==========================================================================
    Archetype.ARCHETYPE_A: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_A,
        name="nivel_puntual",
        template="¿Cuál es el nivel/valor de {métrica} para {conjunto/dimensión} en {período}?",
        description="Asks for a specific metric value at a specific time point",
        intent=Intent.NIVEL_PUNTUAL,
        pattern_type=PatternType.COMPARACION,
        default_chart=None,
        examples=[
            "¿Cuál es el saldo total de la cartera del sistema a la fecha de corte?",
            "¿Cuál fue la tasa de captación reportada por una entidad específica en un mes determinado?",
        ],
    ),
    Archetype.ARCHETYPE_B: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_B,
        name="evolucion_temporal",
        template="¿Cómo ha evolucionado {métrica} para {conjunto/dimensión} a lo largo de {período}?",
        description="Asks about trends, time series, or how something has changed over time",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.COMPARACION,
        default_chart=ChartType.LINE,
        examples=[
            "¿Cómo ha evolucionado el saldo total de la cartera durante el último año?",
            "¿Cómo han evolucionado las tasas de captación del mercado en los últimos 12 meses por tipo de entidad?",
        ],
    ),
    Archetype.ARCHETYPE_C: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_C,
        name="composicion_porcentaje",
        template="¿Qué porcentaje/porción de {conjunto} corresponde a {categoría/condición} en {período}?",
        description="Asks about portions of a total, percentages, or proportions",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.COMPARACION,
        default_chart=ChartType.PIE,
        examples=[
            "¿Qué porcentaje del saldo total de la cartera corresponde a cada tipo de producto?",
            "¿Qué porcentaje del volumen del mercado corresponde a cada tipo de entidad financiera?",
        ],
    ),
    Archetype.ARCHETYPE_D: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_D,
        name="participacion_share",
        template="¿Cuál es la participación de {actor} en {mercado/conjunto} durante {período}?",
        description="Asks about an entity's share within a larger set",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.COMPARACION,
        default_chart=ChartType.PIE,
        examples=[
            "¿Cuál es la participación de una entidad específica en el saldo total de la cartera del sistema?",
            "¿Cuál es la participación de una entidad en el mercado de captación en un período determinado?",
        ],
    ),
    Archetype.ARCHETYPE_E: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_E,
        name="contribucion_total",
        template="¿Cuál es la contribución de cada {categoría} al total de {métrica} en {período}?",
        description="Asks what each category contributes to an aggregate total",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.COMPARACION,
        default_chart=ChartType.STACKED_BAR,
        examples=[
            "¿Cómo contribuye cada producto al saldo total de la cartera?",
            "¿Cómo contribuye cada segmento de cartera al total de cartera vigente?",
        ],
    ),
    Archetype.ARCHETYPE_F: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_F,
        name="comparacion_directa",
        template="¿Cómo se compara {métrica} entre {grupo A} y {grupo B} en {período}?",
        description="Asks to compare metrics between two specific groups or categories",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.COMPARACION,
        default_chart=ChartType.BAR,
        examples=[
            "¿Cómo se compara el saldo de la cartera de consumo frente a la cartera de vivienda?",
            "¿Cómo se comparan las tasas de captación entre bancos y compañías de financiamiento?",
        ],
    ),
    Archetype.ARCHETYPE_G: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_G,
        name="ranking_los_mas",
        template="¿Cuáles {unidades} presentan el mayor/menor {métrica} dentro de {conjunto} en {período}?",
        description="Asks to rank or order entities by a metric",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.COMPARACION,
        default_chart=ChartType.BAR,
        examples=[
            "¿Cuáles son las entidades con mayor saldo de cartera en el sistema financiero?",
            "¿Cuáles son las entidades que reportan las tasas de captación más altas en un período determinado?",
        ],
    ),
    Archetype.ARCHETYPE_H: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_H,
        name="concentracion_top_n",
        template="¿Qué tan concentrado está {recurso/métrica} en {top-N/contrapartes} durante {período}?",
        description="Asks how concentrated a resource or metric is among top performers",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.COMPARACION,
        default_chart=ChartType.PIE,
        examples=[
            "¿Qué tan concentrado está el saldo de la cartera en las cinco principales entidades del sistema?",
            "¿Qué tan concentrado está el mercado de captación en las principales entidades financieras?",
        ],
    ),

    # ==========================================================================
    # Relationship (I–J)
    # ==========================================================================
    Archetype.ARCHETYPE_I: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_I,
        name="relacion_entre_variables",
        template="¿Cómo se relaciona {métrica A} con {métrica B} para {conjunto} en {período}?",
        description="Asks about the relationship or correlation between two metrics",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.RELACION,
        default_chart=None,
        examples=[
            "¿Cómo se relacionan las tasas de captación con el volumen captado por las entidades?",
            "¿Cómo se relaciona el saldo total de la cartera con el nivel de cartera vigente por producto?",
        ],
    ),
    Archetype.ARCHETYPE_J: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_J,
        name="sensibilidad_derivada",
        template="¿Cuál es la sensibilidad de {resultado} ante cambios en {variable} durante {período}?",
        description="Asks about sensitivity or how much one metric changes when another changes",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.RELACION,
        default_chart=None,
        examples=[
            "¿Qué tan sensible es el volumen captado ante cambios en las tasas de interés del mercado?",
            "¿Qué tan sensible es la cartera vigente ante variaciones en el saldo total de la cartera?",
        ],
    ),

    # ==========================================================================
    # Projection (K–L)
    # ==========================================================================
    Archetype.ARCHETYPE_K: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_K,
        name="descomposicion_cambio",
        template="¿Qué porción del cambio en {métrica} se explica por {driver A} vs {driver B} en {período}?",
        description="Asks what portion of a change is explained by different drivers",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.PROYECCION,
        default_chart=None,
        examples=[
            "¿Qué parte del crecimiento de la cartera se explica por consumo frente a cartera comercial?",
            "¿Qué parte del cambio en el volumen del mercado se explica por bancos frente a otras entidades?",
        ],
    ),
    Archetype.ARCHETYPE_L: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_L,
        name="escenario_what_if",
        template="Si {supuesto/condición}, ¿cuál sería el impacto en {resultado} durante {horizonte}?",
        description="Asks about the impact of a hypothetical assumption on an outcome",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.PROYECCION,
        default_chart=None,
        examples=[
            "Si las tasas de captación aumentan, ¿cuál sería el impacto esperado en el volumen captado en los próximos meses?",
            "Si la cartera vigente crece por encima del promedio histórico, ¿cuál sería el impacto en el saldo total de la cartera?",
        ],
    ),

    # ==========================================================================
    # Simulation (M–N)
    # ==========================================================================
    Archetype.ARCHETYPE_M: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_M,
        name="capacidad_dado_un_recurso",
        template="Dado {restricción/recurso}, ¿hasta qué nivel puede llegar {resultado} durante {horizonte}?",
        description="Asks for the maximum achievable level given a constraint",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.SIMULACION,
        default_chart=None,
        examples=[
            "Dado un límite de crecimiento de cartera, ¿hasta qué nivel podría llegar el saldo total en el próximo año?",
            "Dado un tope de fondeo disponible, ¿hasta qué nivel podría crecer el volumen captado manteniendo las tasas actuales?",
        ],
    ),
    Archetype.ARCHETYPE_N: ArchetypeInfo(
        archetype=Archetype.ARCHETYPE_N,
        name="requerimiento_inverso",
        template="¿Qué nivel de {variable/palanca} se requiere para alcanzar {objetivo} en {horizonte}?",
        description="Asks what level of a variable is needed to reach a specific objective",
        intent=Intent.REQUIERE_VIZ,
        pattern_type=PatternType.SIMULACION,
        default_chart=None,
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
    """Get all archetypes for prompt."""
    return "\n\n".join(info.get_all_info_as_string() for info in ARCHETYPES.values())


def get_archetypes_by_pattern_type(pattern_type: PatternType) -> list[ArchetypeInfo]:
    """Get all archetypes for a pattern type."""
    return [a for a in ARCHETYPES.values() if a.pattern_type == pattern_type]


def get_archetypes_by_intent(intent: Intent) -> list[ArchetypeInfo]:
    """Get all archetypes for an intent."""
    return [a for a in ARCHETYPES.values() if a.intent == intent]


def get_archetype_name(archetype_letter: str) -> str:
    """Get archetype name by archetype letter.
    Args:
        archetype_letter: str (e.g., "N")
    Returns:
        str: Archetype name. Falls back to "nivel_puntual" (archetype A) for invalid input.
    """
    try:
        archetype_enum = Archetype(archetype_letter)
        return ARCHETYPES[archetype_enum].name
    except ValueError:
        # Invalid archetype letter - fall back to archetype A (nivel_puntual)
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Invalid archetype letter '{archetype_letter}', defaulting to 'A' (nivel_puntual)")
        return ARCHETYPES[Archetype.ARCHETYPE_A].name

def get_chart_type_for_archetype(archetype: Archetype) -> ChartType | None:
    """Get default chart type for an archetype.
    Args:
        archetype: Archetype
    Returns:
        ChartType | None: Default chart type
    """
    return ARCHETYPES[archetype].default_chart
def get_archetype_letter_by_name(name: str) -> Archetype | None:
    """Get archetype letter by archetype name.
    Args:
        name: str
    Returns:
        Archetype | None: Archetype enum or None if not found
    """
    for archetype, info in ARCHETYPES.items():
        if info.name == name:
            return archetype
    return None