"""
Archetype definitions (A-N) for intent classification.
"""

from dataclasses import dataclass, asdict
from src.config.constants import Archetype, PatternType, Intent, ChartType


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

    def to_dict(self) -> dict:
        """Get all information for the archetype as a dictionary."""
        return asdict(self)
    
    def get_all_info_as_string(self) -> str:
        """Get all information for the archetype as a string."""
        return f"Archetype: {self.archetype}\nName: {self.name}\nTemplate: {self.template}\nDescription: {self.description}\nExamples: {self.examples}"


ARCHETYPES: dict[Archetype, ArchetypeInfo] = {
    # ==========================================================================
    # Comparación (A-H)
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
            "¿Cuál es el saldo total de las cuentas en el mes actual?",
            "¿Cuál es el monto total de préstamos originados en el período seleccionado?",
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
            "¿Cómo ha evolucionado el saldo total de cuentas mes a mes durante el último año?",
            "¿Cómo ha evolucionado el monto de préstamos originados por mes?",
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
            "¿Qué porcentaje del saldo total corresponde a cada tipo de cuenta en el mes actual?",
            "¿Qué proporción del monto transado corresponde a cada tipo de transacción?",
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
            "¿Cuál es la participación de cada sucursal en el monto total transado del mes?",
            "¿Cuál es la participación de cada tipo de préstamo en el saldo total de préstamos activos?",
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
            "¿Cómo contribuye cada tipo de transacción al monto total mensual transado?",
            "¿Cómo contribuye cada sucursal al total de préstamos originados este trimestre?",
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
            "¿Cómo se compara el saldo promedio entre cuentas de ahorro y cuentas corrientes durante el año?",
            "¿Cómo se compara el monto promedio transado entre clientes naturales y jurídicos?",
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
            "¿Cómo se ordenan las sucursales según el monto total transado en el período?",
            "¿Cómo se ordenan los tipos de préstamo según su monto originado anual?",
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
            "¿Qué tan concentrado está el monto total transado en las 3 principales sucursales?",
            "¿Qué tan concentrada está la originación de préstamos en los primeros 3 tipos de préstamo?",
        ],
    ),
    # ==========================================================================
    # Relación (I-J)
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
            "¿Cómo se relaciona el saldo de cuentas con el monto de préstamos originados por cliente durante el año?",
            "¿Cómo se relaciona el monto transado con el número de transacciones por mes?",
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
            "¿Cuál es la sensibilidad del monto total de préstamos ante cambios en la tasa de interés durante el año?",
            "¿Cuál es la sensibilidad del saldo total de cuentas ante cambios en el número de clientes durante el trimestre?",
        ],
    ),
    # ==========================================================================
    # Proyección (K-L)
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
            "¿Qué porción del cambio en el monto total transado se explica por nuevas cuentas vs transacciones existentes en el último trimestre?",
            "¿Qué porción del cambio en el saldo de préstamos se explica por originaciones nuevas vs pagos realizados durante el año?",
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
            "Si la tasa de interés aumenta un 2%, ¿cuál sería el impacto en el monto total de préstamos originados durante el próximo trimestre?",
            "Si se duplica el número de clientes nuevos, ¿cuál sería el impacto en el saldo total de cuentas durante el próximo año?",
        ],
    ),
    # ==========================================================================
    # Simulación (M-N)
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
            "Dado un límite de capital disponible de 10 millones, ¿hasta qué nivel puede llegar el monto total de préstamos originados durante el próximo año?",
            "Dado un número máximo de 1000 transacciones diarias, ¿hasta qué nivel puede llegar el monto total transado durante el próximo mes?",
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
            "¿Qué nivel de tasa de interés se requiere para alcanzar un monto total de préstamos originados de 50 millones en el próximo año?",
            "¿Qué número de clientes nuevos se requiere para alcanzar un saldo total de cuentas de 100 millones en el próximo trimestre?",
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
