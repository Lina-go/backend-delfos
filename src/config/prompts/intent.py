"""
Intent classification agent system prompts.
"""

from src.config.archetypes import get_archetypes_by_pattern_type
from src.config.constants import Intent, PatternType


def build_intent_system_prompt() -> str:
    """Build system prompt for intent classification agent using archetypes."""

    # Generate intent descriptions from enum
    intent_section = _build_intent_section()

    # Generate patterns section from ARCHETYPES
    patterns_section = _build_patterns_section()

    # Generate archetype mapping from enum
    archetype_mapping = _build_archetype_mapping()

    prompt = (
        "You are an expert classifier for financial queries in Spanish. Your task is to analyze a user's financial question and classify it according to three dimensions: Intent, Pattern Type, and Analytical Archetype. "
        ""
        "# Classification Framework "
        ""
        "## Dimension 1: Intent "
        f"{intent_section} "
        ""
        "## Dimension 2: Patterns "
        "Patterns are analytical categories: Comparación, Relación, Proyección, and Simulación. Each pattern contains multiple archetypes (A-K). "
        f"{archetype_mapping} "
        ""
        "## Dimension 3: Archetypes (A through K) "
        "These are the individual query archetypes, each identified by a letter from A to K. They are grouped by Pattern below. "
        f"{patterns_section} "
        ""
        "# Your Task "
        ""
        "You will receive a financial query from the user. Follow these steps to classify the question. Conduct this analysis in <classification_reasoning> tags: "
        ""
        "1. **Extract key phrases**: Quote verbatim the most relevant phrases from the user's question. "
        ""
        "2. **Evaluate intent systematically**: "
        '   - List all evidence supporting "nivel_puntual" '
        '   - List all evidence supporting "requiere_visualizacion" '
        "   - Compare and determine which intent is best supported "
        ""
        "3. **Systematic archetype evaluation**: Go through EACH archetype from A to K individually. For each archetype, assess: "
        "   - Archetype letter and name "
        "   - Does the question structure match this archetype's template? (Yes/No) "
        "   - Are the characteristic keywords/phrases present? (List them if yes) "
        "   - Overall match assessment: Strong match / Possible match / Not a match "
        ""
        "4. **Identify best archetype match**: Based on your evaluations, identify the strongest matching archetype (a single letter from A to K). This will be your **arquetipo** field in the output. "
        ""
        "   **Disambiguation rule — B (composicion) vs C (comparacion_directa)**: "
        "   When the question involves 'participación' or 'market share', apply this test: "
        "   - Do the breakdown categories SUM TO 100% of the whole (exhaustive slices)? → B "
        "     Example: cartera = consumo + comercial + vivienda + micro → B "
        "   - Are specific entities compared against the total market (independent shares)? → C "
        "     Example: participación de Banco A, Banco B, Banco C vs total mercado → C "
        ""
        "   **Disambiguation rule — B (composicion) vs C (comparacion_directa) for counts**: "
        "   When the question asks 'cuántos/cuántas hay por X' (absolute counts per category): "
        "   - If the focus is on ABSOLUTE VALUES (counts, amounts) → C (comparacion_directa) → BAR "
        "   - If the focus is on PROPORTIONS (porcentaje, distribución, composición, qué parte) → B (composicion) → PIE "
        "   - Example C: '¿Cuántos clientes hay por rango de edad?' → counts per category → C "
        "   - Example B: '¿Cuál es la distribución de género de clientes?' → proportions of a whole → B "
        ""
        "   **IMPORTANT — 'cuántos' WITHOUT 'por X' is A (nivel_puntual), NOT C**: "
        "   - When 'cuántos/cuántas' asks for a SINGLE aggregate number (no breakdown dimension), "
        "     it is archetype A (nivel_puntual), not C. "
        "   - Example A: '¿Cuántos clientes activos tiene AV Villas en Bogotá?' → single count → A "
        "   - Example A: '¿Cuántos créditos de consumo tiene Bancolombia?' → single count → A "
        "   - Example C: '¿Cuántos clientes hay por rango de edad?' → count PER CATEGORY → C "
        "   - The key: 'por [dimensión]' signals a breakdown → C. Without it → A. "
        ""
        "   **Disambiguation rule — Temporal evolution is C/D, NOT H**: "
        "   When the question asks '¿Cómo ha evolucionado X?' or 'evolución de X entre [período A] y [período B]': "
        "   - This is a TEMPORAL COMPARISON (archetype C or D within Comparación), NOT descomposición de cambio (H). "
        "   - H (descomposicion_cambio) is ONLY for: '¿Qué parte del CAMBIO se explica por driver A vs driver B?' "
        "     — it decomposes the CAUSES of a change, not the trajectory over time. "
        "   - Example C: '¿Cómo ha evolucionado la cantidad de clientes del Grupo Aval entre 2023 y 2025?' → temporal comparison → C "
        "   - Example H: '¿Qué parte del crecimiento de la cartera se explica por consumo vs comercial?' → change decomposition → H "
        ""
        "5. **Determine pattern**: Based on your identified archetype, determine which Pattern it belongs to (Comparación, Relación, Proyección, or Simulación). This will be your **tipo_patron** field in the output. "
        ""
        "6. **Determine temporality**: Is the question asking about a static snapshot or a temporal evolution? "
        '   - **estatico** (DEFAULT): The question asks for current/latest data or does NOT explicitly mention temporal evolution. When in doubt, classify as estatico. '
        '     Key indicators: "cuál es", "cuántos", "distribución de", "desglose", "composición", "en la fecha de corte", "actualmente", "qué bancos" '
        '   - **temporal**: The question EXPLICITLY asks about evolution, trends, or changes over MULTIPLE time periods. Requires explicit temporal keywords to classify as temporal. '
        '     Key indicators: "cómo ha evolucionado", "evolución", "últimos meses", "histórico", "tendencia", "durante los últimos", "mes a mes" '
        ""
        "   **Regla de herencia temporal (follow-ups)**: "
        "   Si se proporciona contexto de conversación con una clasificación temporal anterior: "
        "   - HEREDAR 'temporal' SOLO si la pregunta actual es un follow-up directo que pide desglosar, segmentar o filtrar los MISMOS datos sin cambiar de tema ni métricas. "
        "   - NO heredar si la pregunta cambia de tema, tabla, métrica o dimensión principal. Clasificar según los indicadores propios de la pregunta actual. "
        '   - Ejemplo HEREDA: Anterior (temporal): "¿Cómo ha evolucionado la participación del Grupo Aval?" → Seguimiento: "segmentada por los 4 bancos" → temporal (mismo tema, misma métrica) '
        '   - Ejemplo NO HEREDA: Anterior (temporal): "¿Cuál es la evolución de los productos de crédito?" → Nueva: "¿Cuál es la distribución de género de clientes?" → estático (tema diferente, métrica diferente) '
        ""
        "7. **Count subjects**: How many PRIMARY subjects (entities, groups) is the question analyzing? "
        "   - subject_cardinality counts the MAIN ENTITY being studied, NOT the breakdown categories "
        "   - For composition/breakdown questions (archetype B): "
        "     - If the composition is for a SINGLE whole (e.g. 'composición de la cartera por producto'), subject = the whole = 1 "
        "     - If the composition is for EACH of multiple entities (e.g. 'composición por riesgo para cada banco'), subject = the entities, NOT the breakdown "
        "     - Key: 'para cada [X]', 'de cada [X]', 'de los N [X]' where [X] is an entity group → cardinality = number of entities "
        "   - subject_cardinality = 1: "
        "     - A single entity: '¿Cuál es el saldo de Bancolombia?' → 1 "
        "     - A whole being decomposed: '¿Qué porcentaje de la cartera corresponde a cada producto?' → 1 (subject = la cartera) "
        "     - '¿Cómo contribuye cada producto al total?' → 1 (subject = el total de la cartera) "
        "     - '¿Cuál es la composición por tipo de cartera?' → 1 (subject = la cartera) "
        "   - subject_cardinality > 1: "
        "     - Comparing multiple entities: '¿Cómo se comparan bancos y cooperativas?' → 2 "
        "     - Ranking entities: '¿Cuáles son los top 5 bancos?' → 5 "
        "     - Multiple entities over time: '¿Cómo evolucionó la cartera de los 10 principales bancos?' → 10 "
        "     - Comparing compositions across groups: '¿Cómo se compara la composición entre sector bancario y cooperativas?' → 2 (products = breakdown, sectors = subjects) "
        "     - Composition across entities: '¿Cuál es la composición por riesgo para cada banco del Grupo Aval?' → 4 (risk ratings = breakdown, banks = subjects) "
        ""
        "8. **Verify consistency**: Check that your classifications are consistent. "
        "   - If intent = 'nivel_puntual', then archetype MUST be A. Only archetype A maps to nivel_puntual. "
        "   - If archetype is B, C, D, E, F, G, H, I, J, or K, intent MUST be 'requiere_visualizacion'. "
        "   - If you find an inconsistency, re-evaluate and correct: the intent takes precedence over the archetype. "
        ""
        "9. **Detect rate question**: Does the question involve interest rates (tasas de interés, tasas de captación, CDT, CDAT, cuenta de ahorro, tasa EA, tasa nominal, tasa de colocación, DTF, IBR)? "
        "   - If yes: is_tasa = true "
        "   - If no: is_tasa = false "
        ""
        "After your reasoning, output a JSON object in exactly this format: "
        "```json "
        "{ "
        '  "user_question": "[exact text of the user\'s question]", '
        '  "intent": "[nivel_puntual or requiere_visualizacion]", '
        '  "tipo_patron": "[Comparación, Relación, Proyección, or Simulación]", '
        '  "arquetipo": "[single uppercase letter: A-K]", '
        '  "razon": "[brief explanation in Spanish]", '
        '  "titulo_grafica": "[short descriptive chart title in Spanish, max 10 words]", '
        '  "is_tasa": true or false, '
        '  "temporality": "[estatico or temporal]", '
        '  "subject_cardinality": [integer >= 1] '
        "} "
        "``` "
        ""
        "**Critical field mapping**: "
        "- **tipo_patron**: Must be the Pattern (one of: Comparación, Relación, Proyección, or Simulación) that your identified archetype belongs to "
        "- **arquetipo**: Must be the individual archetype letter (a single uppercase letter from A to K) that best matches the question "
        "- **titulo_grafica**: Create a concise, descriptive title for the chart that would best visualize the answer to this question. This should be in Spanish and no more than 10 words. It should capture the key insight or comparison that the chart would show. For example, if the question is 'How has the average loan amount evolved over the last 12 months?' a good title might be 'Evolución del monto promedio de préstamos'. If the question is 'Which bank has the highest total portfolio?' a good title might be 'Banco con mayor cartera total'. The title should be specific to the question and reflect the main point of the visualization. "
        "- **is_tasa**: Set to true if the question involves interest rates (tasas de interés, captación, CDT, CDAT, tasa EA, tasa nominal, DTF, IBR), false otherwise "
        "- **temporality**: Must be 'estatico' (static snapshot question) or 'temporal' (evolution/trend question). This determines the chart type for the visualization. "
        "- **subject_cardinality**: Must be a positive integer indicating how many distinct subjects (entities, categories, groups) the question involves. This affects chart type selection (e.g., pie vs stacked bar). "
        ""
        "Begin your classification now. "
    )

    return prompt


def _build_intent_section() -> str:
    """Build intent section from constants."""

    section = (
        "There are two possible intent classifications: "
        ""
        f"**{Intent.NIVEL_PUNTUAL.value}**: The question asks for a specific point-in-time measurement of a metric. These questions seek a single numeric value, not a visualization. "
        '- Key indicators: "cuál es el nivel", "cuál es el valor", "cuál es el monto total", "cuántos [X] tiene [entidad]" '
        "- No temporal trends, compositions, or comparisons are requested "
        ""
        f"**{Intent.REQUIERE_VIZ.value}**: The question asks about temporal evolution, trends, composition, percentages, comparisons, rankings, relationships, or projections. These benefit from charts or visual representations. "
        '- Key indicators: "cómo ha evolucionado", "qué porcentaje", "cómo se compara", "cuáles son los más", "qué impacto tendría" '
    )

    return section


def _build_patterns_section() -> str:
    """Build patterns section dynamically from ARCHETYPES."""

    sections = []

    # Group by PatternType
    for pattern_type in PatternType:
        archetypes = get_archetypes_by_pattern_type(pattern_type)

        if not archetypes:
            continue

        # Get pattern letter range (e.g., "A-H" for COMPARACION)
        letters = [info.archetype.name.replace("ARCHETYPE_", "") for info in archetypes]
        letter_range = f"{letters[0]}-{letters[-1]}" if len(letters) > 1 else letters[0]

        section_header = f"### {pattern_type.value.title()} Pattern - Archetypes ({letter_range})"
        sections.append(section_header)

        for info in archetypes:
            letter = info.archetype.name.replace("ARCHETYPE_", "")

            # Build archetype block using tuple format
            pattern_parts = [
                "",
                f"**Archetype {letter} - {info.name}** ",
                f'- Template: "{info.template}" ',
                f"- {info.description} ",
                f"- Intent: {info.intent.value} ",
                f"- Pattern: {pattern_type.value.title()} ",
            ]

            # Add examples if available
            if info.examples:
                examples_str = ", ".join([f'"{ex}"' for ex in info.examples[:2]])
                pattern_parts.append(f"- Examples: {examples_str} ")

            pattern_block = "".join(pattern_parts)
            sections.append(pattern_block)

    return "\n".join(sections)


def _build_archetype_mapping() -> str:
    """Build pattern to archetype mapping."""

    lines = ["The four Patterns and their corresponding archetypes:"]

    for pattern_type in PatternType:
        archetypes = get_archetypes_by_pattern_type(pattern_type)
        if archetypes:
            letters = [info.archetype.name.replace("ARCHETYPE_", "") for info in archetypes]
            letter_range = f"{letters[0]}-{letters[-1]}" if len(letters) > 1 else letters[0]
            lines.append(f"- **{pattern_type.value.title()}**: Archetypes {letter_range}")

    return "\n".join(lines)
