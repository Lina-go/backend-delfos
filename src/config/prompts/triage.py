"""
Triage agent system prompts.
"""

from src.config.constants import QueryType
from src.config.database import get_all_table_names


def build_triage_system_prompt(
    has_context: bool = False,
    context_summary: str | None = None,
    conversation_history: str | None = None,
) -> str:
    """Build system prompt for triage agent.

    Args:
        has_context: Whether the user has previous conversation data available.
        context_summary: Summary of data available in context (from ConversationContext.get_summary())
        conversation_history: Formatted conversation history from ConversationContext.get_history_summary()

    Returns:
        System prompt string for the triage classifier.
    """
    valid_query_types = ", ".join([f'"{qt.value}"' for qt in QueryType])
    tables_list = ", ".join(get_all_table_names())

    # Build context data section if available
    context_data_section = ""
    if has_context and context_summary:
        context_data_section = f"""
## DATOS YA DISPONIBLES EN CONTEXTO
El usuario tiene datos de una consulta anterior:
```
{context_summary}
```

**REGLA IMPORTANTE**: Si la pregunta del usuario puede responderse con estos datos
(menciona entidades, valores o columnas que aparecen arriba), clasifica como **{QueryType.FOLLOW_UP.value}**.
Esto aplica incluso si la pregunta esta en otro idioma (ingles, etc.).

"""

    # Build conversation history section
    history_section = ""
    if conversation_history:
        history_section = f"""
## HISTORIAL DE CONVERSACION
{conversation_history}

**REGLA**: Usa el historial para entender el contexto de la conversacion actual.
Si el usuario responde a una pregunta de clarificacion previa, interpreta su respuesta
en el contexto completo de la conversacion.

"""

    # Build context-aware categories
    context_categories = ""
    if has_context:
        context_categories = f"""
4. **{QueryType.FOLLOW_UP.value}**: Preguntas sobre los datos anteriores (SOLO cuando hay contexto previo).
   - Pregunta por valores especificos de los datos ya disponibles.
   - Pide filtrar, comparar o explicar datos anteriores.
   - Pregunta "por que?" o pide explicacion sobre resultados previos.
   - Ejemplos: "Cual fue el de Davivienda?", "Y en octubre?", "Por que?", "El mayor", "What about Bancolombia?"
   - **IMPORTANTE**: Solo clasifica como follow_up si la respuesta puede derivarse DIRECTAMENTE
     de las columnas existentes en el contexto. Si la pregunta introduce una metrica o dimension
     NUEVA que NO aparece en las columnas del contexto, clasifica como data_question.
     Ejemplo: contexto tiene [label, tasa, desembolsos] y pregunta pide "cartera" → data_question (cartera NO esta en contexto).

5. **{QueryType.VIZ_REQUEST.value}**: Solicita visualizar o cambiar tipo de grafica (SOLO cuando hay datos previos).
   - Pide graficar datos existentes o cambiar visualizacion.
   - Ejemplos: "Graficalo", "En barras", "Show me a chart", "Mejor en pie".
"""

    # Build context-aware rules
    context_rules = ""
    if has_context:
        context_rules = f"""
- Si pregunta por valores que EXISTEN en el contexto previo -> **{QueryType.FOLLOW_UP.value}**
- Si pregunta "por que?" sobre datos anteriores -> **{QueryType.FOLLOW_UP.value}**
- Si pide graficar o cambiar visualizacion -> **{QueryType.VIZ_REQUEST.value}**
"""
    else:
        context_rules = f"""
- Si pregunta "por que?" pero NO hay contexto -> **{QueryType.GENERAL.value}**
- Si pide graficar pero NO hay datos -> **{QueryType.GENERAL.value}**
"""

    prompt = f"""Clasifica la pregunta del usuario en una de las categorias disponibles: {valid_query_types}.

## Contexto de Conversacion
Existe conversacion previa con datos: **{"Si" if has_context else "NO"}**
{context_data_section}{history_section}## Categorias

1. **{QueryType.GREETING.value}**: Saludos, despedidas o agradecimientos.
   - Conversacion social.
   - Ejemplos: "Hola", "Buenos dias", "Gracias", "Chao", "Hi", "Thanks".

2. **{QueryType.DATA_QUESTION.value}**: Pide informacion, metricas o comparaciones que requieren NUEVA consulta a la base de datos.
   - Involucra metricas financieras (saldos, tasas, carteras, mora).
   - Compara entidades, periodos, productos.
   - Pregunta por relacion, correlacion o asociacion entre dos metricas financieras.
   - Requiere datos que NO estan en el contexto previo.
   - Tablas disponibles: {tables_list}.
   - Ejemplos: "Cual es la tasa de mora?", "Compara bancos por saldo", "Show me rates for 2024", "Como se relaciona la tasa con los desembolsos?".

3. **{QueryType.GENERAL.value}**: Preguntas sobre conceptos financieros, capacidades del sistema, o conversacion general.
   - Definiciones o explicaciones de terminos financieros.
   - Preguntas sobre que puede hacer Delfos, que datos tiene, como funciona.
   - Conversacion casual que no es saludo ni requiere datos especificos.
   - Ejemplos: "Que es un CDT?", "Que datos tienes?", "Que puedo consultar?", "Como funciona?", "What can you do?".
{context_categories}
6. **{QueryType.OUT_OF_SCOPE.value}**: Preguntas no relacionadas con finanzas o analisis de datos.
   - Temas fuera del dominio de Delfos.
   - Ejemplos: "Que hora es?", "What's the weather?", "Quien gano el partido?".

7. **{QueryType.NEEDS_CLARIFICATION.value}**: La pregunta es demasiado vaga o ambigua para procesarla.
   - No se puede determinar que datos busca el usuario.
   - Falta informacion critica: metrica, entidad, o tipo de dato.
   - El usuario dice algo muy general sin especificar que quiere.
   - Ejemplos: "dame los datos", "compara", "muestra informacion", "informacion de bancos" (sin metrica).
   - **IMPORTANTE**: Solo clasifica como needs_clarification si la pregunta es REALMENTE ambigua.
     Si el usuario pide algo razonable aunque impreciso (ej: "cartera de Bancolombia"), clasifícalo como data_question.
     Si falta solo el rango temporal, NO es ambiguo (el sistema aplica defaults).

## Reglas de Clasificacion

1. Primero verifica si es saludo/despedida/agradecimiento -> **{QueryType.GREETING.value}**
2. **CRITICO**: Si hay contexto previo ({"Si" if has_context else "NO"}) Y la pregunta se refiere a datos que YA EXISTEN en ese contexto, clasifica como **{QueryType.FOLLOW_UP.value}**
{context_rules}
3. Si requiere datos NUEVOS de la base de datos -> **{QueryType.DATA_QUESTION.value}**
4. Si es sobre conceptos, capacidades del sistema o conversacion general -> **{QueryType.GENERAL.value}**
5. Si no es sobre finanzas -> **{QueryType.OUT_OF_SCOPE.value}**
6. ULTIMO RECURSO: Si la pregunta es demasiado vaga para saber QUE consultar -> **{QueryType.NEEDS_CLARIFICATION.value}**

## Formato de Respuesta

<analysis>
[Tu razonamiento aqui - maximo 3 oraciones en espanol]
</analysis>
<classification>
{{
  "query_type": "{QueryType.GREETING.value}" | "{QueryType.DATA_QUESTION.value}" | "{QueryType.FOLLOW_UP.value}" | "{QueryType.VIZ_REQUEST.value}" | "{QueryType.GENERAL.value}" | "{QueryType.OUT_OF_SCOPE.value}" | "{QueryType.NEEDS_CLARIFICATION.value}",
  "reasoning": "Explicacion breve en espanol"
}}
</classification>

## Ejemplos

User: "Hola"
<analysis>
Saludo simple, no requiere datos.
</analysis>
<classification>
{{
  "query_type": "{QueryType.GREETING.value}",
  "reasoning": "Saludo social."
}}
</classification>

User: "Cual es la cartera total de los bancos en 2024?"
<analysis>
Pregunta por datos especificos que requieren consulta a la base de datos.
</analysis>
<classification>
{{
  "query_type": "{QueryType.DATA_QUESTION.value}",
  "reasoning": "Requiere consultar datos de cartera bancaria."
}}
</classification>

User: "que datos tienes?"
<analysis>
El usuario pregunta sobre las capacidades del sistema, no solicita datos especificos de la base de datos.
</analysis>
<classification>
{{
  "query_type": "{QueryType.GENERAL.value}",
  "reasoning": "Pregunta sobre capacidades del sistema, no requiere consulta a base de datos."
}}
</classification>
"""

    # Add follow-up example only when context exists
    if has_context:
        prompt += f"""
User: "Y el de Davivienda?" (cuando el contexto tiene datos de varios bancos incluyendo Davivienda)
<analysis>
El usuario pregunta por un valor especifico (Davivienda) que ya existe en los datos del contexto. No necesita nueva consulta.
</analysis>
<classification>
{{
  "query_type": "{QueryType.FOLLOW_UP.value}",
  "reasoning": "Pregunta por datos que ya estan disponibles en el contexto anterior."
}}
</classification>

User: "What was the balance in July?" (cuando el contexto tiene datos con Mes: 7, 8, 9, 10)
<analysis>
El usuario pregunta en ingles por "July" (mes 7) que existe en el contexto. El idioma no importa, los datos estan disponibles.
</analysis>
<classification>
{{
  "query_type": "{QueryType.FOLLOW_UP.value}",
  "reasoning": "July = mes 7, que existe en el contexto previo."
}}
</classification>
"""

    # Add clarification example
    prompt += f"""
User: "dame los datos"
<analysis>
La pregunta es demasiado vaga. No especifica que metrica, entidad o tipo de dato busca.
</analysis>
<classification>
{{
  "query_type": "{QueryType.NEEDS_CLARIFICATION.value}",
  "reasoning": "Pregunta ambigua sin metrica, entidad o tipo de dato especifico."
}}
</classification>
"""

    return prompt
