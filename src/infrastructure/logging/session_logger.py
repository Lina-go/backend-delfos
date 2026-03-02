"""Session-based agent response logger in Markdown format."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any


class SessionLogger:
    """Saves per-agent responses to timestamped Markdown files."""

    def __init__(self, base_dir: str | None = None) -> None:
        if base_dir:
            self.base_dir = Path(base_dir)
        else:
            # Calculate path to project root: src/infrastructure/logging/session_logger.py -> project root
            self.base_dir = Path(__file__).parent.parent.parent.parent / "logs"

        self.session_dir: Path | None = None
        self.agent_counter: int = 0
        self.session_timestamp: str | None = None

    def start_session(self, user_id: str = "anonymous", user_message: str = "") -> str:
        """Create a timestamped session directory and return its path."""
        self.session_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.session_dir = self.base_dir / self.session_timestamp
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.agent_counter = 0

        started_at = datetime.now().isoformat()

        metadata_content = f"""# Sesión: {self.session_timestamp}

## Información de la Sesión

- **Usuario:** {user_id}
- **Inicio:** {started_at}

## Mensaje Original

```
{user_message}
```

---

## Agentes Ejecutados

Los archivos de respuesta de cada agente están en este directorio.
"""

        session_file = self.session_dir / "00_session_info.md"
        session_file.write_text(metadata_content, encoding="utf-8")

        return str(self.session_dir)

    @staticmethod
    def _md_section(title: str, content: str, lang: str = "") -> list[str]:
        """Build a Markdown section with a fenced code block."""
        return [f"## {title}", "", f"```{lang}", content, "```", ""]

    def log_agent_response(
        self,
        agent_name: str,
        raw_response: str,
        parsed_response: Any | None = None,
        input_text: str | None = None,
        system_prompt: str | None = None,
        execution_time_ms: float | None = None,
    ) -> str:
        """Write an agent response to a numbered Markdown file and return its path."""
        if self.session_dir is None:
            raise RuntimeError("Session not started. Call start_session() first.")

        self.agent_counter += 1
        file_number = f"{self.agent_counter:02d}"
        filename = f"{file_number}_{agent_name}.md"
        filepath = self.session_dir / filename

        content_parts = [
            f"# {agent_name}",
            "",
            f"**Ejecutado:** {datetime.now().isoformat()}",
        ]

        if execution_time_ms is not None:
            content_parts.append(f"**Tiempo de ejecución:** {execution_time_ms:.2f} ms")

        content_parts.extend(["", "---", ""])

        if system_prompt:
            content_parts.extend(self._md_section("System Prompt", system_prompt))

        if input_text:
            content_parts.extend(self._md_section("Input", input_text))

        content_parts.extend(self._md_section("Respuesta Raw", raw_response))

        if parsed_response and not raw_response:
            content_parts.extend(self._md_section(
                "Respuesta Parseada (JSON)",
                json.dumps(parsed_response, indent=2, ensure_ascii=False),
                lang="json",
            ))

        content = "\n".join(content_parts)
        filepath.write_text(content, encoding="utf-8")

        return str(filepath)

    def end_session(
        self,
        success: bool,
        final_message: str = "",
        errors: list[str] | None = None,
    ) -> None:
        """Append a summary section to the session info file."""
        if not self.session_dir:
            return

        session_file = self.session_dir / "00_session_info.md"
        status = "Exitoso" if success else "Con errores"

        summary = f"""

---

## Resumen de Ejecución

- **Estado:** {status}
- **Agentes ejecutados:** {self.agent_counter}
- **Finalizado:** {datetime.now().isoformat()}

### Mensaje Final

```
{final_message}
```
"""

        if errors:
            summary += "\n### Errores\n\n"
            for error in errors:
                summary += f"- {error}\n"

        with open(session_file, "a", encoding="utf-8") as f:
            f.write(summary)

        self.session_dir = None
        self.agent_counter = 0
        self.session_timestamp = None
