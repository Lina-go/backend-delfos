"""Session-based markdown logger."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


class SessionLogger:
    """
    Logger that saves each agent's responses to markdown files.
    """

    def __init__(self, base_dir: Optional[str] = None) -> None:
        """
        Initialize the logger.

        Args:
            base_dir: Base directory for logs. Defaults to 'logs' in the root.
        """
        if base_dir:
            self.base_dir = Path(base_dir)
        else:
            # Calculate path to project root: src/infrastructure/logging/session_logger.py -> project root
            self.base_dir = Path(__file__).parent.parent.parent.parent / "logs"

        self.session_dir: Optional[Path] = None
        self.agent_counter: int = 0
        self.session_timestamp: Optional[str] = None

    def start_session(self, user_id: str = "anonymous", user_message: str = "") -> str:
        """
        Start a new session by creating a timestamped directory.

        Args:
            user_id: User ID.
            user_message: Original user message.

        Returns:
            Path of the session directory.
        """
        self.session_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.session_dir = self.base_dir / self.session_timestamp
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.agent_counter = 0

        started_at = datetime.now().isoformat()

        metadata_content = f"""# Sesión: {self.session_timestamp}

- **User ID**: {user_id}
- **Started at**: {started_at}
- **User Message**: {user_message}

---

"""

        metadata_file = self.session_dir / "00_Metadata.md"
        metadata_file.write_text(metadata_content, encoding="utf-8")

        return str(self.session_dir)

    def log_agent_response(
        self,
        agent_name: str,
        raw_response: str,
        parsed_response: Optional[Any] = None,
        input_text: Optional[str] = None,
        execution_time_ms: Optional[float] = None,
    ) -> None:
        """
        Log an agent's response to a markdown file.

        Args:
            agent_name: Name of the agent.
            raw_response: Raw response from the agent.
            parsed_response: Parsed response (optional).
            input_text: Input text sent to the agent (optional).
            execution_time_ms: Execution time in milliseconds (optional).
        """
        if self.session_dir is None:
            raise RuntimeError("Session not started. Call start_session() first.")

        self.agent_counter += 1
        file_number = f"{self.agent_counter:02d}"
        filename = f"{file_number}_{agent_name}.md"
        filepath = self.session_dir / filename

        content = f"# {agent_name}\n\n"
        content += f"**Execution Time**: {execution_time_ms:.2f} ms\n\n" if execution_time_ms else ""
        content += f"**Timestamp**: {datetime.now().isoformat()}\n\n"

        if input_text:
            content += f"## Input\n\n```\n{input_text}\n```\n\n"

        content += f"## Raw Response\n\n```\n{raw_response}\n```\n\n"

        if parsed_response:
            content += f"## Parsed Response\n\n```json\n{json.dumps(parsed_response, indent=2, ensure_ascii=False)}\n```\n"

        filepath.write_text(content, encoding="utf-8")

    def end_session(self) -> None:
        """End the current session."""
        self.session_dir = None
        self.agent_counter = 0
        self.session_timestamp = None

