"""Intent service models -- with sub_type as primary classification field."""

from pydantic import BaseModel, field_validator

from src.config.constants import Intent


class IntentResult(BaseModel):
    """Result from intent classification.

    `sub_type` is the primary classification field.
    Legacy fields (tipo_patron, arquetipo, temporality, subject_cardinality)
    are preserved for backward compatibility and are auto-populated from sub_type
    when not explicitly set.
    """

    user_question: str
    intent: Intent
    sub_type: str  # Primary field: e.g. "tendencia_comparada"

    # Chart title
    titulo_grafica: str | None = None

    # Interest rate flag
    is_tasa: bool = False

    # Reasoning
    razon: str | None = None

    # -- Legacy fields (auto-populated for backward compatibility) ----------
    tipo_patron: str | None = None
    arquetipo: str | None = None
    temporality: str | None = None
    subject_cardinality: int = 1

    @field_validator("sub_type", mode="before")
    @classmethod
    def normalize_sub_type(cls, v: str) -> str:
        """Ensure sub_type is lowercase and stripped."""
        return v.lower().strip() if isinstance(v, str) else v

    def model_post_init(self, __context: object) -> None:
        """Auto-populate legacy fields from sub_type if not set."""
        from src.config.subtypes import (
            get_legacy_archetype,
            get_legacy_pattern_type,
            get_subtype_from_string,
            get_temporality,
        )

        st = get_subtype_from_string(self.sub_type)
        if st is None:
            return

        if self.tipo_patron is None:
            self.tipo_patron = get_legacy_pattern_type(st)

        if self.arquetipo is None:
            self.arquetipo = get_legacy_archetype(st)

        if self.temporality is None:
            self.temporality = get_temporality(st)
