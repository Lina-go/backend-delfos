"""Tests for pattern hooks registry."""

from src.patterns.registry import PatternHooks, get_hooks, register


class TestPatternHooksRegistry:
    def test_register_and_retrieve(self):
        hooks = PatternHooks(get_chart_type=lambda st: "test")
        register("test_type", hooks)
        retrieved = get_hooks("test_type")
        assert retrieved.get_chart_type is not None
        assert retrieved.get_chart_type("x") == "test"

    def test_get_hooks_none_returns_empty(self):
        hooks = get_hooks(None)
        assert hooks.enrich_sql_prompt is None
        assert hooks.post_process is None
        assert hooks.build_data_points is None
        assert hooks.get_chart_type is None

    def test_get_hooks_unknown_returns_empty(self):
        hooks = get_hooks("nonexistent_sub_type_xyz")
        assert hooks.enrich_sql_prompt is None
        assert hooks.get_chart_type is None

    def test_relacion_auto_registered(self):
        """Importing src.patterns auto-registers relacion hooks."""
        import src.patterns  # noqa: F401

        hooks = get_hooks("relacion")
        assert hooks.enrich_sql_prompt is not None
        assert hooks.post_process is not None
        assert hooks.build_data_points is not None
        assert hooks.get_chart_type is not None

    def test_covariacion_auto_registered(self):
        import src.patterns  # noqa: F401

        hooks = get_hooks("covariacion")
        assert hooks.enrich_sql_prompt is not None

    def test_empty_hooks_all_none(self):
        hooks = PatternHooks()
        assert hooks.enrich_sql_prompt is None
        assert hooks.post_process is None
        assert hooks.build_data_points is None
        assert hooks.get_chart_type is None
