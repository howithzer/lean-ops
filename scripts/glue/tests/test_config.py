"""
Unit tests for utils/config.py
"""

import os
import pytest
from utils.config import (
    Config,
    get_env,
    get_env_bool,
    get_env_int,
    get_logger,
    ENVELOPE_COLUMN_VARIANTS,
    PRESERVE_TYPE_COLUMNS,
)


class TestGetEnvHelpers:
    """Tests for environment variable helper functions."""
    
    def test_get_env_returns_value(self, clean_env):
        clean_env["TEST_KEY"] = "test_value"
        assert get_env("TEST_KEY") == "test_value"
    
    def test_get_env_returns_default(self, clean_env):
        clean_env.pop("NONEXISTENT", None)
        assert get_env("NONEXISTENT", "default") == "default"
    
    def test_get_env_bool_true_values(self, clean_env):
        for val in ["true", "True", "TRUE", "1", "yes", "on"]:
            clean_env["BOOL_KEY"] = val
            assert get_env_bool("BOOL_KEY") is True, f"Failed for {val}"
    
    def test_get_env_bool_false_values(self, clean_env):
        for val in ["false", "False", "FALSE", "0", "no", "off"]:
            clean_env["BOOL_KEY"] = val
            assert get_env_bool("BOOL_KEY") is False, f"Failed for {val}"
    
    def test_get_env_bool_default(self, clean_env):
        clean_env.pop("NONEXISTENT", None)
        assert get_env_bool("NONEXISTENT", True) is True
        assert get_env_bool("NONEXISTENT", False) is False
    
    def test_get_env_int_valid(self, clean_env):
        clean_env["INT_KEY"] = "42"
        assert get_env_int("INT_KEY") == 42
    
    def test_get_env_int_invalid_returns_default(self, clean_env):
        clean_env["INT_KEY"] = "not_a_number"
        assert get_env_int("INT_KEY", 10) == 10
    
    def test_get_env_int_missing_returns_default(self, clean_env):
        clean_env.pop("NONEXISTENT", None)
        assert get_env_int("NONEXISTENT", 99) == 99


class TestConfig:
    """Tests for Config dataclass."""
    
    def test_config_defaults(self):
        config = Config()
        assert config.topic_name == "events"
        assert config.raw_database == "iceberg_raw_db"
        assert config.curated_database == "iceberg_curated_db"
        assert config.max_flatten_depth == 5
        assert config.enable_deep_flatten is True
    
    def test_config_from_env(self, sample_config_env):
        config = Config.from_env()
        assert config.topic_name == "test_topic"
        assert config.raw_database == "test_raw_db"
        assert config.curated_database == "test_curated_db"
        assert config.enable_deep_flatten is True
        assert config.max_flatten_depth == 3
    
    def test_config_from_env_with_overrides(self, sample_config_env):
        config = Config.from_env(topic_name="override_topic")
        assert config.topic_name == "override_topic"
        assert config.raw_database == "test_raw_db"  # From env
    
    def test_config_is_immutable(self):
        config = Config()
        with pytest.raises(Exception):  # FrozenInstanceError
            config.topic_name = "new_value"
    
    def test_config_iceberg_warehouse_property(self):
        config = Config(iceberg_bucket="my-bucket")
        assert config.iceberg_warehouse == "s3://my-bucket/"
    
    def test_config_raw_table_property(self):
        config = Config(raw_database="raw_db", topic_name="my_topic")
        assert config.raw_table == "glue_catalog.raw_db.my_topic_staging"
    
    def test_config_curated_table_property(self):
        config = Config(curated_database="curated_db")
        assert config.curated_table == "glue_catalog.curated_db.events"


class TestLogger:
    """Tests for logging configuration."""
    
    def test_get_logger_returns_logger(self):
        logger = get_logger("test.module")
        assert logger.name == "test.module"
    
    def test_get_logger_same_instance(self):
        logger1 = get_logger("same.module")
        logger2 = get_logger("same.module")
        assert logger1 is logger2


class TestConstants:
    """Tests for configuration constants."""
    
    def test_envelope_column_variants_contains_expected(self):
        assert "idempotency_key" in ENVELOPE_COLUMN_VARIANTS
        assert "idempotencykey" in ENVELOPE_COLUMN_VARIANTS
        assert "message_id" in ENVELOPE_COLUMN_VARIANTS
        assert "json_payload" in ENVELOPE_COLUMN_VARIANTS
    
    def test_envelope_column_variants_is_frozen(self):
        with pytest.raises(AttributeError):
            ENVELOPE_COLUMN_VARIANTS.add("new_column")
    
    def test_preserve_type_columns_contains_ingestion_ts(self):
        assert "ingestion_ts" in PRESERVE_TYPE_COLUMNS
