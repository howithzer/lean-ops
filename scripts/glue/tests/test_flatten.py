"""
Unit tests for utils/flatten.py
"""

import json
import pytest
from utils.flatten import (
    recursive_flatten,
    sanitize_column_name,
    discover_payload_keys,
)


class TestSanitizeColumnName:
    """Tests for column name sanitization."""
    
    def test_replaces_dashes(self):
        assert sanitize_column_name("user-id") == "user_id"
        assert sanitize_column_name("first-name-last") == "first_name_last"
    
    def test_replaces_dots(self):
        assert sanitize_column_name("user.email") == "user_email"
        assert sanitize_column_name("a.b.c") == "a_b_c"
    
    def test_converts_to_lowercase(self):
        assert sanitize_column_name("UserName") == "username"
        assert sanitize_column_name("FirstName") == "firstname"
    
    def test_combined_transformations(self):
        assert sanitize_column_name("User-ID.Type") == "user_id_type"
    
    def test_empty_string(self):
        assert sanitize_column_name("") == ""
    
    def test_already_clean(self):
        assert sanitize_column_name("clean_name") == "clean_name"


class TestRecursiveFlatten:
    """Tests for recursive JSON flattening."""
    
    def test_flat_dict(self):
        obj = {"userId": "123", "eventType": "click"}
        result = dict(recursive_flatten(obj))
        assert result["userid"] == "123"
        assert result["eventtype"] == "click"
    
    def test_nested_dict(self):
        obj = {"user": {"id": "456", "name": "John"}}
        result = dict(recursive_flatten(obj))
        assert result["user_id"] == "456"
        assert result["user_name"] == "John"
    
    def test_deeply_nested(self):
        obj = {"a": {"b": {"c": {"d": "value"}}}}
        result = dict(recursive_flatten(obj, max_depth=5))
        assert result["a_b_c_d"] == "value"
    
    def test_max_depth_reached(self):
        obj = {"a": {"b": {"c": {"d": {"e": "deep"}}}}}
        result = dict(recursive_flatten(obj, max_depth=2))
        # At depth 2, should stop at level 2 and serialize remaining as JSON
        # depth 0: a -> depth 1: b -> depth 2: c (serializes remaining)
        assert "a_b" in result
        # The value should be a JSON string containing c and below
        assert isinstance(result["a_b"], str)
        assert "c" in result["a_b"]
    
    def test_array_becomes_json_string(self):
        obj = {"items": [1, 2, 3]}
        result = dict(recursive_flatten(obj))
        assert result["items"] == "[1, 2, 3]"
    
    def test_null_value(self):
        obj = {"userId": None, "eventType": "view"}
        result = dict(recursive_flatten(obj))
        assert result["userid"] is None
        assert result["eventtype"] == "view"
    
    def test_empty_dict(self):
        obj = {}
        result = list(recursive_flatten(obj))
        assert result == []
    
    def test_special_characters_in_keys(self):
        obj = {"user-id": "789", "event.type": "purchase"}
        result = dict(recursive_flatten(obj))
        assert result["user_id"] == "789"
        assert result["event_type"] == "purchase"
    
    def test_boolean_values(self):
        obj = {"isActive": True, "isDeleted": False}
        result = dict(recursive_flatten(obj))
        assert result["isactive"] == "True"
        assert result["isdeleted"] == "False"
    
    def test_numeric_values(self):
        obj = {"count": 42, "price": 19.99}
        result = dict(recursive_flatten(obj))
        assert result["count"] == "42"
        assert result["price"] == "19.99"
    
    def test_prefix_handling(self):
        obj = {"nested": {"key": "value"}}
        result = dict(recursive_flatten(obj, prefix="parent"))
        assert result["parent_nested_key"] == "value"


class TestRecursiveFlattenEdgeCases:
    """Edge case tests for recursive flattening."""
    
    def test_deeply_nested_beyond_max_depth(self):
        # Create 10 levels of nesting
        obj = {"l1": {"l2": {"l3": {"l4": {"l5": {"l6": {"l7": {"l8": {"l9": {"l10": "deep"}}}}}}}}}}
        result = dict(recursive_flatten(obj, max_depth=3))
        # Should stop at depth 3 and serialize the rest
        # depth 0: l1 -> depth 1: l2 -> depth 2: l3 -> depth 3: l4 (serializes remaining)
        assert "l1_l2_l3" in result
        assert "l4" in result["l1_l2_l3"]
    
    def test_mixed_types_in_dict(self):
        obj = {
            "string": "text",
            "number": 123,
            "boolean": True,
            "null": None,
            "array": [1, 2],
            "nested": {"key": "value"}
        }
        result = dict(recursive_flatten(obj))
        assert result["string"] == "text"
        assert result["number"] == "123"
        assert result["boolean"] == "True"
        assert result["null"] is None
        assert result["array"] == "[1, 2]"
        assert result["nested_key"] == "value"
    
    def test_unicode_values(self):
        obj = {"name": "José", "city": "北京"}
        result = dict(recursive_flatten(obj))
        assert result["name"] == "José"
        assert result["city"] == "北京"
    
    def test_empty_nested_dict(self):
        obj = {"outer": {"inner": {}}}
        result = dict(recursive_flatten(obj))
        # Empty nested dict should produce no keys from inner
        assert "outer_inner" not in result or result.get("outer_inner") == "{}"
