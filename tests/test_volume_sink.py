"""Tests for filesystem JSON writes to volume-like paths."""

import gzip
import json
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from metabricks.core.contracts import DataEnvelope
from metabricks.sinks.types import FilesystemSinkRuntimeConfig
from metabricks.sinks.filesystem_sink import FilesystemSink


@pytest.fixture
def temp_volume_dir():
    """Create temporary directory for volume testing"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def volume_sink(temp_volume_dir: str) -> FilesystemSink:
    cfg = FilesystemSinkRuntimeConfig(
        system_type="filesystem",
        mode="batch",
        format="json",
        root=temp_volume_dir,
        folder_path="raw",
        object_name="data.json",
        compression="none",
    )
    return FilesystemSink(cfg)


class TestVolumeSinkInit:
    """Test FilesystemSink initialization"""
    
    def test_sink_initializes_with_config(self, temp_volume_dir):
        cfg = FilesystemSinkRuntimeConfig(
            system_type="filesystem",
            mode="batch",
            format="json",
            root=temp_volume_dir,
            folder_path="raw",
            object_name="data.json",
            compression="none",
        )
        sink = FilesystemSink(cfg)
        assert sink.config.system_type == "filesystem"


class TestVolumeSinkWrite:
    """Test write operations for filesystem sink"""
    
    def test_write_json_no_compression(self, temp_volume_dir):
        """Test writing uncompressed JSON to volume"""
        cfg = FilesystemSinkRuntimeConfig(
            system_type="filesystem",
            mode="batch",
            format="json",
            root=temp_volume_dir,
            folder_path="raw",
            object_name="data.json",
            compression="none",
        )
        sink = FilesystemSink(cfg)
        
        test_data = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        envelope = DataEnvelope(
            payload_type="json",
            data=test_data,
            schema=None,
            context=None
        )
        
        result = sink.write(envelope)
        
        # Verify file was written
        output_file = Path(temp_volume_dir) / "raw" / "data.json"
        assert output_file.exists()
        
        # Verify content
        with open(output_file, "r") as f:
            written_data = json.load(f)
        assert written_data == test_data
        
        # Verify audit result
        assert result["status"] == "success"
        assert result["record_count"] == 2
        assert "raw/data.json" in result["target_location"]
    
    def test_write_json_with_gzip_compression(self, temp_volume_dir):
        """Test writing gzip-compressed JSON to volume"""
        cfg = FilesystemSinkRuntimeConfig(
            system_type="filesystem",
            mode="batch",
            format="json",
            root=temp_volume_dir,
            folder_path="compressed",
            object_name="data.json.gz",
            compression="gzip",
        )
        sink = FilesystemSink(cfg)
        
        test_data = [{"id": 1, "value": "test" * 100}]  # Larger data for compression benefit
        envelope = DataEnvelope(
            payload_type="json",
            data=test_data,
            schema=None,
            context=None
        )
        
        result = sink.write(envelope)
        
        # Verify file was written
        output_file = Path(temp_volume_dir) / "compressed" / "data.json.gz"
        assert output_file.exists()
        
        # Verify it's actually gzipped
        with gzip.open(output_file, "rt", encoding="utf-8") as f:
            written_data = json.load(f)
        assert written_data == test_data
        
        assert result["status"] == "success"
        assert result["record_count"] == 1
    
    def test_write_creates_nested_directories(self, temp_volume_dir: str):
        """Test that write creates nested directory structure"""
        cfg = FilesystemSinkRuntimeConfig(
            system_type="filesystem",
            mode="batch",
            format="json",
            root=temp_volume_dir,
            folder_path="nested/deep/path",
            object_name="data.json",
            compression="none",
        )
        sink = FilesystemSink(cfg)
        
        test_data = [{"id": 1}]
        envelope = DataEnvelope(
            payload_type="json",
            data=test_data,
            schema=None,
            context=None
        )
        
        result = sink.write(envelope)
        
        output_file = Path(temp_volume_dir) / "nested" / "deep" / "path" / "data.json"
        assert output_file.exists()
        assert result["status"] == "success"
    
    def test_write_empty_list(self, volume_sink: FilesystemSink, temp_volume_dir: str):
        """Test writing empty list"""
        # Modify sink for this test
        volume_sink.config.folder_path = "empty"
        volume_sink.config.object_name = "empty.json"
        
        test_data = []
        envelope = DataEnvelope(
            payload_type="json",
            data=test_data,
            schema=None,
            context=None
        )
        
        result = volume_sink.write(envelope)
        
        assert result["status"] == "success"
        assert result["record_count"] == 0
    
    def test_write_single_record(self, volume_sink: FilesystemSink, temp_volume_dir: str):
        """Test writing single record (non-list)"""
        # Modify sink for this test
        volume_sink.config.folder_path = "single"
        volume_sink.config.object_name = "single.json"
        
        test_data = {"id": 1, "name": "single_record"}
        envelope = DataEnvelope(
            payload_type="json",
            data=test_data,
            schema=None,
            context=None
        )
        
        result = volume_sink.write(envelope)
        
        assert result["status"] == "success"
        assert result["record_count"] == 1


class TestVolumeSinkCompressionCodecs:
    """Test different compression codecs"""
    
    @pytest.mark.parametrize("codec,filename", [
        ("none", "data.json"),
        ("gzip", "data.json.gz"),
        ("gz", "data.json.gz"),
        ("gzip", "data.tar.gz"),
    ])
    def test_compression_codec_variants(self, temp_volume_dir, codec: str, filename: str):
        """Test various compression codec specifications"""
        cfg = FilesystemSinkRuntimeConfig(
            system_type="filesystem",
            mode="batch",
            format="json",
            root=temp_volume_dir,
            folder_path="codec_test",
            object_name=filename,
            compression=codec,
        )
        sink = FilesystemSink(cfg)
        
        test_data = [{"id": i} for i in range(10)]
        envelope = DataEnvelope(
            payload_type="json",
            data=test_data,
            schema=None,
            context=None
        )
        
        result = sink.write(envelope)
        
        assert result["status"] == "success"
        output_file = Path(temp_volume_dir) / "codec_test" / filename
        assert output_file.exists()


class TestVolumeSinkAuditResult:
    """Test audit result structure and content"""
    
    def test_audit_result_contains_required_fields(self, volume_sink: FilesystemSink, temp_volume_dir: str):
        """Test audit result has all required fields"""
        # Modify sink for this test
        volume_sink.config.folder_path = "audit"
        volume_sink.config.object_name = "data.json"
        
        test_data = [{"id": 1}, {"id": 2}]
        envelope = DataEnvelope(
            payload_type="json",
            data=test_data,
            schema=None,
            context=None
        )
        
        result = volume_sink.write(envelope)
        
        assert "extract_time_utc" in result
        assert "target_location" in result
        assert "status" in result
        assert "record_count" in result
        assert result["record_count"] == 2
    
    def test_audit_timestamp_format(self, volume_sink: FilesystemSink, temp_volume_dir: str):
        """Test audit result has a parseable ISO timestamp."""
        # Modify sink for this test
        volume_sink.config.folder_path = "time"
        volume_sink.config.object_name = "data.json"
        
        test_data = [{"id": 1}]
        envelope = DataEnvelope(
            payload_type="json",
            data=test_data,
            schema=None,
            context=None
        )
        
        result = volume_sink.write(envelope)
        
        timestamp = result["extract_time_utc"]
        datetime.fromisoformat(timestamp)


class TestVolumeSinkOverwrite:
    """Test file overwrite behavior"""
    
    def test_write_overwrites_existing_file(self, volume_sink: FilesystemSink, temp_volume_dir: str):
        """Test that writing to same file overwrites previous content"""
        # Modify sink for this test
        volume_sink.config.folder_path = "overwrite_test"
        volume_sink.config.object_name = "data.json"
        
        # First write
        data1 = [{"id": 1, "version": "first"}]
        envelope1 = DataEnvelope(
            payload_type="json",
            data=data1,
            schema=None,
            context=None
        )
        result1 = volume_sink.write(envelope1)
        
        # Second write to same file
        data2 = [{"id": 2, "version": "second"}]
        envelope2 = DataEnvelope(
            payload_type="json",
            data=data2,
            schema=None,
            context=None
        )
        result2 = volume_sink.write(envelope2)
        
        # Verify only second write exists
        output_file = Path(temp_volume_dir) / "overwrite_test" / "data.json"
        with open(output_file, "r") as f:
            final_data = json.load(f)
        
        assert final_data == data2
        assert result1["status"] == "success"
        assert result2["status"] == "success"
