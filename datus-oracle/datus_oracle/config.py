# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class OracleConfig(BaseModel):
    """Configuration options for Oracle connections."""

    model_config = ConfigDict(extra="forbid")

    host: str = Field(..., description="Oracle server host")
    port: int = Field(default=1521, ge=1, le=65535, description="Oracle listener port")
    username: str = Field(..., description="Oracle username")
    password: str = Field(..., description="Oracle password")
    database: Optional[str] = Field(default=None, description="Service name to connect to")
    sid: Optional[str] = Field(default=None, description="SID to connect to (alternative to service_name)")
    schema_name: Optional[str] = Field(default=None, description="Default schema (defaults to username)")
    client_lib_dir: Optional[str] = Field(default=None, description="Oracle Instant Client directory for thick mode")
    timeout_seconds: int = Field(default=30, ge=1, description="Connection timeout in seconds")

    @model_validator(mode="after")
    def _validate_identifier(self) -> "OracleConfig":
        """Ensure either database (service name) or sid is provided."""
        if not self.database and not self.sid:
            raise ValueError("Either database or sid must be provided")
        return self
