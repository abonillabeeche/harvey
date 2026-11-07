"""Pluggable storage backends. Each backend replicates the disks of a VM with one storage engine.

To add a backend: subclass StorageBackend, set `name` and `drivers`, decorate it with @register.
The orchestrator picks the backend per VM from the CSI driver of its PVs; `replication.backend` in the config
forces one.
"""

from __future__ import annotations

from typing import Dict, Optional, Type

from .base import StorageBackend

REGISTRY: Dict[str, Type[StorageBackend]] = {}


def register(cls: Type[StorageBackend]) -> Type[StorageBackend]:
    REGISTRY[cls.name] = cls
    return cls


def backend_for_driver(driver: Optional[str]) -> Optional[str]:
    for name, cls in REGISTRY.items():
        if driver in cls.drivers:
            return name
    return None


# built-in backends (import registers them)
from . import netapp, portworx  # noqa: E402,F401

__all__ = ["REGISTRY", "StorageBackend", "backend_for_driver", "register"]
