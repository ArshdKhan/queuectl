"""Configuration management"""

import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


@dataclass
class Config:
    """Application configuration"""
    max_retries: int = 3
    backoff_base: float = 2.0
    db_path: str = str(Path.home() / ".queuectl" / "queue.db")
    worker_poll_interval: float = 1.0
    job_timeout: int = 300

    CONFIG_FILE = Path.home() / ".queuectl" / "config.json"

    @classmethod
    def load(cls) -> 'Config':
        """Load configuration from file or return defaults"""
        if cls.CONFIG_FILE.exists():
            try:
                with open(cls.CONFIG_FILE, 'r') as f:
                    data = json.load(f)
                    return cls(**data)
            except Exception:
                return cls()
        return cls()

    def save(self) -> None:
        """Save configuration to file"""
        self.CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(self.CONFIG_FILE, 'w') as f:
            json.dump(asdict(self), f, indent=2)

    def set(self, key: str, value: Any) -> None:
        """Set a configuration value and save"""
        if hasattr(self, key):
            setattr(self, key, value)
            self.save()
        else:
            raise ValueError(f"Unknown configuration key: {key}")

    def get(self, key: str) -> Any:
        """Get a configuration value"""
        if hasattr(self, key):
            return getattr(self, key)
        raise ValueError(f"Unknown configuration key: {key}")
