"""Job executor with subprocess handling"""

import subprocess
from typing import Tuple


class JobExecutor:
    """Executes job commands via subprocess"""

    def __init__(self, timeout: int = 300):
        self.timeout = timeout

    def execute(self, command: str) -> Tuple[bool, str]:
        """
        Execute shell command, return (success, error_msg)
        Exit code 0 = success, non-zero = failure
        """
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=self.timeout
            )
            
            if result.returncode == 0:
                return True, ""
            else:
                stderr = result.stderr.strip() if result.stderr else f"Exit code {result.returncode}"
                return False, f"Exit code {result.returncode}: {stderr}"
        
        except subprocess.TimeoutExpired:
            return False, f"Command timeout after {self.timeout}s"
        
        except FileNotFoundError:
            return False, "Command not found"
        
        except Exception as e:
            return False, str(e)
