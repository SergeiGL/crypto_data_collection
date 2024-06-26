import sys
from typing import Dict
from functools import lru_cache, cached_property


class ColoredOutput:
    ANSI_GREEN = '\033[92m'
    ANSI_RED = '\033[91m'
    ANSI_END = '\033[0m'
    
    WINDOWS_GREEN = "\x1b[32m"
    WINDOWS_RED = "\x1b[31m"
    WINDOWS_END = "\x1b[0m"

    @staticmethod
    @lru_cache(maxsize=1)
    def _get_os_colors() -> Dict[str, str]:
        is_win32 = sys.platform == "win32"
        return {
            'GREEN': ColoredOutput.WINDOWS_GREEN if is_win32 else ColoredOutput.ANSI_GREEN,
            'RED': ColoredOutput.WINDOWS_RED if is_win32 else ColoredOutput.ANSI_RED,
            'END': ColoredOutput.WINDOWS_END if is_win32 else ColoredOutput.ANSI_END,
            'IS_WIN32': is_win32
        }

    @staticmethod
    def _enable_ansi_escape_for_windows() -> None:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)

    @staticmethod
    @cached_property
    def _console_mode_set() -> bool:
        colors = ColoredOutput._get_os_colors()
        if colors['IS_WIN32']:
            ColoredOutput._enable_ansi_escape_for_windows()
        return True

    @classmethod
    def _print_colored(cls, message: str, color: str) -> None:
        colors = cls._get_os_colors()
        _ = cls._console_mode_set  # Initialize console mode if required
        print(f"{colors[color]}{message}{colors['END']}")

    @classmethod
    def green(cls, message: str) -> None:
        cls._print_colored(message, 'GREEN')

    @classmethod
    def red(cls, message: str) -> None:
        cls._print_colored(message, 'RED')

