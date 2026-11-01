"""Resolve the visible Windows Desktop used for special-swing workbook publication."""
from __future__ import annotations

import ctypes
import os
from ctypes import wintypes
from dataclasses import dataclass
from pathlib import Path

from detectors.swing_open_close_30m_special_v1.config import PROJECT_ROOT

KNOWN_FOLDER = "known_folder"
USER_SHELL_FOLDERS = "user_shell_folders"
HOME_DESKTOP = "home_desktop"
_FOLDERID_DESKTOP = (0xB4BFCC3A, 0xDB2C, 0x424C, (0xB0, 0x29, 0x7F, 0xE9, 0x9A, 0x87, 0xC6, 0x41))
_USER_SHELL_KEY = r"Software\Microsoft\Windows\CurrentVersion\Explorer\User Shell Folders"
_cached: "DesktopLocation | None" = None


class DesktopResolutionError(RuntimeError):
    """Raised when a Desktop path cannot be used as the workbook destination."""


class KnownFolderUnavailable(DesktopResolutionError):
    """Raised when the Known Folder API or a fallback source cannot be called."""


class _GUID(ctypes.Structure):
    _fields_ = (
        ("Data1", wintypes.DWORD),
        ("Data2", wintypes.WORD),
        ("Data3", wintypes.WORD),
        ("Data4", ctypes.c_ubyte * 8),
    )


@dataclass(frozen=True, slots=True)
class DesktopLocation:
    path: Path
    method: str


def visible_desktop() -> DesktopLocation:
    """Return the process-wide visible Desktop, resolving it on first use."""
    global _cached
    if _cached is None:
        _cached = resolve_visible_desktop()
    return _cached


def clear_desktop_cache() -> None:
    global _cached
    _cached = None


def resolve_visible_desktop(
    *,
    known_folder=None,
    registry_desktop=None,
    home: Path | None = None,
) -> DesktopLocation:
    """Resolve one absolute Desktop directory and record the method used."""
    known = _known_folder_desktop if known_folder is None else known_folder
    try:
        raw = known()
    except KnownFolderUnavailable:
        raw = None
    else:
        return _finalize(raw, KNOWN_FOLDER)
    registry = _registry_desktop if registry_desktop is None else registry_desktop
    try:
        raw = registry()
    except KnownFolderUnavailable:
        raw = None
    else:
        return _finalize(raw, USER_SHELL_FOLDERS)
    home_dir = Path.home() if home is None else Path(home)
    return _finalize(str(home_dir / "Desktop"), HOME_DESKTOP)


def _known_folder_desktop() -> str:
    if os.name != "nt":
        raise KnownFolderUnavailable("Windows Known Folder API is not available")
    try:
        shell32 = ctypes.windll.shell32
        ole32 = ctypes.windll.ole32
    except (AttributeError, OSError) as exc:
        raise KnownFolderUnavailable("shell32 is unavailable") from exc
    if not hasattr(shell32, "SHGetKnownFolderPath"):
        raise KnownFolderUnavailable("SHGetKnownFolderPath is unavailable")
    data1, data2, data3, data4 = _FOLDERID_DESKTOP
    folder_id = _GUID(data1, data2, data3, (ctypes.c_ubyte * 8)(*data4))
    output = ctypes.c_wchar_p()
    shell32.SHGetKnownFolderPath.argtypes = [
        ctypes.POINTER(_GUID),
        wintypes.DWORD,
        wintypes.HANDLE,
        ctypes.POINTER(ctypes.c_wchar_p),
    ]
    shell32.SHGetKnownFolderPath.restype = ctypes.c_long
    result = int(shell32.SHGetKnownFolderPath(ctypes.byref(folder_id), 0, None, ctypes.byref(output)))
    if result != 0:
        raise KnownFolderUnavailable(f"SHGetKnownFolderPath failed with HRESULT {result}")
    try:
        value = output.value
    finally:
        if output:
            ole32.CoTaskMemFree.argtypes = [ctypes.c_void_p]
            ole32.CoTaskMemFree.restype = None
            ole32.CoTaskMemFree(ctypes.cast(output, ctypes.c_void_p))
    if not value or not str(value).strip():
        raise DesktopResolutionError("Windows Known Folder Desktop path is empty")
    return str(value)


def _registry_desktop() -> str:
    if os.name != "nt":
        raise KnownFolderUnavailable("User Shell Folders registry is not available")
    try:
        import winreg

        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, _USER_SHELL_KEY) as key:
            value, _kind = winreg.QueryValueEx(key, "Desktop")
    except OSError as exc:
        raise KnownFolderUnavailable("User Shell Folders Desktop value is unavailable") from exc
    if not isinstance(value, str) or not value.strip():
        raise KnownFolderUnavailable("User Shell Folders Desktop value is empty")
    return value


def _finalize(raw: str, method: str) -> DesktopLocation:
    text = os.path.expandvars(str(raw)).strip().strip('"')
    if not text:
        raise DesktopResolutionError(f"{method} Desktop path is empty")
    path = Path(text).expanduser()
    path = path.resolve(strict=False) if path.is_absolute() else path.resolve()
    if not path.is_absolute():
        raise DesktopResolutionError(f"{method} Desktop path is not absolute: {path}")
    project = PROJECT_ROOT.resolve()
    if path == project or project in path.parents:
        raise DesktopResolutionError(
            f"{method} Desktop path {path} is inside the project and cannot receive workbooks"
        )
    if path.exists():
        if not path.is_dir():
            raise DesktopResolutionError(f"{method} Desktop path is not a directory: {path}")
    else:
        if not path.parent.is_dir():
            raise DesktopResolutionError(f"{method} Desktop parent does not exist: {path.parent}")
        path.mkdir()
    return DesktopLocation(path, method)
