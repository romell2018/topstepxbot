# -*- mode: python ; coding: utf-8 -*-

import os
import pathlib

block_cipher = None

# PyInstaller executes this spec with __file__ undefined, so fall back to CWD.
BASE_DIR = pathlib.Path.cwd()


def bundle_datas():
    datas = []
    for folder in ("Backend", "Frontend"):
        src_root = BASE_DIR / folder
        if not src_root.exists():
            continue
        for root, _dirs, files in os.walk(src_root):
            root_path = pathlib.Path(root)
            for name in files:
                full_path = root_path / name
                rel_dest = full_path.parent.relative_to(BASE_DIR)
                datas.append((str(full_path), str(rel_dest)))
    return datas


datas = bundle_datas()


a = Analysis(
    ['desktop_app.py'],
    pathex=[str(BASE_DIR)],
    binaries=[],
    datas=datas,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='DuvenchyBot',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
