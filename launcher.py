"""啟動後端（確保子程序在父 batch 退出後存活）
注意：前端需先 `cd frontend && npm run build`，backend 會直接 serve 靜態檔案，
不需要 Vite dev server。
"""
import subprocess
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FLAGS = 0x08000000  # CREATE_NO_WINDOW

# Backend（同時 serve API + 靜態前端）
subprocess.Popen(
    [sys.executable, "main.py"],
    cwd=os.path.join(HERE, "backend"),
    creationflags=FLAGS,
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
