"""以「脫離父程序」的方式啟動 LAB 指數監控，讓它在啟動者(終端/工作階段)結束後仍存活。

仿 5011 launcher.py 的做法：detached + no window，輸出導到 logs/lab_monitor.log。
用法：python monitor_launcher.py（或雙擊 start_lab_monitor.bat）
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(HERE, "logs", "lab_monitor.log")
os.makedirs(os.path.join(HERE, "logs"), exist_ok=True)

# DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW
FLAGS = 0x00000008 | 0x00000200 | 0x08000000

logf = open(LOG, "a", encoding="utf-8")
env = dict(os.environ, PYTHONIOENCODING="utf-8")
p = subprocess.Popen(
    [sys.executable, "monitor_lab_index.py"],
    cwd=HERE,
    creationflags=FLAGS,
    stdin=subprocess.DEVNULL,
    stdout=logf,
    stderr=logf,
    env=env,
    close_fds=True,
)
print(f"LAB 指數監控已啟動（PID {p.pid}），log：{LOG}")
