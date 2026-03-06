# Dependencies Conflict Management for TikTok Ads

## Purpose

- Installing or running Python libraries with an unsupported Python version may cause pip install errors or silent incompatibilities

---

## Install

### Switch to Python 3.13 Interpreter

- Multiple Python versions can and should coexist on the same machine, especially for local development.

- Explicitly choose the correct Python interpreter if multiple versions was installed

- Create a Python virtual environment using Python 3.13 interpreter when run from the root folder
```bash
& "C:\Users\ADMIN\AppData\Local\Programs\Python\Python313\python.exe" -m venv venv
```

- Check available Python interpreter if there is any uncertainty by press `Ctrl + Shift + P` then select `Python: Select Interpreter`

- Activate Python virtual environment and check `(venv)` in the terminal
```bash
venv/scripts/activate
```

- Verify Python virtual environment and check Python Interpreter version
```bash
python --version
```

---

### Use pip-tools to render exact libraries version

- Edit `base.in` to change dependencies instead `.txt` files manually to avoid conflict

- Install `pip-tools` with pip
```bash
pip install pip-tools
```

- Compile `base.in` requirements
```bash
pip-compile requirements/base.in -o requirements/base.txt
```

- Install exact libraries version
```bash
pip install -r requirements/base.txt
```