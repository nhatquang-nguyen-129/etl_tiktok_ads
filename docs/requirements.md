# Dependencies Conflict Management for TikTok Ads

## Purpose

- Installing or running Python libraries with an unsupported Python version may cause pip install errors or silent incompatibilities

---

## Install required libraries

### Windows

- Multiple Python versions can and should coexist on the same machine, especially for local development.

- Explicitly choose the correct Python interpreter if multiple versions was installed

- Create a Python virtual environment using Python 3.13 interpreter when run from the root folder
```bash
& "C:\Users\ADMIN\AppData\Local\Programs\Python\Python313\python.exe" -m venv venv
```

- Verify Python Interpreter in VS Code by opening the Command Palette `Command + Shift + P` then select `Python: Select Interpreter`

- Activate Python virtual environment and check `(venv)` in the terminal
```bash
venv/scripts/activate
```

- Verify Python virtual environment and check Python Interpreter version
```bash
python --version
```

---

### MacOS

- Multiple Python versions can and should coexist on the same machine, especially for local development.

- Explicitly choose the correct Python interpreter if multiple versions was installed

- Install Python 3.13.x on MacOS with Homebrew
```bash
brew install python@3.13
```

- Verify all installed Python versions:
```bash
which -a python3
```

- Verify Python 3.13 is available:
```bash
python3.13 --version
```

- Create a Python virtual environment using the Python 3.13 interpreter from the project root directory:
```bash
python3.13 -m venv venv
```

- Verify the virtual environment was created:
```bash
ls venv
```

- Activate the virtual environment and check `(venv)` in the terminal
```bash
source venv/bin/activate
```

- Verify the active Python interpreter:
```bash
which python
```

- Verify Python Interpreter in VS Code by opening the Command Palette `Command + Shift + P` then select `Python: Select Interpreter` and choose 
```bash
./venv/bin/python
```

---

## Use pip-tools to render exact libraries version

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

- Check installed packages
```bash
pip list
```

- Check dependency tree
```bash
pip freeze
```