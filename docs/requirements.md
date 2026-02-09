# Dependencies Conflict Management

## Purpose
- Installing or running Python libraries with an unsupported Python version may cause pip install errors
- Installing or running Python libraries with an unsupported Python version may cause silent incompatibilities


## Python Version Conflict Handling







Because of this, multiple Python versions can and should coexist on the same machine, especially for local development.

## Requirements Conflict Handling

- Edit `base.in` to change dependencies
- Run `pip install pip-tools` to install pip-tools
- Run `pip-compile requirements/base.in -o requirements/base.txt` to update lock file
- Install using `pip install` 
```bash
pip install -r requirements/base.txt`

- Do NOT edit `.txt` files manually to avoid conflict

# Đứng ở root folder repo
& "C:\Users\ADMIN\AppData\Local\Programs\Python\Python313\python.exe" -m venv venv
