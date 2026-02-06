## Dependencies Conflict Management

- Edit `base.in` to change dependencies
- Run `pip install pip-tools` to install pip-tools
- Run `pip-compile requirements/base.in -o requirements/base.txt` to update lock file
- Install using `pip install` 
```bash
pip install -r requirements/base.txt`

- Do NOT edit `.txt` files manually to avoid conflict