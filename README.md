# DuvenchyBot Desktop Bundle

This project wraps **DuvenchyBot**, a TopstepX trading bot, with a Quart web dashboard and a Tkinter desktop launcher. You can run it directly from VS Code for development or ship a PyInstaller build so users can launch the bot like a native app.

---

## Features

- Live trading bot that talks to TopstepX REST/SignalR APIs.
- Quart REST endpoints plus a browser UI for monitoring balance, equity, price, and trade status.
- Desktop launcher (`desktop_app.py`) that mirrors the web dashboard layout, with credential inputs, controllable max contracts and trailing ticks, start/stop control, log streaming, and a dashboard shortcut.
- Config override via environment variables (`TOPSTEPX_CONFIG` / `TOPSTEPX_CONFIG_JSON`), so the GUI can inject credentials without editing YAML.

---

## Repository Layout

- `Backend/duvenchy_topstepx_projectx_bot.py` – main entry point (Quart app + strategy wiring).
- `Backend/topstepx_bot/` – shared modules for API access, indicators, monitors, streamer, etc.
- `Frontend/` – static assets served at `http://127.0.0.1:5000/` for the in-browser dashboard.
- `desktop_app.py` – Tkinter desktop GUI.
- `topstepx_desktop.spec` – PyInstaller spec for packaging (outputs `DuvenchyBot`).

---

## Getting Started in VS Code

1. Clone or download the repo into a local workspace and open the folder in VS Code.
2. Create the virtual environment (optional if you use the existing `Backend/.venv`):
   ```bash
   python3 -m venv Backend/.venv
   ```
3. In VS Code, select the interpreter `Backend/.venv/bin/python` (Command Palette → *Python: Select Interpreter*).
4. Install dependencies (inside the venv):
   ```bash
   source Backend/.venv/bin/activate
   pip install -r Backend/requirements.txt
   ```
5. Launch the bot directly:
   ```bash
   source Backend/.venv/bin/activate
   python Backend/duvenchy_topstepx_projectx_bot.py
   ```
   Visit `http://127.0.0.1:5000/` to view the dashboard.
6. Launch the desktop GUI instead:
   ```bash
   source Backend/.venv/bin/activate
   python3 desktop_app.py
   ```
   Use the **Settings** button to update username, API key, account ID, API base, max contracts, and trailing distance ticks. When ready, click **Start DuvenchyBot**, and optionally open the dashboard from the button.

### Debugging in VS Code

- Add a launch configuration pointing to `Backend/duvenchy_topstepx_projectx_bot.py` or `desktop_app.py`.
- Set breakpoints in backend modules; the desktop app starts the same script, so debugging applies there too.

---

## Running Without VS Code (From Source)

1. Ensure Python 3.11+ and Tkinter are installed on your machine (`python3-tk` on Debian/Ubuntu).
2. In a terminal:
   ```bash
   cd topstepxbot
   python3 -m venv Backend/.venv
   source Backend/.venv/bin/activate
   pip install -r Backend/requirements.txt
   python3 desktop_app.py
   ```
3. Click **Settings** to enter username, API key, account ID, API base, max contracts, and trailing distance ticks. Press **Start DuvenchyBot** when you’re ready.

---

## Building a Distributable ZIP

The desktop GUI can be packaged so end users don’t need Python or VS Code.

1. Install build dependencies in a dedicated venv (recommended):
   ```bash
   python3 -m venv build-env
   source build-env/bin/activate
   pip install --upgrade pip
   pip install pyinstaller pyyaml
   ```
2. From the repo root, run PyInstaller using the provided spec:
   ```bash
   pyinstaller topstepx_desktop.spec
   ```
3. After the build finishes, the runnable bundle lives in `dist/DuvenchyBot/`.
   - On Linux/macOS, run `./DuvenchyBot`.
   - On Windows, run `DuvenchyBot.exe` (build on Windows for a native exe).
4. Zip the entire `dist/DuvenchyBot/` folder and share it with users. They just extract and launch the executable, enter credentials, and click **Start DuvenchyBot**.
5. Rebuild for each operating system when you update the bot.

---

## Updating the Bot

Any time you modify backend logic, frontend files, or the desktop GUI:

1. Retest from source with `python3 desktop_app.py`.
2. Re-run `pyinstaller topstepx_desktop.spec` on each target OS.
3. Replace the old distribution ZIPs with the new ones.

---

## Troubleshooting

- **`ModuleNotFoundError: tkinter`** – install Tk (`sudo apt install python3-tk` on Debian/Ubuntu).
- **`ModuleNotFoundError: yaml`** – the desktop app can run without PyYAML, but running the backend directly from `config.yaml` still requires installing `PyYAML` (`pip install pyyaml`).
- **Network errors on startup** – ensure the machine has internet and valid TopstepX credentials. The desktop log panel shows live errors.
- **PyInstaller build failures** – check the log for missing system libraries (e.g., Tk on Linux) or run the build with `--clean`.

---

## Licensing & Security

- PPE your API keys—share only with trusted users.
- The repository contains placeholder credentials in `config.yaml`; replace with your own or rely on the GUI input.
- Review TopstepX terms before running the bot on live accounts.

Happy trading!
