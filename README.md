# Discord Cloud

A self-hosted dashboard that packages files into ZIPs, stores Discord-safe parts in a private Discord server, and recovers verified downloads.

Built and used on a Raspberry Pi 4. The Python app also runs on Windows 11.

## Features

- Upload files or folders without opening their contents
- Split and verify Discord uploads with SHA-256 hashes
- Browse, download, rename, verify, and delete archives from one dashboard
- Keep temporary Pi files off disk after each job

## Setup

Create a Discord bot, add it to one server, and give it View Channels, Manage Channels, Send Messages, Attach Files, Read Message History, and Manage Messages.

Copy the safe template, then fill in your bot token, server ID, dashboard password, and session secret:

~~~bash
cp .env.example .env
openssl rand -hex 32
~~~

### Raspberry Pi 4 or Linux

~~~bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python app.py
~~~

Open http://PI_ADDRESS:8080. Use hostname -I on the Pi to find the address.

### Windows 11

~~~powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
py app.py
~~~

Open http://localhost:8080.

## Raspberry Pi service

~~~bash
chmod +x install.sh
sudo ./install.sh
sudo systemctl status discord-cloud
~~~

## Keep private

Never commit .env or state.json. The included .gitignore also excludes keys, sessions, logs, virtual environments, and editor files.

Discord is storage, not your only backup. Archive contents are not encrypted by this app, so encrypt sensitive files before uploading them.

## Verify

~~~bash
python app.py --self-test
~~~

## License

[MIT](LICENSE)