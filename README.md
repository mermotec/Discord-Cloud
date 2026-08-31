<h1 align="center">Discord Cloud</h1>

<p align="center">
  <strong>Private Discord storage with a clean local dashboard.</strong><br>
  Built and used on Raspberry Pi 4. Runs on Windows 11 too.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Platform-Raspberry%20Pi%204%20%7C%20Windows%2011-1B263B?style=for-the-badge&logo=raspberrypi&logoColor=white" alt="Raspberry Pi 4 and Windows 11">
  <img src="https://img.shields.io/badge/License-MIT-0F766E?style=for-the-badge" alt="MIT license">
</p>

<p align="center">
  <a href="#quick-start">Quick start</a>
  &nbsp;·&nbsp;
  <a href="#configuration">Configuration</a>
  &nbsp;·&nbsp;
  <a href="#raspberry-pi-service">Pi service</a>
  &nbsp;·&nbsp;
  <a href="#security">Security</a>
</p>

<br>

<table>
  <tr>
    <td width="33%" align="center">
      <strong>01</strong><br>
      <sub>DROP FILES</sub><br><br>
      Select files or complete folders from the dashboard.
    </td>
    <td width="33%" align="center">
      <strong>02</strong><br>
      <sub>PACKAGE</sub><br><br>
      Build a portable ZIP without opening or executing its contents.
    </td>
    <td width="33%" align="center">
      <strong>03</strong><br>
      <sub>STORE AND RESTORE</sub><br><br>
      Upload verified Discord parts and recover them when needed.
    </td>
  </tr>
</table>

<br>

<pre align="center">FILES  →  ZIP PACKAGE  →  DISCORD  →  VERIFIED DOWNLOAD</pre>

## Quick start

Create a Discord bot, add it to one server, and give it these permissions:

`View Channels` · `Manage Channels` · `Send Messages` · `Attach Files` · `Read Message History` · `Manage Messages`

### Raspberry Pi 4 or Linux

~~~bash
git clone https://github.com/YOUR-USERNAME/Discord-Cloud.git
cd Discord-Cloud
cp .env.example .env
openssl rand -hex 32

python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python app.py
~~~

Open `http://PI_ADDRESS:8080`. Run `hostname -I` on the Pi to find the address.

### Windows 11

~~~powershell
git clone https://github.com/YOUR-USERNAME/Discord-Cloud.git
cd Discord-Cloud
Copy-Item .env.example .env

py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
py app.py
~~~

Open `http://localhost:8080`.

## Configuration

Copy `.env.example` to `.env`, then fill in the four required values.

| Setting | What it is |
| :--- | :--- |
| `DISCORD_BOT_TOKEN` | Your Discord bot token |
| `DISCORD_GUILD_ID` | The server ID used for storage |
| `DISCORD_CLOUD_PASSWORD` | Dashboard password, 12+ characters |
| `DISCORD_CLOUD_SECRET` | Session secret, 32+ characters |

<details>
<summary><strong>Generate a session secret</strong></summary>

~~~bash
openssl rand -hex 32
~~~

</details>

## Raspberry Pi service

~~~bash
chmod +x install.sh
sudo ./install.sh

sudo systemctl status discord-cloud
sudo journalctl -u discord-cloud -f
~~~

## Security

> [!IMPORTANT]
> Keep `.env` and `state.json` private. They are ignored by Git and are not included in this repository.

- Discord is storage, not your only backup.
- Discord Cloud does not encrypt your files.
- Encrypt sensitive files before uploading them.
- Keep a second copy of important data.

## Verify

~~~bash
python app.py --self-test
~~~

## Project map

~~~text
app.py              Discord bot and web dashboard
install.sh          Raspberry Pi service installer
.env.example        Safe configuration template
assets/             Local artwork
~~~

## Preview

  <p align="center">
    <img src="Screenshot%202026-08-31%20135230.png" width="32%" alt="Screenshot 1">
    <img src="Screenshot%202026-08-31%20135245.png" width="32%" alt="Screenshot 2">
    <img src="Screenshot%202026-08-31%20135254.png" width="32%" alt="Screenshot 3">
  </p>

<p align="center">
  <sub>Released under the <a href="LICENSE">MIT License</a>.</sub>
</p>
