# pycord-ipc
A pycord extension for inter-process communication.

# Installation 
Python >= 3.10.x is required.
```bash=
# Windows
pip install --upgrade pycord-inter-process

# Linux
pip3 install --upgrade pycord-inter-process
```

# Examples
### Server example  
```py= 
import discord
from pycord.ipc import Server

print("Bot starting...")

intents = discord.Intents.all()
bot = discord.Bot(intents=intents)
ipc_server = Server(bot, secret_key=<"your IPCSecret">, host=<"your IPCHost">, port=63719)

@bot.event
async def on_ready():
    await ipc_server.start()

# @bot.event
# async def on_ipc_ready():
#     """Called upon the IPC Server being ready"""
#     print(f"Starting IPC server")

@bot.event
async def on_ipc_error(endpoint, error):
    print(f"{endpoint} raised {error}")

@ipc_server.route()
async def get_bot_stats(data):
    return {
        "guild_count": len(bot.guilds),
        "channel_count": sum(len(guild.channels) for guild in bot.guilds),
        "member_count": sum(len(guild.members) for guild in bot.guilds),
    }

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

bot.run(<"your Token">)
```

### Client example
```py= 
from quart import Quart, request, session, redirect, url_for, jsonify
from pycord.ipc import Client

app = Quart(__name__)
ipc_client = Client(secret_key=<"your IPCSecret">, host=<"your IPC Server IP">, port=<"your IPC Server Port">)

@app.route("/api/bot/stats", methods=["GET"])
async def get_bot_stats():
    try:
        bot_stats = await ipc_client.request("get_bot_stats")
        return jsonify({
            "success": True,
            "data": {
                "guild_count": bot_stats["guild_count"],
                "channel_count": bot_stats["channel_count"],
                "member_count": bot_stats["member_count"]
            }
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
```