import requests as r
from datetime import datetime
from os import getenv


BOT_TOKEN = getenv("BOT_TOKEN")
__auth_data = {
    "username": getenv("USERNAME"),
    "password": getenv("PASSWORD")
 }

AUTH_URL = "https://console-api.feature.fm/login"


def auth():
        # TODO: don't auth so often
        res = r.post(AUTH_URL, json=__auth_data)  # Send json to auth
        
        if not res.ok:
            raise Exception("Failed to authenticate. Check credentials")
        
        print("Successfully authenticated")

        cookie_jar = res.cookies  # Get CookieJar
        #cookie_jar.pop("spotlightMeButtonCaptionMode")  # Remove unnecessary cookie
        
        #print(", ".join(list(map(lambda x: f"{x[0]}: {x[1]}", cookie_jar.items()))))

        return cookie_jar


if __name__ == "__main__":
    jar = auth()
    cur_time = datetime.today()

    for c in jar:
        exp = datetime.fromtimestamp(c.expires)
        delta = exp - cur_time

        print(f"name: {c.name}, expires: {exp}, delta: {delta}")

