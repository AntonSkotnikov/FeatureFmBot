import grequests as gr
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton
import time

from auth import auth, BOT_TOKEN
from cc import CC_ALPHA_2


RESOLVER_URL = "https://console-api.feature.fm/smartlink-resolver"
MESSAGE_MAX_LENGTH = 4096

bot = telebot.TeleBot(BOT_TOKEN)
kb = ReplyKeyboardMarkup(resize_keyboard=True)
kb.row("/parse", "/json")

jar = auth()  # Get cookie jar
 

def log(*args):
    print(time.strftime("[%x %X]"), *args)
 

def split_isrc(isrc):
    
    if len(isrc) != 12:
        raise ValueError("Invalid ISRC length.")

    country_code = isrc[:2]        # countrycode
    registrant_code = isrc[2:5]    # alphanum
    year_of_reference = isrc[5:7]  # num
    designation_code = isrc[7:12]  # num

    if (registrant_code.isalnum() and year_of_reference.isdigit() and 
        designation_code.isdigit()):  # and country_code in CC_ALPHA_2):
        
        prefix = country_code + registrant_code + year_of_reference

        return prefix, int(designation_code)

    raise ValueError("Invalid ISRC format")


def exception_handler(request, exception):
    isrc = request.kwargs["params"]['q'].strip("isrc:")
    log(f"{exception} while serving {isrc}")
    
    for attempt in range(3):
        resp = gr.map([request])[0]
        if resp is not None:
            print("Exception resolved. Attempts:", attempt+1)
            return resp
    
    print("After 1+3 tries gave up on", isrc)
    return isrc


@bot.message_handler(commands=['start'])
def start(message):
    string = (
            "This bot looks up info for given ISRCs\n"
            "in an open database at feature.fm\n"
            "\n"
            "Feel free to call /parse to start\n"
            "(more at /help)"
    )
    bot.send_message(message.chat.id, string, reply_markup=kb)


@bot.message_handler(commands=['help', 'info', 'h'])
def help(message):
    string = (
        "/<help | info | h> - display this message\n"
        "/parse - scrape ISRC range and parse info\n"
        "/json - scrape ISRC range and return jsons"
    )
    bot.send_message(message.chat.id, string)


@bot.message_handler(commands=['parse', 'json'])
def greet(message):
    query = dict()
    query["raw"] = message.text.startswith("/json")  # TODO: refactor

    bot.send_message(message.chat.id, f"Привет! Введи начальный ISRC")
    bot.register_next_step_handler(message, input_isrc, query)


def input_isrc(message, query):
    try:
        isrc = split_isrc(message.text.upper())
        query["isrc"] = isrc

        bot.send_message(message.chat.id, "Введи количество итераций")
        bot.register_next_step_handler(message, input_iter_amount, query)

    except ValueError:
        bot.send_message(message.chat.id, "Неверный формат ISRC. Введи его еще раз")
        bot.register_next_step_handler(message, input_isrc, query)


def input_iter_amount(message, query):
    try:
        isrc = query["isrc"]
        iter_amount = int(message.text)  # total amount of ISRCs to parse
        log(query, iter_amount)

        if iter_amount > 0:
            iter_amount = min(iter_amount, 10 ** 5 - isrc[1])  # Avoid overflowing designation code (max=10**5)
            query["iter_amount"] = iter_amount
            scrape(message.chat.id, query)

        else:
            bot.send_message(message.chat.id, "Попробуй ещё раз. Введи положительное число")
            bot.register_next_step_handler(message, input_iter_amount, query)

    except ValueError:
        bot.send_message(message.chat.id, "Попробуй ещё раз. Введи число в десятичной системе счисления")
        bot.register_next_step_handler(message, input_iter_amount, query)


def scrape(chat_id, query):
    prefix, code = query["isrc"]
    iter_amount = query["iter_amount"]
    raw = query["raw"]

    bot.send_message(chat_id, f"Парсинг идет с {prefix}{code :05} по {prefix}{code+iter_amount-1 :05}")

    with gr.Session() as s:
        s.cookies = jar  # Global jar
        retries = Retry(total=3, backoff_factor=0.05)
        s.mount("https://", HTTPAdapter(max_retries=retries))

        miss_amount = 0  # amount of "Not found" replies
        iter_step = 100  # amount of requests to send at once

        for step_start in range(0, iter_amount, iter_step):
            step_end = min(step_start + iter_step, iter_amount)
            reqs = []

            for cur_offset in range(step_start, step_end):
                req = gr.get(
                    RESOLVER_URL,
                    params={
                        "q": f"isrc:{prefix}{code+cur_offset :05}",
                        "op": "skipscrape"
                    },
                    session=s
                )

                reqs.append(req)

            resps = gr.map(reqs, exception_handler=exception_handler)

            #fails = list(map(lambda x: isinstance(x, str), resps))
            #print(f"{time.strftime('[%X]')} From {len(resps)} resps {fails.count(True)} were failed.")
            log(f"Got {step_end} / {iter_amount} resps.")
            
            step_miss = send_messages(chat_id, resps, raw)  # Send messages and return amount of missed reqs
            miss_amount += step_miss
        
        if miss_amount:
            if miss_amount != iter_amount:
                template = "По {} другим ISRC треков не найдено."
            else:
                template = "По всем {} ISRC треков не найдено."

            bot.send_message(chat_id, template.format(miss_amount))


class MessageBuf:
    def __init__(self, chat_id):
        self.messages = list()
        self.total_length = 0
        self.chat_id = chat_id


    def flush(self):
        if self.total_length == 0:
            return

        bot.send_message(self.chat_id, "\n\n".join(self.messages), disable_notification=True)
        self.messages.clear()
        self.total_length = 0


    def append(self, message):
        message_length = len(message)

        if message_length <= MESSAGE_MAX_LENGTH:
            if self.total_length + message_length > MESSAGE_MAX_LENGTH:
                self.flush()

            self.messages.append(message)
            self.total_length += message_length + 2  # Compensate for two \n's when flushing
        
        # Message is bigger than MESSAGE_MAX_LENGTH
        else:
            self.flush()

            # Split message into chuncks of MESSAGE_MAX_LENGTH size
            for start in range(0, message_length, MESSAGE_MAX_LENGTH):
                self.append(message[start:min(start + MESSAGE_MAX_LENGTH, message_length)])
            self.flush()


def send_messages(chat_id, resps, raw=False) -> int:
    step_miss = 0
    buf = MessageBuf(chat_id)

    for resp in resps:
        if isinstance(resp, str):
            # resp is swapped to isrc of failed reqv by excpetion handler
            buf.flush()
            bot.send_message(chat_id, f"По {resp} не получено ответа после 4 попыток.")
            continue
        
        json = resp.json()

        if "name" in json:
            #isrc = resp.url.split("3A")[-1]  # split by ":" and take ISRC that goes after it
            step_miss += 1
            #bot.send_message(message.chat.id, f"По {isrc} получен ответ, но записи не существует.")
            continue  # No track found with such ISRC, skipping it
        
        # Query asked for raw json
        if raw:
            message = str(json)
            buf.append(message)
        
        # Parsing json here
        else:
            data = {
                "isrc": None,
                "artists": None,
                "title": None,
                "imageUrl": None,
                "duration": None,
            }
            empty_fields = 5

            platforms = list(json.keys())
            if "anghami" in platforms:
                platforms.remove("anghami")
                platforms.insert(-1, "anghami")
            if "boomplay" in platforms:
                platforms.remove("boomplay")
                platforms.insert(0, "boomplay")

            for platform in platforms:
                pl = json[platform]

                for key in data.keys():
                    if data[key] is None and key in pl:
                        data[key] = pl[key]
                        empty_fields -= 1

                        if empty_fields == 0:
                            break
                
                if empty_fields == 0:
                    break

            message = (
                f"ISRC: {data['isrc']},\n"
                f"artist(s): {', '.join(data['artists']) if data['artists'] is not None else None},\n"
                f"title: {data['title']},\n"
                f"cover: {data['imageUrl']},\n"
                f"duration: {time.strftime('%M:%S', time.gmtime(data['duration']))}."
            )
            buf.append(message)
    
    buf.flush()
    return step_miss


if __name__ == "__main__":
    bot.infinity_polling()
