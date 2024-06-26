from functools import partial
import logging
from telebot import TeleBot
from queue_worker import  QueueWorker

from tg_bot_config import TG_TOKEN, TG_CHAT_ID_ERRORS, TG_TOKEN_MESSAGES, TG_CHAT_ID_MESSAGES


message_bot = TeleBot(TG_TOKEN_MESSAGES)
error_bot = TeleBot(TG_TOKEN)

tg_worker = QueueWorker()


def send_telegram_error(message: str):
    logging.error(message)
    if not tg_worker.is_started():
        tg_worker.start(raw_send_telegram_message)
    for chat_id in TG_CHAT_ID_ERRORS if isinstance(TG_CHAT_ID_ERRORS, list) else [TG_CHAT_ID_ERRORS]:
        tg_worker.put_message(error_bot, message, chat_id)


def send_telegram_message(message: str):
    if not tg_worker.is_started():
        tg_worker.start(raw_send_telegram_message)
    tg_worker.put_message(message_bot, message, TG_CHAT_ID_MESSAGES)


def raw_send_telegram_message(bot: TeleBot, message: str, dialog_id: str):
    bot.send_message(chat_id=dialog_id, text=message, parse_mode='Markdown')


start_telegram_worker = partial(tg_worker.start, raw_send_telegram_message)
