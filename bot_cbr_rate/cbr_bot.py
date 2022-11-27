# импорт библиотек
from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.filters import Text
# import telebot
import asyncio
import psycopg2
import pandas as pd
from config_postgresql import host, user, password, port
from token_bar import token_cbr_bot

# переменные для бота
bot = Bot(token=token_cbr_bot)
dp = Dispatcher(bot)

# присоединение к базе и запрос данных
connection = psycopg2.connect(
    host=host,
    database='cbr_rate',
    user=user,
    password=password,
    port=port)

rate_7days = pd.read_sql("""
     select *
     from cbr_rate_table
     where (select max(date) from cbr_rate_table)-date < 7
    """, connection).rename (columns = {'curr_rate':'Курс', 'date':'Дата'})

# стартовое приветствие и панель кнопок
@dp.message_handler(commands="start")
async def start(message: types.Message):
    # кнопки
    sb1 = types.KeyboardButton("Курс валюты сегодня")
    sb2 = types.KeyboardButton('Курс валюты за 7 дней')
    # Воспроизведение разметки клавиатуры
    start_keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    start_keyboard.add(sb1).add(sb2)
    # приветственное сообщение
    await bot.send_message(message.from_user.id,
                           'Доброго дня и приятного курса!', reply_markup=start_keyboard)

# Панель кнопок для курса сегодня
@dp.message_handler(Text(equals="Курс валюты сегодня"))
async def choose_currency(message: types.Message):
    # кнопки
    but_1 = types.InlineKeyboardButton(text="USD",
                                       callback_data="USD_today")
    but_2 = types.InlineKeyboardButton(text="EUR",
                                       callback_data="EUR_today")
    but_3 = types.InlineKeyboardButton(text="GBP",
                                       callback_data="GBP_today")
    but_4 = types.InlineKeyboardButton(text="Китайский юань",
                                       callback_data="CNY_today")
    but_5 = types.InlineKeyboardButton(text="Турецкая лира",
                                       callback_data="TRY_today")
    but_6 = types.InlineKeyboardButton(text="Остальные",
                                       callback_data="other_today")
    but_7 = types.InlineKeyboardButton(text="Швейцарский франк",
                                       callback_data="CHF_today")
    # Воспроизведение разметки клавиатуры
    key = types.InlineKeyboardMarkup()
    key.row(but_1, but_2).row(but_3, but_4).row(but_5, but_6).add(but_7)
    # сообщение инструкция
    await bot.send_message(message.chat.id, "Выбери вид валюты", reply_markup=key)

# Панель кнопок для курса за 7 дней
@dp.message_handler(Text(equals="Курс валюты за 7 дней"))
async def choose_currency(message: types.Message):
    # кнопки
    but_1 = types.InlineKeyboardButton(text="USD",
                                       callback_data="USD_7day")
    but_2 = types.InlineKeyboardButton(text="EUR",
                                       callback_data="EUR_7day")
    but_3 = types.InlineKeyboardButton(text="GBP",
                                       callback_data="GBP_7day")
    but_4 = types.InlineKeyboardButton(text="Китайский юань",
                                       callback_data="CNY_7day")
    but_5 = types.InlineKeyboardButton(text="Турецкая лира",
                                       callback_data="TRY_7day")
    but_6 = types.InlineKeyboardButton(text="Остальные",
                                       callback_data="other_7day")
    but_7 = types.InlineKeyboardButton(text="Швейцарский франк",
                                       callback_data="CHF_7day")
    # Воспроизведение разметки клавиатуры
    key = types.InlineKeyboardMarkup()
    key.row(but_1, but_2).row(but_3, but_4).row(but_5, but_6).add(but_7)
    await bot.send_message(message.chat.id, "Выбери вид валюты", reply_markup=key)

# ответы на запрос сегодняшнего курса
@dp.callback_query_handler(lambda c:True)
async def inlin(c):
    if c.data == 'USD_today':
        rate_usd = rate_7days.query('literal_id == "USD" and Дата == Дата.max()'
                                    ).reset_index().loc[0, 'Курс']
        await bot.send_message(c.message.chat.id, rate_usd)
    if c.data == 'EUR_today':
        rate_eur = rate_7days.query('literal_id == "EUR" and Дата == Дата.max()'
                                    ).reset_index().loc[0, 'Курс']
        await bot.send_message(c.message.chat.id, rate_eur)
    if c.data == 'GBP_today':
        rate_gbp = rate_7days.query('literal_id == "GBP" and Дата == Дата.max()'
                                    ).reset_index().loc[0, 'Курс']
        await bot.send_message(c.message.chat.id, rate_gbp)
    if c.data == 'CNY_today':
        rate_cny = rate_7days.query('literal_id == "CNY" and Дата == Дата.max()'
                                    ).reset_index().loc[0, 'Курс']
        await bot.send_message(c.message.chat.id, rate_cny)
    if c.data == 'TRY_today':
        rate_try = rate_7days.query('literal_id == "TRY" and Дата == Дата.max()'
                                    ).reset_index().loc[0, 'Курс']
        await bot.send_message(c.message.chat.id, rate_try)
    if c.data == 'CHF_today':
        rate_cnf = rate_7days.query('literal_id == "CHF" and Дата == Дата.max()'
                                    ).reset_index().loc[0, 'Курс']
        await bot.send_message(c.message.chat.id, rate_cnf)
    if c.data == 'other_today':
        rate_other = (
            rate_7days.query('literal_id not in ("USD", "EUR", "GBP", "CHF", "CNY", "TRY") and Дата == Дата.max()')
            .reset_index().loc[:, 'curr_name':'Курс'].set_index('Курс'))
        rate_other.columns = [''] * len(rate_other.columns)
        await bot.send_message(c.message.chat.id, rate_other)

    # ответы на запрос  курса за семь дней
    if c.data == 'USD_7day':
        rate_usd = rate_7days.query('literal_id == "USD"').reset_index().loc[
                                                        :, ['Дата', 'Курс']].set_index('Дата')
        rate_usd.columns = [''] * len(rate_usd.columns)
        await bot.send_message(c.message.chat.id, rate_usd)
    if c.data == 'EUR_7day':
        rate_eur = rate_7days.query('literal_id == "EUR"').reset_index().loc[
                                                         :, ['Дата', 'Курс']].set_index('Дата')
        rate_eur.columns = [''] * len(rate_eur.columns)
        await bot.send_message(c.message.chat.id, rate_eur)
    if c.data == 'GBP_7day':
        rate_gbp = rate_7days.query('literal_id == "GBP"').reset_index().loc[
                                                          :, ['Дата', 'Курс']].set_index('Дата')
        rate_gbp.columns = [''] * len(rate_gbp.columns)
        await bot.send_message(c.message.chat.id, rate_gbp)
    if c.data == 'CNY_7day':
        rate_cny = rate_7days.query('literal_id == "CNY"').reset_index().loc[
                                                           :, ['Дата', 'Курс']].set_index('Дата')
        rate_cny.columns = [''] * len(rate_cny.columns)
        await bot.send_message(c.message.chat.id, rate_cny)
    if c.data == 'TRY_7day':
        rate_try = rate_7days.query('literal_id == "TRY"').reset_index().loc[
                                                            :, ['Дата', 'Курс']].set_index('Дата')
        rate_try.columns = [''] * len(rate_try.columns)
        await bot.send_message(c.message.chat.id, rate_try)
    if c.data == 'CHF_7day':
        rate_chf = rate_7days.query('literal_id == "CHF"').reset_index().loc[
                                                             :, ['Дата', 'Курс']].set_index('Дата')
        rate_chf.columns = [''] * len(rate_chf.columns)
        await bot.send_message(c.message.chat.id, rate_chf)
    if c.data == 'other_7day':
        other = rate_7days.query('literal_id not in ("USD", "EUR", "GBP", "CHF", "CNY", "TRY")'
                                 ).reset_index().loc[:, ['Курс', 'Дата',  'curr_name']].set_index('Курс')
        for i in other.curr_name.unique():
            rate_other = other.query('curr_name == @i')
            rate_other.columns = [''] * len(rate_other.columns)
            await bot.send_message(c.message.chat.id, rate_other)



# запускаем лонг поллинг
if __name__ == '__main__':
    executor.start_polling(dp)