import asyncio
import logging
import sys
import json

import pymongo

from os import getenv
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.utils.markdown import hbold

from datetime import datetime, timezone

load_dotenv()

TOKEN = getenv("BOT_TOKEN")

dp = Dispatcher()

db = pymongo.MongoClient('localhost', 27017)['test_database']
sample_collection = db.sample_collection


ALLOWED_GROUPINGS = ['hour', 'day', 'week', 'month']


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    await message.answer(f"Hello, {hbold(message.from_user.full_name)}!")


@dp.message()
async def message_handler(message: types.Message) -> None:
    """
    """
    # Send a copy of the received message
    message_data = None

    try: 
        message_data = json.loads(message.text)
    except Exception as err:
        logging.error('Failed to read json from user input')
        await message.answer('No valid data provided')

    if not str(message_data['group_type']).lower() in ALLOWED_GROUPINGS:
        await message.answer('No valid grouping type provided')
        return
    
    goruping_type = None
    match message_data['group_type']:
        case 'month':
            goruping_type = {"date": {"$dateTrunc": {"date": "$dt", "unit": "month"}}}

        case 'week':
            goruping_type = {"date": {"$dateTrunc": {"date": "$dt", "unit": "week"}}}

        case 'day': 
            goruping_type = {"date": {"$dateTrunc": {"date": "$dt", "unit": "day"}}}

        case 'hour':
            goruping_type = {"date": {"$dateTrunc": {"date": "$dt", "unit": "hour"}}}
    
        case _:
            goruping_type = None


    aggregation = sample_collection.aggregate(
        [
            {"$match":
                {"dt": {
                    '$gte': datetime.strptime(message_data['dt_from'],  "%Y-%m-%dT%H:%M:%S"), 
                    '$lte':  datetime.strptime(message_data['dt_upto'],  "%Y-%m-%dT%H:%M:%S")
                    }
                }
            },
            {"$project": 
                {"date_grouping": goruping_type, "value": "$value"}
            },
            {"$group":
                {"_id": "$date_grouping", "total_value": {"$sum": "$value"}}
            },
            {"$sort":
                {"_id": 1}
            }
        ]
    )

    lables = []
    data = []

    for item in aggregation:
        lables.append(datetime.strftime(item['_id']['date'], "%Y-%m-%dT%H:%M:%S"))
        data.append(item['total_value'])

    await message.answer(f'{{"dataset": {data}\n"lables": {lables}}}')


async def main() -> None:
    # Initialize Bot instance with a default parse mode which will be passed to all API calls
    bot = Bot(TOKEN, parse_mode=ParseMode.HTML)
    # And the run events dispatching
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())