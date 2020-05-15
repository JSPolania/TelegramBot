import logging
import os
from pytz import timezone

from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
from telegram import ParseMode
from datetime import time, datetime
from voomerBot.relocator import kicks_requirements, kicks_locations, kicks_relocation
from voomerBot.vehiclesDumper import get_vehicles
from voomerBot.vehicle_kicker import check_kicks
from voomerBot.utils import distribution, distribution_points, zoho
import pandas as pd
from time import sleep

network_retries = int(os.environ.get('NETWORK_RETRIES', '5'))
network_sleep = int(os.environ.get('NETWORK_SLEEP', '2'))
timeout_connect = float(os.environ.get('TIMEOUT_CONNECT', '9.15'))
timeout_read = float(os.environ.get('TIMEOUT_READ', '60'))
CHAT_ID = int(os.environ.get('CHAT_ID'))
CITY = os.environ.get('CITY')

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

def start(update, context):
    context.bot.send_message(chat_id=update.message.chat_id, text="hola, soy un robot!", parse_mode=ParseMode.HTML)

def getid(update, context):
    context.bot.send_message(chat_id=update.message.chat_id, text=update.message.chat_id)

def error(bot, update, error):
    logger.warning('Update "%s" caused error "%s"', update, error)

def check(context):
    hora = datetime.now(timezone('America/Bogota')).strftime("%H:%M:%S")
    context.bot.send_message(chat_id=CHAT_ID, text="Patinetas a revisar a las {}".format(hora))
    dist = distribution(CITY) 
    points = distribution_points(dist['points'])
    kicks_zoho = zoho()
    kicks = get_vehicles(CITY)
    kicks_loc = kicks_locations(kicks, points, dist['map'], kicks_zoho)
    checked_kicks = check_kicks(kicks_loc)

    context.bot.send_message(chat_id=CHAT_ID, text=checked_kicks, timeout=15, parse_mode=ParseMode.HTML)

def user_check(update, context):
    check(context)

def programmed_check(context):
    check(context)

if __name__ == "__main__":
    # Set these variable to the appropriate values
    TOKEN = os.environ.get('TELEGRAM_TOKEN')
    NAME = os.environ.get('HEROKU_APP_NAME')

    # Port is given by Heroku
    PORT = os.environ.get('PORT')

    # Enable logging
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Set up the Updater
    request_dict = {}
    request_dict['read_timeout'] = 7
    request_dict['connect_timeout'] = 6
    #request_kwargs=request_dict
    updater = Updater(TOKEN, use_context=True)
    dispatcher = updater.dispatcher
    
    # Add handlers
    dispatcher.add_handler(CommandHandler('start', start))
    dispatcher.add_handler(CommandHandler('getid', getid))
    dispatcher.add_handler(CommandHandler('check', user_check))
    dispatcher.add_error_handler(error)

    j = updater.job_queue
    for h in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]:
        for m in [0,30]:
            j.run_daily(programmed_check, time(h,m,0), (0, 1, 2, 3, 4, 5, 6))

    #updater.start_polling()
    updater.start_webhook(listen="0.0.0.0", port=int(PORT), url_path=TOKEN)
    updater.bot.setWebhook("https://{}.herokuapp.com/{}".format(NAME, TOKEN))
    #updater.idle()


    """
def reloc(context): 
    hora = datetime.now(timezone('America/Bogota')).strftime("%H:%M:%S")
    print(CHAT_ID)
    context.bot.send_message(chat_id=CHAT_ID, text="Patinetas por punto a las {}".format(hora))

    dist = distribution(CITY) 
    points = distribution_points(dist['points'])
    kicks_zoho = zoho()
    kicks = get_vehicles(CITY)
    kicks_loc = kicks_locations(kicks, points, dist['map'], kicks_zoho)
    relocation_msg = kicks_relocation(kicks_loc, dist['points'])

    for msg in relocation_msg:
        context.bot.send_message(chat_id=CHAT_ID, text=msg, timeout=15, parse_mode=ParseMode.HTML)
        sleep(10)

def user_reloc(update, context):
    reloc(context)

def programmed_reloc(context):
    reloc(context)

"""