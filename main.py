from image_download import download_loop
import datetime


channel_sel = 10

start_date = datetime.datetime(2017, 11, 21, 5, 0, 0)
minutes_step = 60

#start_date = datetime.datetime.now()
#start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
#minutes_step = 15

download_loop(start_date, channel_sel, minutes_step)