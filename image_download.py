import datetime
import time
import boto3
import urllib3
import matplotlib.pyplot as plt 
from mpl_toolkits.basemap import Basemap
from remap import remap
import numpy as np


def julian_to_date(julianday, year=2017, hour=0, minute=0):
    daynormal = datetime.datetime(year, 1, 1) + datetime.timedelta(days=julianday-1, hours=hour, minutes=minute)
    return daynormal.strftime('%Y-%m-%d-%H-%M-%S')


def create_filename(path, channel, year, julianday, hour, minute):
    return "{}/{}-G-{:02d}.nc".format(path, julian_to_date(julianday, year=year, hour=hour, minute=minute), channel)


def do_request_filename(date, s3_client, bucket='noaa-goes16'):
    day_str = date.strftime('%j')
    hour_str = date.strftime('%H')
    year_str = date.strftime('%Y')
    prefix = 'ABI-L2-CMIPF/' + year_str + '/' + day_str + '/' + hour_str
    ready = False
    while not ready:
        response = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
        ready = 'Contents' in list(response.keys())
        time.sleep(1)
    return response


def split_downloaded_filename(path):
    name = path.rsplit('/', 1)
    file_name = name[1]
    file_adress = name[0]
    channel = int(file_name[19:21])
    year = int(file_name[27:31])
    julianday = int(file_name[31:34])
    hour = int(file_name[34:36])
    minute = int(file_name[36:38])
    return file_adress, file_name, channel, year, julianday, hour, minute


def do_request_goes_image(file_address, file_name):
    http = urllib3.PoolManager()
    path = 'https://noaa-goes16.s3.amazonaws.com/' + file_address + '/' + file_name
    r = http.request('GET', path, preload_content=False)
    return r


def write_file(file_name, r):
    with open(file_name, 'wb') as out:
        while True:
            data = r.read(1000)
            if not data:
                break
            out.write(data)


def download_image(response, date=datetime.datetime.now(), channel_sel=10):
    for file in response['Contents']:
        file_adress, file_name, channel, year, julianday, hour, minute = split_downloaded_filename(file['Key'])
        if channel == channel_sel and int(date.strftime('%M')) == minute:
            r = do_request_goes_image(file_adress, file_name)
            new_file_name = create_filename('../Images', channel, year, julianday, hour, minute)
            write_file(new_file_name, r)
            r.release_conn()
            return new_file_name


def read_and_save_image(file_name):
    # Parameters
    extent = [-90.0, -70.0, -20, 10.0]
    resolution = 2.0

    # Call the reprojection funcion
    grid = remap(file_name, extent, resolution, 'HDF5')
    data = grid.ReadAsArray()
    min_data = np.min(data)
    max_data = np.max(data)

    # Plot the Data
    bmap = Basemap(llcrnrlon=extent[0], llcrnrlat=extent[1], urcrnrlon=extent[2], urcrnrlat=extent[3], epsg=4326)
    bmap.imshow(data, origin='upper', cmap='gist_ncar', vmin=min_data, vmax=max_data)

    # Save png
    DPI = 300
    png_file_name = file_name[:-2] + 'png'
    plt.savefig(png_file_name, dpi=DPI, bbox_inches='tight', pad_inches=0)
    return png_file_name


def download_loop(download_date, channel_sel, minutes_step):
    loop = True
    while loop:
        time_now = datetime.datetime.now()
        while time_now > download_date:
            s3_client = boto3.client('s3')
            response = do_request_filename(download_date, s3_client)
            nc_filename = download_image(response, download_date, channel_sel)
            png_filename = read_and_save_image(nc_filename)
            print(png_filename)
            download_date = download_date + datetime.timedelta(minutes=minutes_step)
            time.sleep(10)
        time.sleep(5)




