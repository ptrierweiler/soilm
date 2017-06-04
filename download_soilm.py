#!/usr/bin/python3
import os
import bs4
import requests
import sys
import datetime
import wget
import psycopg2

# page link
# find the most recent month
page_url = "https://www.esrl.noaa.gov/psd/data/gridded/data.cpcsoil.html"
soilm_link = "ftp://ftp.cdc.noaa.gov/Datasets/cpcsoil/soilw.mon.mean.v2.nc"
data_path = '/data/soilm'

page_res = requests.get(page_url)

# checking for page validity
if page_res.status_code != requests.codes.ok:
    print("Page link is invalid")
    exit()

soup = bs4.BeautifulSoup(page_res.text, 'html5lib')

date_str_raw = ''
for i in soup.select('li'):
    text = i.text
    if 'Monthly Means:' in text:
        date_str_raw = text.strip()

mon = int(date_str_raw[-5:-3])
year = int(date_str_raw[-10:-6])

soil_date_cur = datetime.date(year, mon, 1)

# put data base check to download new file

os.chdir(data_path)
if os.path.isfile('soilw.mon.mean.v2.nc') is True:
    os.remove('soilw.mon.mean.v2.nc')

wget.download(soilm_link)


if os.path.isfile('band_text.txt') is True:
    os.remove('band_text.txt')

os.system("gdalinfo -nomd soilw.mon.mean.v2.nc | grep Band > band_text.txt")

# load band_text.txt
band_file = open("/data/soilm/band_text.txt", "r")
band_data = band_file.readlines()
band_file.close()

band_list = []
for i in band_data:
    band_list.append(int(i.strip('\n').split()[1]))

max_band = max(band_list)

date_band_dict = {}
date_band_dict.update({soil_date_cur: max_band})
soil_date = soil_date_cur
for i in range(max_band, 1, -1):
    bt = i -1
    yt = (soil_date - datetime.timedelta(days=1)).year
    mt = (soil_date - datetime.timedelta(days=1)).month
    soil_date = datetime.date(yt, mt, 1)
    if soil_date >= datetime.date(1980,1,1):
        date_band_dict.update({soil_date: bt})

# find dates in the db
try:
    conn = psycopg2.connect("dbname='tlaloc'")
except:
    print("I am unable to connect to the database")
    exit()
cur = conn.cursor()

slct_qry = "select tablename from pg_catalog.pg_tables " + \
           "where schemaname = 'soilm' and tablename <> 'data'"

cur.execute(slct_qry)
layers_db_q = cur.fetchall()

db_date_list = []
for i in layers_db_q:
    db_date_list.append(datetime.datetime.strptime(i[0].strip('soilm_'),
                                                   '%Y_%m_%d').date())

# removing images in the db from date_band_dict
for i in db_date_list:
    del date_band_dict[i]

os.chdir(data_path)
for date, b in date_band_dict.items():
    date_str = datetime.datetime.strftime(date, '%Y_%m_%d')
    print("Making soilm_{}.tif using band: {}".format(date_str, b))
    tif_file = "soilm_{}.tif".format(date_str)
    proj = "soilm_{}_proj.tif".format(date_str)
    east = "soilm_{}_east.tif".format(date_str)
    west = "soilm_{}_west.tif".format(date_str)
    fix = "soilm_{}_fix.tif".format(date_str)
    layer = "soilm_{}".format(date_str)
    os.system('gdal_translate -b {} '.format(b) +
              'NETCDF:"soilw.mon.mean.v2.nc" soilm_{}.tif'.format(date_str))

    print("Creating {}".format(proj))
    os.system('gdalwarp -t_srs "WGS84" {} {}'.format(tif_file, proj))

    print("Creating {}".format(west))
    os.system("gdal_translate -srcwin 360 0 360 360 " +
              "-a_ullr -180 90 0 -90 {} {}".format(proj, west))

    print("Creating {}".format(east))
    os.system("gdal_translate -srcwin 0 0 360 360 " +
              "-a_ullr 0 90 180 -90 {} {}".format(proj, east))

    print("Creating {}".format(fix))
    os.system("gdal_merge.py -n -9.96920996838686905e+36 -a_nodata 0 " +
              "-o {} {} {}".format(fix, east, west))

    os.remove(tif_file)
    os.remove(proj)
    os.remove(east)
    os.remove(west)

    cur.execute("select count(*) from pg_catalog.pg_tables " +
			    "where tablename = '{}' and schemaname = 'soilm'".format(layer))

    layer_chk = cur.fetchall()[0][0]

    if layer_chk == 0:
        os.system("raster2pgsql -C -I -N -999 {} ".format(fix) +
			      "-d soilm.{} | psql tlaloc".format(layer))
    os.remove(fix)

os.system("/scripts/soilm/summerize_soil.py")
