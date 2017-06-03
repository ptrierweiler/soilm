#!/usr/bin/python3
import psycopg2
import datetime

schema = 'soilm'
table = 'data'

layer = 'nass_asds'

try:
    conn = psycopg2.connect("dbname='tlaloc'")
except:
    print("I am unable to connect to the database")
    exit()
cur = conn.cursor()
conn.autocommit = True

def summerize(layer):
# Find dates in db
    print("Finding dates in {}.{} for {}".format(schema, table, layer))
    cur.execute("select distinct date from " +
                "{}.{} where geolayer = '{}'".format(schema, table, layer))
    db_list = cur.fetchall()
    db_list_cln = []
    for i in db_list:
        db_list_cln.append(i[0])

    # image list
    cur.execute("select tablename from pg_catalog.pg_tables where " +
                 "schemaname = '{}' and tablename <> '{}'".format(schema, table))
    image_list_db = cur.fetchall()

    date_list = []
    for i in image_list_db:
        n = i[0]
        date_str = n.replace('soilm_','')
        date = datetime.datetime.strptime(date_str, '%Y_%m_%d').date()
        date_list.append(date)

    date_diff_list = list(set(date_list) - set(db_list_cln))

    print("Summerizing " + str(len(date_diff_list)) + " images")

    for i in date_diff_list:
        print("processing " + layer + " date: " + str(i))
        image = schema + ".soilm_{}_01_01".format(i.year)
        geo = "wgs84" '.' + layer
        cur.execute('SELECT gid, (stats).count,(stats).mean::numeric(7,3), '+
	                'median::numeric(7,3) FROM (SELECT gid, ' +
                    'ST_SummaryStats(ST_Clip(rast, {geo}.wkb_geometry)::raster) as stats, '.format(geo=geo) +
	                'ST_Quantile(ST_Clip(rast,{geo}.wkb_geometry)::raster,.5) as median '.format(geo=geo) +
	                'from {image}, {geo} where '.format(image=image, geo=geo) +
                    'st_intersects(rast, {geo}.wkb_geometry)) as foo'.format(geo=geo))
        sum_data = cur.fetchall()
        for row in sum_data:
            gid = row[0]
            cell_cnt = row[1]
            mean = row[2]
            median = row[3]
            date = i
            cur.execute("insert into {}.{} values".format(schema, table) +
                        "(%s, %s, %s, %s, %s, %s)", (gid, cell_cnt, mean,
                                                          median, date, layer))

summerize("nass_asds")
summerize("brasil_mesoregion")
summerize("nass_asds")
