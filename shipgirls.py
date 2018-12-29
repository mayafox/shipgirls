#!/usr/bin/env python3
'''.'''
import requests
import sqlite3
import string
import pandas
from sql_ops import SQLOps

SHIPS_BY_STATS_URL = 'https://azurlane.koumakan.jp/List_of_Ships_by_Stats'
NEW_HEADERS = ['id', 'ship_name', 'rarity', 'nation', 'type',
               'health', 'firepower', 'aa', 'torpedo', 'evasion', 'air_power',
               'fuel_consumption', 'reload', 'armor', 'speed', 'asw',
               'oxygen', 'ammo']
NEW_HEADER_TYPES = ['text', 'text', 'text', 'text', 'text', 'real', 'real',
                    'real', 'real', 'real', 'real', 'real', 'real', 'text',
                    'real', 'real', 'real', 'real']
TABLE_MAP = {}
TABLE_MAP['dd'] = {'table': 'dd', 'index': 1}
TABLE_MAP['cl'] = {'table': 'cl', 'index': 4}
TABLE_MAP['ca'] = {'table': 'ca', 'index': 7}
TABLE_MAP['bb'] = {'table': 'bb', 'index': 10}
TABLE_MAP['bc'] = {'table': 'bc', 'index': 13}
TABLE_MAP['bm'] = {'table': 'bm', 'index': 16}
TABLE_MAP['cv'] = {'table': 'cv', 'index': 19}
TABLE_MAP['cvl'] = {'table': 'cvl', 'index': 22}
TABLE_MAP['ar'] = {'table': 'ar', 'index': 25}
TABLE_MAP['colab'] = {'table': 'colab', 'index': 28}
TABLE_MAP['retro'] = {'table': 'retro', 'index': 30}
NUMERICS = string.digits + '.'


def validate_modifier(mod_string):
    ret_value = True
    for char in mod_string:
        if char not in NUMERICS:
            ret_value = False
    return ret_value


def main():
    ship_list_html = requests.get(SHIPS_BY_STATS_URL).content
    ship_list = pandas.read_html(ship_list_html)
    db = SQLOps(filename=':memory:', row_factory=True)
    table = 'master'
    db.create_table(table, NEW_HEADERS, NEW_HEADER_TYPES, ordinal=True)
    for key in TABLE_MAP.keys():
        ship_list[TABLE_MAP[key]['index']].to_sql(TABLE_MAP[key]['table'],
                                                  db.database)
        db.cursor.execute('DELETE from "{}"'.format(TABLE_MAP[key]['table'])
                          + ' where "index" is 0')
        for row in db.select_rows(
              table_name=TABLE_MAP[key]['table']).fetchall():
            clean_row = list(row)[1:]
            clean_row[4] = ''.join([x for x in list(clean_row[4])[:5]
                                    if x in string.ascii_uppercase][:-1])
            clean_row[-4] = float(clean_row[-4])
            row_data = {}
            for index, key in enumerate(NEW_HEADERS):
                row_data[key] = clean_row[index]
            db.add_row(row_data=row_data, table_name=table)
    for row in db.select_rows(table_name=table).fetchall():
        record = dict(zip(db.get_column_names(table), row))
        name = record['ship_name'].replace(' ', '_')
        ship_url = 'https://azurlane.koumakan.jp/{}'.format(name)
        ship_html = requests.get(ship_url).content
        ship_data = pandas.read_html(ship_html)
        print('fetched data for ship: {}'.format(record['ship_name']))
        for index, data in enumerate(ship_data):
            if 'Rarity' in str(ship_data[index]):
                if len(ship_data[index][1][1]) == 4:
                    record['rarity'] = 'common'
                if len(ship_data[index][1][1]) == 5:
                    record['rarity'] = 'rare/elite'
                if len(ship_data[index][1][1]) == 6:
                    record['rarity'] = 'super rare'
            if 'Nationality' in str(ship_data[index]):
                record['nation'] = ship_data[index][1][1]
            if 'Equipment' in str(ship_data[index][0][0]):
                for row, slot in enumerate(list(ship_data[index][0][2:])):
                    row_index = row + 2
                    row_id = 'slot_{}'.format(slot)
                    modifier_string = ship_data[index][1][row_index]
                    modifier = modifier_string.replace('%', '').split('/')[-1]
                    if not validate_modifier(str(modifier)):
                        modifier = '0'
                    elif '' == modifier:
                        modifier = '0'
                    record[row_id + '_modifier'] = float(modifier)/100
                    record[row_id + '_equipment'] = ship_data[index][2][
                        row_index]
        db.add_row(row_data=record, table_name=table)
    db.export_csv(table_name=table, filename='data.csv')


if __name__ == '__main__':
    main()
