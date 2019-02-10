#!/usr/bin/env python3
'''.'''
import requests
import sqlite3
import string
import decimal
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


def base_stat_lookup(equipment):
    if 'main' in equipment.lower():
        return 'firepower'
    if 'anti-air' in equipment.lower():
        return 'aa'
    if 'torpedoes' in equipment.lower():
        return 'torpedo'
    return 'air_power'


def get_rarity(ship_data, index):
    if str(ship_data[index][1][1]).lower() != 'nan':
        if len(ship_data[index][1][1]) == 4:
            return 'common'
        if len(ship_data[index][1][1]) == 5:
            return 'rare/elite'
        if len(ship_data[index][1][1]) == 6:
            return 'super rare'
    return ''


def import_ships(content, db, table):
    ship_list = pandas.read_html(content)
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
    for tmp_table in [name for name in db.get_tables() if name != table]:
        db.cursor.execute('DROP TABLE {}'.format(tmp_table))
    return


def get_equip(ship_data, index, record):
    for row, slot in enumerate(list(ship_data[index][0][2:])):
        row_index = row + 2
        row_id = 'slot_{}'.format(slot)
        mod_string = ship_data[index][1][row_index]
        modifier = mod_string.replace('%', '').split('/')[-1]
        if not validate_modifier(str(modifier)):
            modifier = '0'
        elif '' == modifier:
            modifier = '0'
        mod_key = row_id + '_modifier'
        equip_key = row_id + '_equipment'
        effect_key = row_id + '_effective'
        record[mod_key] = float(modifier)/100
        record[equip_key] = ship_data[index][2][row_index]
        slot_base_stat = base_stat_lookup(record[equip_key])
        eff_value = record[slot_base_stat] * record[mod_key]
        record[effect_key] = eff_value
    return record


def main():
    table = 'master'
    db = SQLOps(filename=':memory:', row_factory=True)
    db.create_table(table, NEW_HEADERS, NEW_HEADER_TYPES, ordinal=True)
    ship_list_html = requests.get(SHIPS_BY_STATS_URL)
    if ship_list_html.status_code == 200:
        import_ships(content=ship_list_html.content, db=db, table=table)
    types = set([row['type'] for row in
                 db.select_rows(col_name='"type"',
                                table_name=table).fetchall()])
    for ship_type in types:
        db.create_table(ship_type.lower(), NEW_HEADERS,
                        NEW_HEADER_TYPES, ordinal=True)
    for row in db.select_rows(table_name=table).fetchall():
        record = dict(zip(db.get_column_names(table), row))
        name = record['ship_name'].replace(' ', '_')
        ship_url = 'https://azurlane.koumakan.jp/{}'.format(name)
        ship_html = requests.get(ship_url)
        if ship_html.status_code == 200:
            ship_data = pandas.read_html(ship_html.content)
            print('Fetched data for ship: {}'.format(record['ship_name']))
            for index, data in enumerate(ship_data):
                if 'Rarity' in str(ship_data[index]):
                    record['rarity'] = get_rarity(ship_data, index)
                if 'Nationality' in str(ship_data[index]):
                    record['nation'] = ship_data[index][1][1]
                if 'Equipment' in str(ship_data[index][0][0]):
                    record = get_equip(ship_data, index, record)
            db.add_row(row_data=record, table_name=table)
            db.add_row(row_data=record, table_name=record['type'].lower())
    db.export_csv(table_name=table, filename='data.csv')
    for ship_type in types:
        db.export_csv(table_name=ship_type.lower(),
                      filename='{}.csv'.format(ship_type.lower()))
    return


if __name__ == '__main__':
    main()
