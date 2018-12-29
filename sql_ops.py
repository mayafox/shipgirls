#!/usr/bin/env python3
"""SQLOps"""

import sqlite3
import csv
import time


class SQLOps(object):
    """SQLOps is a collection of common functiions for working with sqlite3
       databases.

    Parameters
    ----------
    filename : string
        Name of the file to use for the database. Default is :memory:
        If using db_rotation, a unique filename with a timestamp will be
        generated based off this filename. A extension of .db will be appended
        automatically.
    db_rotation_time : int
        Interval between database rotations to prevent overload.
        Default is 0. Values are 0 for no rotation, or
        86400 (1 day) to 1209600 (2 weeks) seconds. No rotation is supported
        if using :memory:.

    Attributes
    ----------
    database : sqlite3 connect object
        Description of attribute `database`.
    cursor : sqlite3 cursor object
        Description of attribute `cursor`.

    """

    def __init__(self, filename=':memory:', db_rotation_time=0,
                 row_factory=False):
        self.base_filename = filename
        if (db_rotation_time not in range(86400, 1209600)
                and db_rotation_time != 0):
            raise ValueError('db_rotation_time is not a valid value.')
        else:
            self.rotation_interval = db_rotation_time
        if self.base_filename != ':memory:' and self.rotation_interval != 0:
            self.start_time = int(time.time())
            filename = self.base_filename + '_{}.db'.format(self.start_time)
        else:
            if (self.base_filename != ':memory:'
                    and self.rotation_interval != 0):
                raise ValueError('db_rotation_time must be 0 if using '
                                 + ':memory:.')
            if self.base_filename != ':memory:':
                filename = self.base_filename + '.db'
            else:
                filename = self.base_filename
        self.database = sqlite3.connect(filename)
        if row_factory:
            self.database.row_factory = sqlite3.Row
        self.cursor = self.database.cursor()
        self.__valid_types = ['text', 'integer', 'numeric', 'real', 'none']
        self.__ordinals = {}
        return

    def check_types(self, types):
        """Check that the types are valid for sqlite3.

        Parameters
        ----------
        types : list, string
            SQL field types to be validated.

        Returns
        -------
        boolean
            True - all field types are valid
            False - one or more field types are invalid
        """
        state = True
        if isinstance(types, str):
            types = types.split()
        for item in types:
            if item not in self.__valid_types:
                state = False
        return state

    def get_tables(self):
        """Get table names from the database.
        Returns
        -------
        list
            List of the table names in the database.

        """
        return [x[0] for x in self.cursor.execute('SELECT name FROM '
                                                  + 'sqlite_master WHERE type '
                                                  + '= "table" ORDER BY name;'
                                                  ).fetchall()]

    def table_exists(self, table_name="table"):
        """Check if the specified table is present in the database.

        Parameters
        ----------
        table_name : string
            The table name to look for in the database.

        Returns
        -------
        boolean
            True - table is present in the database.
            False - table is not present in the database.
        """
        return table_name in self.get_tables()

    def drop_table(self, table_name="table"):
        """Drops the specified table from the database.

        Parameters
        ----------
        table_name : string
            The table to be dropped.

        Returns
        -------
        None

        """
        if self.table_exists():
            self.cursor.execute('DROP TABLE IF EXISTS '
                                + '"{}";'.format(table_name))
        return

    @staticmethod
    def __sql_pragma_table(table_name='table'):
        return 'PRAGMA table_info("{}");'.format(table_name)

    def get_column_names(self, table_name="table"):
        """Returns the column names for the specified table

        Parameters
        ----------
        table_name : string
            Table to get the column names for.

        Returns
        -------
        list
            List containing the column names as ordered in the table.

        """
        return [x[1] for x in self.cursor.execute(
            self.__sql_pragma_table(table_name)).fetchall()]

    def get_column_types(self, table_name="table"):
        """Returns the sql data types for the columns.

        Parameters
        ----------
        table_name : string
            The table to get the column types for.

        Returns
        -------
        list
            List of the sql data types as ordered in the table.

        """
        return [x[2] for x in self.cursor.execute('PRAGMA table_info'
                                                  + '("{}")'.format(table_name)
                                                  + ';').fetchall()]

    def column_is_pk(self, column_name, table_name="table"):
        return [x[5] for x in self.cursor.execute(
            self.__sql_pragma_table(table_name)).fetchall()
                if x[1] == column_name]

    def get_pk_name(self, table_name="table"):
        value = [x for x in self.get_column_names(table_name)
                 if self.column_is_pk(x, table_name)]
        if value:
            return value[0]
        return []

    def column_exists(self, column_name, table_name="table"):
        """Returns if the column is present in the table.

        Parameters
        ----------
        column_name : string
            Column to verify presence in the specified table.
        table_name : string
            Table to reference for the column.

        Returns
        -------
        boolean
            True  -  Column exists in the table.
            False -  Column does not exist in the table.

        """
        return column_name in self.get_column_names(table_name)

    def add_column(self, name, sql_type, key=False, table_name='table'):
        """Add a single column to a table.

        Parameters
        ----------
        name : string
            Name of the column to add. Must be unique.
        sql_type : string
            SQL data type for the column.
            Valid types are: 'text', 'integer', 'numeric', 'real', 'none'
        key : boolean
            Column will be the primary key for the table.
        table_name : string
            table to add the column too.

        Returns
        -------
        None

        """
        if not self.check_types(sql_type):
            raise ValueError('ERROR: type is not a valid sql data type.')
        if self.column_exists(name, table_name):
            raise Exception('ERROR: Column already exists in the table.')
        sql_call = 'ALTER TABLE "{}" '.format(table_name)
        sql_call = sql_call + 'ADD COLUMN \'{}\' \'{}\''.format(name, sql_type)
        if key:
            sql_call = sql_call + ' PRIMARY KEY'
        sql_call = sql_call + ';'
        if not self.column_exists(name, table_name):
            self.cursor.execute(sql_call)
        return

    def create_table(self, table_name, col_list, type_list, drop=False,
                     key_name='', ordinal=False):
        """Create a table with the specified columns.

        Parameters
        ----------
        table_name : string
            Name of the table to create
        col_list : list
            List of the column names as strings.
        type_list : list
            List of the column SQL data types as strings.
            Valid types are: 'text', 'integer', 'numeric', 'real', 'none'
        drop : boolean
            Drop the table if it exists prior to creating it.
        key_name : string
            name of the column that will be the primary key.
        ordinal : boolean
            Use a column named 'ordinal' as the numeric primary key.

        Returns
        -------
        None

        """
        sql_call = ''
        if not self.check_types(type_list):
            print('ERROR: One or more of the SQL data types are invalid.')
        if not drop and self.table_exists(table_name):
            raise Exception('ERROR: Table is already present.')
        if drop:
            self.drop_table(table_name)
        if self.table_exists(table_name):
            for (col, sql_type) in zip(col_list, type_list):
                if not self.column_exists(col, table_name):
                    key = col == key_name
                    self.add_column(col, sql_type, key, table_name)
        else:
            sql_call = 'CREATE TABLE {}'.format(table_name) + '('
            if ordinal:
                key_name = 'ordinal'
                if 'ordinal' not in col_list:
                    new_col_list = ['ordinal'] + col_list
                    new_type_list = ['integer'] + type_list
                else:
                    new_col_list = col_list
                    new_type_list = type_list
                self.__ordinals[table_name] = 1
            for (col, sql_type) in zip(new_col_list, new_type_list):
                sql_call = sql_call + "'{}' {}".format(col, sql_type)
                if col == key_name:
                    sql_call = sql_call + ' PRIMARY KEY, '
                else:
                    if col == col_list[-1]:
                        sql_call = sql_call + ');'
                    else:
                        sql_call = sql_call + ', '
        self.cursor.execute(sql_call)
        return

    def select_rows(self, col_name='*', sql_filter='',
                    ordering='', table_name='table'):
        """Executes an SQL select statement against the table.

        Parameters
        ----------
        col_name : string
            Column to select as the return value in the query.
            Default is '*', or  all columns
        sql_filter : string
            SQL query language to use for limiting the selection.
            Default is '' (none)
        table_name : string
            Name of the table to operate on.

        Returns
        -------
        sqlite3 cursor object
            returns an sql cursor execute object
        Examples
        -------
        to get all rows in the table 'dogs':
            mydb.select_rows(table_name='dogs').fetchall()
        to get a specific dog's name from the table 'dogs'
           mydb.select_rows('name', sql_filter='name is "clarus"',
                            table_name='dogs').fetchall()
        to setup a row_factory to iterate over the rows in a selection:
            mydb.select_rows(table_name='dogs')
            record = mydb.cursor.fetchone()
            print(record.keys())
            while record:
                print([list(record)])
                record = mydb.cursor.fetchone()
        """
        sql_call = 'SELECT {} FROM {}'.format(col_name, table_name)
        if sql_filter:
            sql_call = sql_call + ' WHERE {}'.format(sql_filter)
        if ordering:
            sql_call = sql_call + ' ORDER BY {}'.format(ordering)
        sql_call = sql_call + ';'
        return self.cursor.execute(sql_call)

    def get_next_ordinal(self, table_name='table'):
        """Gets the next unique ordinal value for ordinal based tables.

        Parameters
        ----------
        table_name : str
            Name of the table to get the ordinal for.

        Returns
        -------
        int
            The next ordinal value for the table.

        """
        if self.column_exists('ordinal', table_name):
            current_id = self.select_rows(col_name='ordinal',
                                          table_name=table_name).fetchall()
            if not current_id:
                return self.__ordinals[table_name]
            return current_id[-1][0] + 1
        else:
            raise Exception('no ordinal column in table'.format(table_name))

    def __sql_gen_tuple(self, table_name):
        sql_call = 'INSERT OR REPLACE INTO {}'.format(table_name) + '('
        sql_call = sql_call + ', '.join(self.get_column_names(table_name))
        sql_call = sql_call + ') VALUES ('
        dummy = ['?'] * len(self.get_column_names(table_name))
        sql_call = sql_call + ', '.join(dummy) + ');'
        return sql_call

    def __sql_gen_dict(self, table_name):
        sql_call = 'INSERT OR REPLACE INTO {}'.format(table_name) + "('"
        sql_call = sql_call + "', '".join(self.get_column_names(table_name))
        sql_call = sql_call + "') VALUES ("
        for key in self.get_column_names(table_name):
            if '.' in key:
                new_key = key.replace('.', '_')
                self.row[new_key] = self.row[key]
                sql_call = sql_call + ':{}'.format(new_key)
            else:
                sql_call = sql_call + ':{}'.format(key)
            if key == self.get_column_names(table_name)[-1]:
                    sql_call = sql_call + ');'
            else:
                sql_call = sql_call + ', '
        return str(sql_call)

    def __sql_gen_list(self, data, table_name):
        sql_call = 'INSERT OR REPLACE INTO {} VALUES '.format(table_name) + '('
        table_types = self.get_column_types(table_name)
        for index, value in enumerate(data):
            if table_types[index] in ['integer', 'numeric',
                                      'real', 'none']:
                sql_call = sql_call + '{}'.format(value)
            else:
                sql_call = sql_call + '"{}"'.format(value)
            if value == data[-1]:
                sql_call = sql_call + ');'
            else:
                sql_call = sql_call + ', '
        return sql_call

    def __validate_row_data_dict(self, data, table_name):
        return [x for x in data.keys()
                if x not in self.get_column_names(table_name)]

    def add_row(self, row_data, table_name='table'):
        """inserts or replaces the row data into the table.

        Parameters
        ----------
        row_data : list of values,
                   list of tuples,
                   dict
            Row data to add to the table.
        table_name : str
            Name of the table to insert or replace the row into.

        Returns
        -------
        None

        """
        self.rotate()
        many_flag = False
        if self.column_exists('ordinal', table_name):
            if isinstance(row_data, list):
                if isinstance(row_data[0], tuple):
                    many_flag = True
                    sql_call = self.__sql_gen_tuple(table_name)
                else:
                    # some additional validation may need to be done here
                    # as list may have to many values, or not enough.
                    # it may come down to only accepting lists with tuples,
                    # and dicts.
                    if len(row_data) != len(self.get_column_names(
                            table_name)):
                        row_data.insert(0, self.get_next_ordinal(table_name))
                    sql_call = self.__sql_gen_list(row_data, table_name)
            elif isinstance(row_data, dict):
                self.row = {}
                if 'ordinal' not in row_data.keys():
                    row_data['ordinal'] = self.get_next_ordinal(table_name)
                if self.__validate_row_data_dict(row_data, table_name):
                    for column in self.__validate_row_data_dict(row_data,
                                                                table_name):
                        sql_type = 'text'
                        if isinstance(row_data[column], (int, bool)):
                            sql_type = 'integer'
                        elif isinstance(row_data[column], float):
                            sql_type = 'real'
                        elif isinstance(row_data[column], str):
                            sql_type = 'text'
                        else:
                            sql_type = 'NULL'
                        self.add_column(column, sql_type,
                                        table_name=table_name)
                for key in [x for x in self.get_column_names(table_name)
                            if x not in row_data.keys()]:
                    row_data[key] = ''
                self.row = row_data
                sql_call = self.__sql_gen_dict(table_name)

            else:
                raise TypeError('row data is an unsupported type')
        else:
            if isinstance(row_data, list):
                if isinstance(row_data[0], tuple):
                    many_flag = True
                    sql_call = self.__sql_gen_tuple(table_name)
                else:
                    sql_call = self.__sql_gen_list(row_data, table_name)
            elif isinstance(row_data, dict):
                self.row = {}
                if self.__validate_row_data_dict(row_data, table_name):
                    for column in self.__validate_row_data_dict(row_data,
                                                                table_name):
                        if isinstance(row_data[column], (int, bool)):
                            sql_type = 'integer'
                        elif isinstance(row_data[column], float):
                            sql_type = 'real'
                        elif isinstance(row_data[column], str):
                            sql_type = 'text'
                        else:
                            sql_type = 'NULL'
                        self.add_column(column, sql_type,
                                        table_name=table_name)
                self.row = row_data
                sql_call = self.__sql_gen_dict(table_name)
                print(sql_call)
            else:
                raise TypeError('row data is an unsupported type')
        if isinstance(row_data, list):
            if many_flag:
                self.cursor.executemany(sql_call, row_data)
            else:
                self.cursor.execute(sql_call)
        if isinstance(row_data, dict):
            try:
                self.cursor.execute(sql_call, self.row)
            except Exception as err:
                print(err)
        return

    def export_csv(self, filename, sql_filter='', table_name='table'):
        """Exports the table to the specified file as a csv.

        Parameters
        ----------
        filename : string
            filename and path of the desired csv file.
        sql_filter : string
            SQL query language to use for limiting the selection.
            Default is '' (none)
        table_name : string
            Name of the table to export.

        Returns
        -------
        None

        """
        self.database.row_factory = sqlite3.Row
        if filter:
            self.select_rows(sql_filter=sql_filter, table_name=table_name)
        else:
            self.select_rows(table_name=table_name)
        record = self.cursor.fetchone()
        with open(filename, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=',')
            writer.writerow(record.keys())
            while record:
                writer.writerow(list(record))
                record = self.cursor.fetchone()
        return

    def check_time(self):
        return int(time.time()) >= self.start_time + self.rotation_interval

    def rotate(self):
        if self.rotation_interval:
            if self.check_time():
                new_time = self.start_time + self.rotation_interval
                filename = self.base_filename + '_{}.db'.format(new_time)
                tmp_database = SQLOps(filename)
                for table in self.get_tables():
                    tmp_database.create_table(table,
                                              self.get_column_names(table),
                                              self.get_column_types(table),
                                              key_name=self.get_pk_name(table))
                    self.__ordinals[table] = self.get_next_ordinal(table)

                tmp_database.database.commit()
                tmp_database.database.close()
                self.database.commit()
                self.database.close()
                self.start_time = new_time
                self.database = sqlite3.connect(filename)
                self.cursor = self.database.cursor()
        pass


def main():
    """example calls using a database in RAM.

    Returns
    -------
    None

    """
    table = 'dogs'
    my_data = [(1, 'dog1', 'St. Bernard', 'True'),
               (2, 'dog2', 'Rottweiler', 'true')]
    values = {'ordinal': 3, 'name': 'dog3',
              'breed': 'St Bernard', 'good_dog': 'True'}
    clarus = SQLOps()
    clarus.get_tables()
    clarus.create_table(table, ['name', 'breed', 'good_dog'],
                        ['text', 'text', 'text'], ordinal=True)
    clarus.get_tables()
    clarus.get_column_names(table)
    clarus.get_column_types(table)
    clarus.column_is_pk('breed', table)
    clarus.column_is_pk('ordinal', table)
    clarus.add_row(my_data, table)
    clarus.select_rows(table_name=table)
    clarus.column_exists('ordinal', table_name=table)
    clarus.get_next_ordinal(table)
    clarus.add_row(values, table)
    clarus.export_csv('test.csv', table_name=table)
    return


if __name__ == "__main__":
    main()
