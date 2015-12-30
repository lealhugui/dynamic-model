# -*- coding: utf-8 -*-

import sqlite3
from collections import OrderedDict


INSERT_PROD = """INSERT INTO produto
    (descricao, preco) VALUES (?, ?)
"""
CREATE_PROD = """CREATE TABLE produto
(id INTEGER PRIMARY KEY AUTOINCREMENT, descricao VARCHAR(120), preco NUMERIC)
"""

class Conn(object):

    _INNER_CONN = None

    def __init__(self, db_path=None):
        in_memory = db_path is None
        self._INNER_CONN = self.get_connection(
            ":memory:" if in_memory else db_path
        )
        if self._INNER_CONN is not None:
            self._INNER_CONN.row_factory = sqlite3.Row

            if in_memory:
                try:
                    self.execute("DROP TABLE produto")
                except:
                    pass
                self.execute(CREATE_PROD)


    def get_connection(self, db):
        return sqlite3.connect(db)

    def _close_inner_conn(self):
        if self._INNER_CONN is not None:
            self._INNER_CONN.close()

    def _get_cursor(self):
        if self._INNER_CONN is None:
            raise Exception("Closed Conn")

        return self._INNER_CONN.cursor()

    def execute(self, sql, parameters=[]):
        if self._INNER_CONN is None:
            raise Exception("Closed Conn")

        try:
            self._get_cursor().execute(sql, parameters)
            self._INNER_CONN.commit()
            self._close_inner_conn()
        except Exception as e:
            self._INNER_CONN.rollback()
            self._close_inner_conn()
            raise

    def query(self, sql, parameters=[]):
        if self._INNER_CONN is None:
            raise Exception("Closed Conn")

        try:
            return [_row_as_dict(row) for row in
                    self._get_cursor().execute(sql, parameters)]
        except Exception as e:
            raise

def _row_as_dict(row):
    result = dict()

    for k in row.keys():
        result[k] = row[k]

    return result


if __name__ == '__main__':
    """TESTE"""
    import random

    # Crio a instancia com o banco em memoria, o que cria uma tabela ficticia
    # chamada "produto"
    c = Conn()
    for i in range(10):
        # insiro alguns dados
        print(i)
        t = random.randrange(0, 100)
        c.execute(INSERT_PROD, ("teste{}".format(t), t))

    p = c.query("select * from produto")

    print(p)
