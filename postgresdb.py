from typing import List
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import extras
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from functools import lru_cache

from common.io.logger import logger


def nan_to_null(
        f,
        _NULL=psycopg2.extensions.AsIs('NULL'),
        _NaN=np.NaN,
        _Float=psycopg2.extensions.Float):
    if not np.isnan(f):
        return _Float(f)
    return _NULL
psycopg2.extensions.register_adapter(float, nan_to_null)


class RKPostgres:
    """
    Postgresへの接続ポイント
    Postgresから取得したカラム名の小文字→大文字変換はここで行う
    """

    DB_CONTAINER_NAME = os.environ.get('POSTGRES_CONTAINER_NAME')

    ## データベースのアクセス先: 'postgresql://{PostGresuserName}:{PostGresPassword}@{ContainerName}/{DBName}'
    DB_PLACE = \
        f'''postgresql://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASSWORD')}@{DB_CONTAINER_NAME}/{os.environ.get('POSTGRES_DB')}'''

    def __init__(self):
        pass

    @staticmethod
    def get_engine_sqlalchemy():
        logger.info(RKPostgres.DB_PLACE)
        return create_engine(RKPostgres.DB_PLACE)

    @staticmethod
    def get_connection_psycopg2():
        return psycopg2.connect(RKPostgres.DB_PLACE)

    def fetch_query(self, query: str) -> pd.DataFrame:
        """
        データ取得のクエリを実行
        :param query:
        :return:
        """
        with RKPostgres.get_connection_psycopg2() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query)
                colnames = [col.name for col in cur.description]
                df = pd.DataFrame(cur.fetchall(), columns=colnames)
                df.columns = df.columns.str.upper()
        return df

    def indel_query(self, query: str):
        """
        クエリの実行
        :param query:
        :return:
        """
        with RKPostgres.get_connection_psycopg2() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(query)
                    conn.commit()
                except Exception as e:
                    logger.info(e)

    def insert_dataframe(self, tb_name: str, df: pd.DataFrame, if_exists: str='ignore'):
        """
        tb_nameにdataframeをインサート
        dataframeのカラムとdatabaseのカラムはカラム名が一致している場所にインサート
        :param tb_name:
        :param df:
        :param if_exists
        :return:
        """
        if if_exists == 'warning':
            on_conflict = ''
        elif if_exists == 'ignore':
            on_conflict = 'ON CONFLICT DO NOTHING'
        #elif if_exists == 'update':
        #    on_conflict = 'ON CONFLICT DO UPDATE'
        else:
            raise(ValueError('if_exists should be in (warning, ignore, #update)'))
        cols = self.get_col_order(tb_name)
        df.columns = df.columns.str.upper()
        query = f'''INSERT INTO {tb_name} VALUES %s {on_conflict};'''
        values = [tuple(row) for row in df[cols].mask(pd.isnull(df), None).values.tolist()]
        with RKPostgres.get_connection_psycopg2() as conn:
            with conn.cursor() as cur:
                try:
                    extras.execute_values(cur=cur, sql=query, argslist=values)
                    conn.commit()
                except Exception as e:
                    print(e)

    def get_col_order(self, tb_name: str) -> List[str]:
        """

        :param tb_name: テーブルのカラムインデックスを取得
        :return:
        """
        return self.fetch_query(f'''SELECT * FROM {tb_name} LIMIT 1 OFFSET 0''').columns.tolist()


if __name__ == '__main__':

    db = RKPostgres()

    # TABLE作成
    db.indel_query('CREATE TABLE person(id int primary key , name varchar(20), age int)')
    db.indel_query('''INSERT INTO person(id, name, age) VALUES (1, 'Kawmaori', 24)''')
    db.indel_query('''INSERT INTO person(id, name, age) VALUES (2, 'Yamashita', 25)''')

    # TABLEフェッチ
    db = RKPostgres()
    df = db.fetch_query('SELECT * FROM person')
    logger.info(df)

    # INSERT
    db.indel_query('''INSERT INTO person(id, name, age) VALUES (3, 'Harada', 22)''')
    df = db.fetch_query('SELECT * FROM person')
    logger.info(df)
    db.indel_query('''DELETE FROM person WHERE name = 'Harada' ''')
    df = db.fetch_query('SELECT * FROM person')
    logger.info(df)

    # データフレーム挿入
    db.insert_dataframe(tb_name='person', df=pd.DataFrame({'ID': [3, 4], 'NAME': ['Harada', 'Kamimura'], 'AGE': [21, 17]}),
                        if_exists='ignore')
    df = db.fetch_query('SELECT * FROM person')
    print(df)

    # 衝突判定
    db.insert_dataframe(tb_name='person', df=pd.DataFrame({'id': [3], 'name': ['Kawamori'], 'age': [24]}),
                        if_exists='ignore')
    df = db.fetch_query('SELECT * FROM person')
    print(df)

    # 行デリート
    db.indel_query('''DELETE FROM person WHERE name IN ('Harada', 'Kamimura') ''')

    # テーブルデリート
    db.indel_query('''DROP TABLE person''')

