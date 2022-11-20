import pandas as pd
import os
import requests
from enum import Enum
import zipfile
from tqdm import tqdm
from lxml import etree
from bs4 import BeautifulSoup
from datetime import date
import numpy as np
import urllib3
import glob
import re
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)
from pathlib import Path
from functools import lru_cache
import unicodedata

from common.database.rk_postgres import RKPostgres
from common.io.logger import logger

pd.set_option('display.max.columns', 100)
pd.set_option('display.max.row', 200)


START_DATE = date(2021, 6, 30)
END_DATE = date(2022, 6, 30)


docmeta = (
    RKPostgres().fetch_query(f'''
    SELECT *
    FROM edinet.document_meta
    WHERE PERIODEND > '{START_DATE.strftime('%Y-%m-%d')}'
    AND PERIODEND <= '{END_DATE.strftime('%Y-%m-%d')}'
    AND DOCTYPECODE = '120'
    ''')
    .assign(NORM_FILERNAME = lambda df: normalize_company_name(df['FILERNAME']))
)

major_share = (
    RKPostgres().fetch_query(f'''
    SELECT DOCID, SECCODE, RANK, NAME, SHARE, RATIO, PERIODSTART, PERIODEND, FILERNAME
    FROM edinet.major_shareholder
    INNER JOIN edinet.document_meta
    USING(DOCID)
    WHERE PERIODEND > '{START_DATE.strftime('%Y-%m-%d')}'
    AND PERIODEND <= '{END_DATE.strftime('%Y-%m-%d')}'
    ''')
)

company_name = (
    RKPostgres().fetch_query(f'''
    SELECT SECCODE, FILERNAME
    FROM edinet.document_meta
    ''')
    .assign(NORM_FILERNAME = lambda df: normalize_company_name(df['FILERNAME']))
    .drop_duplicates(subset=['SECCODE', 'NORM_FILERNAME'])
)

officer_share = (
RKPostgres().fetch_query(f'''
    SELECT DOCID, FILERNAME, SECCODE, NAME, SHARE
    FROM edinet.officer
    INNER JOIN edinet.document_meta
    USING(DOCID)
    WHERE PERIODEND > '{START_DATE.strftime('%Y-%m-%d')}'
    AND PERIODEND <= '{END_DATE.strftime('%Y-%m-%d')}'
    ''')
    .assign(NAME = lambda df: normalize_company_name(df['NAME']))
    .assign(FILERNAME = lambda df: normalize_company_name(df['FILERNAME']))
)

treasury_share = (
RKPostgres().fetch_query(f'''
    SELECT DOCID, SECCODE, FILERNAME, NAME, SHARE
    FROM edinet.treasury_share
    INNER JOIN edinet.document_meta
    USING(DOCID)
    WHERE PERIODEND > '{START_DATE.strftime('%Y-%m-%d')}'
    AND PERIODEND <= '{END_DATE.strftime('%Y-%m-%d')}'
    ''')
    .assign(FILERNAME = lambda df: normalize_company_name(df['FILERNAME']))
    .assign(NAME = lambda df: normalize_company_name(df['NAME']))
)

stocksetc = (
RKPostgres().fetch_query(f'''
    SELECT DOCID, FILERNAME, SECCODE, COMPANY_NAME, SHARE, REASON
    FROM edinet.stocksetc
    INNER JOIN edinet.document_meta
    USING(DOCID)
    WHERE PERIODEND > '{START_DATE.strftime('%Y-%m-%d')}'
    AND PERIODEND <= '{END_DATE.strftime('%Y-%m-%d')}'
    ''')
    .assign(FILERNAME = lambda df: normalize_company_name(df['FILERNAME']))
    .assign(INV_COMPANY_NAME = lambda df: normalize_company_name(df['COMPANY_NAME']))
)

issued_share = (
RKPostgres().fetch_query(f'''
    SELECT DOCID, SECCODE, ISSUED_SHARE
    FROM edinet.issued_share
    INNER JOIN edinet.document_meta
    USING(DOCID)
    WHERE PERIODEND > '{START_DATE.strftime('%Y-%m-%d')}'
    AND PERIODEND <= '{END_DATE.strftime('%Y-%m-%d')}'
''')
)

document_meta = (
    RKPostgres().fetch_query(f'''
    SELECT *
    FROM edinet.document_meta
    ''')
)
