import pandas as pd
import requests
from urllib.request import urlopen
import zipfile
from tqdm import tqdm
from lxml import etree
from bs4 import BeautifulSoup
from datetime import date
import urllib3
import time
from bs4 import BeautifulSoup
import glob
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)
from functools import lru_cache

from common.database.rk_postgres import RKPostgres

pd.set_option('display.max.columns', 30)

_dates = pd.date_range(date(2022,6,1), date(2022,9,25)).date
doc_ids = []
for _date in tqdm(_dates):
    edinet_metadata = EdinetApi().get_ducument_meta(_date)
    if (len(edinet_metadata)!=0) and (edinet_metadata is not None):
        RKPostgres().insert_dataframe('edinet.document_meta', edinet_metadata)
    time.sleep(0.5)
    
edinet_metadata = RKPostgres().fetch_query("SELECT * FROM edinet.document_meta")
doc_ids = edinet_metadata['DOCID']
for docid in tqdm(doc_ids):
    try:
        EdinetApi().get_doc(docid)
    except:
        print(docid)
