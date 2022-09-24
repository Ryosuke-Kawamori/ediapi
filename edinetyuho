import pandas as pd
import os
import requests
from urllib.request import urlopen
import glob
import zipfile
from tqdm import tqdm
from lxml import etree
from bs4 import BeautifulSoup
from datetime import date
import urllib3

class EdinetYuho:
    
    def __init__(self, dir_path: Path):
        self.dir_path = dir_path
    
    @property
    def xbrl_path(self)->str:
        filepath = os.path.join(self.dir_path, 'XBRL/PublicDoc/')
        files = glob.glob(filepath+'*.xbrl') #htmファイルの取得
        target_file = files[0]
        return target_file

    @property
    def eTree(self):
        return etree.parse(self.xbrl_path)
    
    @property
    def major_shareholder(self):
        root = self.eTree.getroot()
        major_shareholder_text = root.xpath('//jpcrp_cor:MajorShareholdersTextBlock', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')})[0].text
        _major_shareholder_df = pd.read_html(major_shareholder_text)[0]
        major_shareholder_df = _major_shareholder_df.iloc[1:-1] # 先頭行はカラム名最後の行は合計なので削除
        major_shareholder_df.columns = _major_shareholder_df.iloc[0]
        if not(set(['氏名又は名称', '住所', '所有株式数(千株)', '発行済株式(自己株式を除く)の総数に対する所有株式数の割合(％)']) == set(major_shareholder_df.columns)):
            raise ValueError(f'''Invalid Columns{major_shareholder_df.columns}''')
        return major_shareholder_df
    
    @property
    def treasury_share(self):
        root = self.eTree.getroot()
        treasury_share_text = root.xpath('//jpcrp_cor:TreasurySharesEtcTextBlock', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')})[0].text
        _treasury_share_df = pd.read_html(treasury_share_text)[0]
        treasury_share_df = _treasury_share_df.iloc[1:-1] # 先頭行はカラム名最後の行は合計なので削除
        treasury_share_df.columns = _treasury_share_df.iloc[0]
        if not(set(['所有者の氏名又は名称', '所有者の住所', '自己名義所有株式数（株）', '他人名義所有株式数（株）', '所有株式数の合計（株）', '発行済株式総数に対する所有株式数の割合(％)']) == set(treasury_share_df.columns)):
            raise ValueError(f'''Invalid Columns{major_shareholder_df.columns}''')
        return treasury_share_df
    
    @property
    def officer(self):
        root = self.eTree.getroot()
        officer_text = root.xpath('//jpcrp_cor:InformationAboutOfficersTextBlock', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')})[0].text
        _officer_df = pd.read_html(officer_text)[0]
        officer_df = _officer_df.iloc[1:]
        officer_df.columns = _officer_df.iloc[0]
        if not(set(['役職名', '氏名', '生年月日', '略歴', '任期', '所有株式数(千株)']) == set(officer_df.columns)):
            raise ValueError(f'''Invalid Columns{officer_df.columns}''')
        return officer_df
    
    @property
    def issued_share(self):
        root = self.eTree.getroot()
        issued_share_text = root.xpath('//jpcrp_cor:IssuedSharesTotalNumberOfSharesEtcTextBlock', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')})[0].text
        _issued_share_df = pd.read_html(issued_share_text)[0]
        issued_share_df = _issued_share_df.iloc[1:]
        issued_share_df.columns = _issued_share_df.iloc[0]
        return issued_share_df
           
    @property
    def stocksetc(self):
        root = self.eTree.getroot()
        stocksetc_text = root.xpath('jplvh_cor:DetailsOfAcquisitionsAndDisposalsOfStocksEtcIssuedByIssuerOfSaidStocksEtcDuringLast60DaysTextBlock', namespaces={'jplvh_cor': root.nsmap.get('jplvh_cor')})[0].text
        _stocksetc_df = pd.read_html(stocksetc_text)[0]
        stocksetc_df = _stocksetc_df.iloc[1:]
        stocksetc_df.columns = _stocksetc_df.iloc[0]
        if not(set(['年月日', '株券等の種類', '数量', '割合', '市場内外取引の別', '取得又は処分の別', '単価']) == set(stocksetc_df.columns)):
            raise ValueError(f'''Invalid Columns{stocksetc_df.columns}''')
        company_name = root.xpath('jplvh_cor:NameOfIssuer', namespaces={'jplvh_cor': root.nsmap.get('jplvh_cor')})[0].text
        seccode = root.xpath('jplvh_cor:SecurityCodeOfIssuer', namespaces={'jplvh_cor': root.nsmap.get('jplvh_cor')})[0].text
        return stocksetc_df.assign(SECCODE = seccode, COMPANY_NAME = company_name)
