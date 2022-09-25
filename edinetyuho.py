class EdinetApi:
    '''EdineAPI利用 並列用にクラス化'''
    
    DOCMETA_URL = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents.json'
    DOC_URL = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents/'
    
    @lru_cache
    def get_ducument_meta(self, target_date: date)->pd.DataFrame:
        # 書類一覧APIのリクエストパラメータ
        params = {"date" : target_date.strftime('%Y-%m-%d'), "type" : 2}
        # 書類一覧APIの呼び出し
        try:
            res = requests.get(EdinetApi.DOCMETA_URL, params=params, verify=False)
            edinet_metadata = pd.DataFrame(res.json()['results'])
            edinet_metadata.columns = [col.upper() for col in edinet_metadata.columns]
        #RKPostgres().insert_dataframe('edinet.document_meta', edinet_metadata)
            return edinet_metadata
        except:
            pass

    
    @lru_cache
    def _get_doc(self, doc_id: str)->requests.models.Response:
        res_params = {'type': 1}
        return requests.get(end_point, params=res_params, stream=True)
    
    def get_doc(self, doc_id: str, output_dir_path: str)->None:
        end_point = EdinetApi.DOC_URL + doc_id
        res_params = {'type': 1}
        res = requests.get(end_point, params=res_params, stream=True)
        # 出力ファイル名
        filename = os.path.join(output_dir_path, f'''/{doc_id}.zip''')
        # ファイルへ出力
        if res.status_code == 200:
            with open(filename, 'wb') as f:
                for chunk in res.iter_content(chunk_size=1024):
                    f.write(chunk)
            zipfile.ZipFile(filename).extractall(os.path.join(output_dir_path, doc_id))


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
        _major_shareholder_df = pd.read_html(major_shareholder_text)[0].dropna(subset=[0])
        major_shareholder_df = _major_shareholder_df.iloc[1:-1] # 先頭行はカラム名最後の行は合計なので削除
        major_shareholder_df.columns = [col.replace('（', '(').replace('）', ')').replace('。', '').replace(' ', '') for col in _major_shareholder_df.iloc[0]]
        if not(set(['氏名又は名称', '住所', '所有株式数(千株)', '発行済株式(自己株式を除く)の総数に対する所有株式数の割合(％)']) == set(major_shareholder_df.columns)):
            raise ValueError(f'''Invalid Columns{major_shareholder_df.columns}''')
        return major_shareholder_df
    
    @property
    def treasury_share(self):
        root = self.eTree.getroot()
        treasury_share_text = root.xpath('//jpcrp_cor:TreasurySharesEtcTextBlock', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')})[0].text
        _treasury_share_df = pd.read_html(treasury_share_text)[0].dropna(subset=[0])
        treasury_share_df = _treasury_share_df.iloc[1:-1] # 先頭行はカラム名最後の行は合計なので削除
        treasury_share_df.columns = [col.replace('（', '(').replace('）', ')').replace('。', '').replace(' ', '') for col in _treasury_share_df.iloc[0]]
        if not(set(['所有者の氏名又は名称', '所有者の住所', '自己名義所有株式数(株)', '他人名義所有株式数(株)', '所有株式数の合計(株)', '発行済株式総数に対する所有株式数の割合(％)']) == set(treasury_share_df.columns)):
            raise ValueError(f'''Invalid Columns{major_shareholder_df.columns}''')
        return treasury_share_df
    
    @property
    def officer(self):
        root = self.eTree.getroot()
        officer_text = root.xpath('//jpcrp_cor:InformationAboutOfficersTextBlock', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')})[0].text
        _officer_df = pd.read_html(officer_text)[0]
        officer_df = _officer_df.iloc[1:]
        officer_df.columns = [col.replace('（', '(').replace('）', ')').replace('。', '').replace(' ', '') for col in _officer_df.iloc[0]]
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
