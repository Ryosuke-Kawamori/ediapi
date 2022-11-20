import pandas as pd
import os
import requests
from enum import Enum
import zipfile
from lxml import etree
from datetime import date
import numpy as np
import urllib3
import glob
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)
from pathlib import Path
from functools import lru_cache
from common.io.logger import logger


def try_first_value(try_list: list):
    if len(try_list) == 0:
        return np.nan
    else:
        if try_list[0].text is None:
            return np.nan
        else:
            try:
                return float(try_list[0].text)
            except:
                return np.nan



def try_first_text(try_list: list):
    if len(try_list) == 0:
        return np.nan
    else:
        return try_list[0].text

    
def normalize_company_name(x: pd.Series):
    return (x
            .fillna('')
            .str.replace('\n', '')
            .str.strip()
            .str.replace(' ', '') #)の後に空白があると全角かっこと判断されて?.stripでは空白削除できない
            .str.upper()
            .str.normalize('NFKC')
            .str.replace(re.compile(r'\(株\)|株式会社'), '', regex=True) # 株式会社削除
            .str.replace(re.compile(r'\(注[1-9]+\)|\(注\)[1-9]+|\(注\)'), '', regex=True) # 注記削除
            .str.replace(' ', '')
           )

class EdinetDecimal(Enum):

    MILLION = ('-6', 1000000)
    THOUSAND = ('-3', 1000)
    UNIT = ('0', 1)
    UNIT2 = ('2', 1)
    UNIT3 = ('3', 1)
    UNIT4 = ('4', 1)
    UNIT5 = ('5', 1)

    def __init__(self, code: str, multiple: int):
        self.code = code
        self.multiple = multiple

    @classmethod
    def code_to_multiple_dict(cls):
        return {d.code: d.multiple for d in EdinetDecimal}

    def code_to_multiple(code: str):
        return EdinetDecimal.code_to_multiple_dict().get(code)


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
            try:
                with zipfile.ZipFile(filename) as zf:
                    zf.extractall(os.path.join(output_dir_path, doc_id))
            except zipfile.BadZipfile:
                logger.info(f'''BadZipfile{filename}''')


class EdinetYuho:

    def __init__(self, dir_path: Path):
        self.dir_path = dir_path

    @staticmethod
    def to_value(etree):
        return float(etree.text)#*EdinetDecimal.code_to_multiple(etree.attrib.get('decimals'))

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
        major_shares = []
        for i in range(1,16):
            is_exists = len(root.xpath(f'''//jpcrp_cor:NameMajorShareholders[@contextRef="CurrentYearInstant_No{i}MajorShareholdersMember"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
            if is_exists == 1:
                member = try_first_text(root.xpath(f'''//jpcrp_cor:NameMajorShareholders[@contextRef="CurrentYearInstant_No{i}MajorShareholdersMember"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                share= try_first_value(root.xpath(f'''//jpcrp_cor:NumberOfSharesHeld[@contextRef="CurrentYearInstant_No{i}MajorShareholdersMember"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                adress = try_first_text(root.xpath(f'''//jpcrp_cor:AddressMajorShareholders[@contextRef="CurrentYearInstant_No{i}MajorShareholdersMember"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                ratio = try_first_value(root.xpath(f'''//jpcrp_cor:ShareholdingRatio[@contextRef="CurrentYearInstant_No{i}MajorShareholdersMember"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                major_shares.append([i, member, adress, share, ratio])
            elif is_exists == 0:
                pass
            else:
                logger.info(f'''Row{i} has Multiple Records''')
        return pd.DataFrame(major_shares, columns = ['順位', '氏名又は名称', '住所', '所有株式数', '発行済株式（自己株式を除く。）の総数に対する所有株式数の割合'])

    @property
    def treasury_share(self):
        root = self.eTree.getroot()
        treasury_shares = []
        for i in range(1,100):
            is_exists = len(root.xpath(f'''//jpcrp_cor:NameOfShareholderTreasurySharesEtc[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
            if is_exists == 1:
                holder = try_first_text(root.xpath(f'''//jpcrp_cor:NameOfShareholderTreasurySharesEtc[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                address = try_first_text(root.xpath(f'''//jpcrp_cor:AddressOfShareholderTreasurySharesEtc[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                shareown = try_first_value(root.xpath(f'''//jpcrp_cor:NumberOfSharesHeldInOwnNameTreasurySharesEtc[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                shareother = try_first_value(root.xpath(f'''//jpcrp_cor:NumberOfSharesHeldInOthersNamesTreasurySharesEtc[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                sharetotal = try_first_value(root.xpath(f'''//jpcrp_cor:TotalNumberOfSharesHeldTreasurySharesEtc[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                ratio = try_first_value(root.xpath(f'''//jpcrp_cor:ShareholdingRatioTreasurySharesEtc[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                treasury_shares.append([i, holder, address, shareown, shareother, sharetotal, ratio])
            elif is_exists == 0:
                pass
            else:
                logger.info(f'''Row{i} has Multiple Records''')
        return pd.DataFrame(treasury_shares, columns = ['レコード番号', '所有者の氏名又は名称', '所有者の住所', '自己名義所有株式数（株）', '他人名義所有株式数（株）', '所有株式数の合計（株）', '発行済株式総数に対する所有株式数の割合（％）'])


    @property
    def officer(self):
        root = self.eTree.getroot()
        titles = root.xpath(f'''//jpcrp_cor:OfficialTitleOrPositionInformationAboutDirectorsAndCorporateAuditors''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')})
        names = root.xpath(f'''//jpcrp_cor:NameInformationAboutDirectorsAndCorporateAuditors''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')})
        shares = root.xpath(f'''//jpcrp_cor:NumberOfSharesHeldOrdinarySharesInformationAboutDirectorsAndCorporateAuditors''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')})

        officers = []
        for i, (title, name, share) in enumerate(zip(titles, names, shares)):
            officers.append([i+1, title.text, name.text, np.nan if share.text==None else share.text])
        return pd.DataFrame(officers, columns = ['レコード番号', '役職名', '氏名', '所有株式数(普通株式)'])


    @property
    def issued_share(self):
        root = self.eTree.getroot()
        return try_first_value(root.xpath(f'''//jpcrp_cor:NumberOfIssuedSharesAsOfFiscalYearEndIssuedSharesTotalNumberOfSharesEtc[@contextRef="FilingDateInstant_OrdinaryShareMember"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))


    @property
    def stocksetc(self):
        root = self.eTree.getroot()

        # 特定投資
        specific_invests = []
        for i in range(1, 60):
            is_exists = len(root.xpath(f'''//jpcrp_cor:NameOfSecuritiesDetailsOfSpecifiedInvestmentEquitySecuritiesHeldForPurposesOtherThanPureInvestmentReportingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
            if is_exists == 1:
                name = try_first_text(root.xpath(f'''//jpcrp_cor:NameOfSecuritiesDetailsOfSpecifiedInvestmentEquitySecuritiesHeldForPurposesOtherThanPureInvestmentReportingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                share = try_first_value(root.xpath(f'''//jpcrp_cor:NumberOfSharesHeldDetailsOfSpecifiedInvestmentEquitySecuritiesHeldForPurposesOtherThanPureInvestmentReportingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                share_notdisclosed = try_first_value(root.xpath(f'''//jpcrp_cor:NumberOfSharesNotDisclosedAsBelowThresholdDetailsOfSpecifiedInvestmentSharesHeldForPurposesOtherThanPureInvestmentReportingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                book_value = try_first_value(root.xpath(f'''//jpcrp_cor:BookValueDetailsOfSpecifiedInvestmentEquitySecuritiesHeldForPurposesOtherThanPureInvestmentReportingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                carry_amount = try_first_value(root.xpath(f'''//jpcrp_cor:CarryingAmountNotDisclosedAsBelowThresholdDetailsOfSpecifiedInvestmentSharesHeldForPurposesOtherThanPureInvestmentReportingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                purpose = try_first_text(root.xpath(f'''//jpcrp_cor:PurposesOfHoldingDetailsOfSpecifiedInvestmentEquitySecuritiesHeldForPurposesOtherThanPureInvestmentReportingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                quant_effect = try_first_text(root.xpath(f'''//jpcrp_cor:QuantitativeEffectsOfShareholdingDetailsOfSpecifiedInvestmentSharesHeldForPurposesOtherThanPureInvestmentReportingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                incdec_reason = try_first_text(root.xpath(f'''//jpcrp_cor:ReasonForIncreaseInNumberOfSharesDetailsOfSpecifiedInvestmentSharesHeldForPurposesOtherThanPureInvestmentReportingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                purpose_long = try_first_text(root.xpath(f'''//jpcrp_cor:PurposeOfShareholdingQuantitativeEffectsOfShareholdingAndReasonForIncreaseInNumberOfSharesDetailsOfSpecifiedInvestmentSharesHeldForPurposesOtherThanPureInvestmentReportingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                inv_hold = try_first_text(root.xpath(f'''//jpcrp_cor:WhetherIssuerOfAforementionedSharesHoldsReportingCompanysSharesDetailsOfSpecifiedInvestmentSharesHeldForPurposesOtherThanPureInvestmentReportingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                specific_invests.append([0, i, name, share, share_notdisclosed, book_value, carry_amount, purpose, quant_effect, incdec_reason, purpose_long, inv_hold])
            elif is_exists == 0:
                pass
            else:
                logger.info(f'''Multiple Records for {i}''')

        # 最大保有
        for i in range(1, 60):
            is_exists = len(root.xpath(f'''//jpcrp_cor:NameOfSecuritiesDetailsOfSpecifiedInvestmentEquitySecuritiesHeldForPurposesOtherThanPureInvestmentLargestHoldingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
            if is_exists == 1:
                name = try_first_text(root.xpath(f'''//jpcrp_cor:NameOfSecuritiesDetailsOfSpecifiedInvestmentEquitySecuritiesHeldForPurposesOtherThanPureInvestmentLargestHoldingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                share = try_first_value(root.xpath(f'''//jpcrp_cor:NumberOfSharesHeldDetailsOfSpecifiedInvestmentEquitySecuritiesHeldForPurposesOtherThanPureInvestmentLargestHoldingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                share_notdisclosed = try_first_value(root.xpath(f'''//jpcrp_cor:NumberOfSharesNotDisclosedAsBelowThresholdDetailsOfSpecifiedInvestmentSharesHeldForPurposesOtherThanPureInvestmentLargestHoldingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                book_value = try_first_value(root.xpath(f'''//jpcrp_cor:BookValueDetailsOfSpecifiedInvestmentEquitySecuritiesHeldForPurposesOtherThanPureInvestmentLargestHoldingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                carry_amount = try_first_value(root.xpath(f'''//jpcrp_cor:CarryingAmountNotDisclosedAsBelowThresholdDetailsOfSpecifiedInvestmentSharesHeldForPurposesOtherThanPureInvestmentLargestHoldingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                purpose = try_first_text(root.xpath(f'''//jpcrp_cor:PurposesOfHoldingDetailsOfSpecifiedInvestmentEquitySecuritiesHeldForPurposesOtherThanPureInvestmentLargestHoldingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                quant_effect = try_first_text(root.xpath(f'''//jpcrp_cor:QuantitativeEffectsOfShareholdingDetailsOfSpecifiedInvestmentSharesHeldForPurposesOtherThanPureInvestmentLargestHoldingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                incdec_reason = try_first_text(root.xpath(f'''//jpcrp_cor:ReasonForIncreaseInNumberOfSharesDetailsOfSpecifiedInvestmentSharesHeldForPurposesOtherThanPureInvestmentLargestHoldingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                purpose_long = try_first_text(root.xpath(f'''//jpcrp_cor:PurposeOfShareholdingQuantitativeEffectsOfShareholdingAndReasonForIncreaseInNumberOfSharesDetailsOfSpecifiedInvestmentSharesHeldForPurposesOtherThanPureInvestmentLargestHoldingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                inv_hold = try_first_text(root.xpath(f'''//jpcrp_cor:WhetherIssuerOfAforementionedSharesHoldsReportingCompanysSharesDetailsOfSpecifiedInvestmentSharesHeldForPurposesOtherThanPureInvestmentLargestHoldingCompany[@contextRef="CurrentYearInstant_Row{i}Member"]''', namespaces={'jpcrp_cor': root.nsmap.get('jpcrp_cor')}))
                specific_invests.append([1, i, name, share, share_notdisclosed, book_value, carry_amount, purpose, quant_effect, incdec_reason, purpose_long, inv_hold])
            elif is_exists == 0:
                pass
            else:
                logger.info(f'''Multiple Records for {i}''')


        return pd.DataFrame(specific_invests, columns = ['LARGEST', 'レコード番号', '銘柄', '株式数', '株式数（記載省略)', '貸借対照表計上額', '貸借対照表計上額（記載省略）', '保有目的', '定量的な保有効果', '株式数が増加した理由',
                                                         '保有目的、定量的な保有効果及び株式数が増加した理由', '当該株式の発行者による提出会社の株式の保有の有無'])
