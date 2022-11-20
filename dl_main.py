dame_docid = []

for idx, df in tqdm(meta.pipe(lambda df: df.dropna(subset=['SECCODE'])).iterrows()):
    docid = df['DOCID']
    target_path = './edinet/'+docid
    if len(glob.glob(target_path))==0:
        EdinetApi().get_doc(docid, output_dir_path='./edinet/')
    else:
        pass
    
    edinet_yuho = EdinetYuho(dir_path=os.path.join('./edinet', docid))
    
    try:
        major_shareholder = (
        edinet_yuho.major_shareholder
        .rename(columns = {'順位': 'RANK',
                           '氏名又は名称': 'NAME',
                           '住所': 'ADDRESS', 
                           '所有株式数': 'SHARE',
                           '発行済株式（自己株式を除く。）の総数に対する所有株式数の割合': 'RATIO'
                          })
        .assign(DOCID = docid)
        )

        treasury_share = (
            edinet_yuho.treasury_share
            .rename(columns = {'レコード番号': 'SEQNO',
                               '所有者の氏名又は名称': 'NAME',
                               '所有者の住所': 'ADDRESS',
                               '自己名義所有株式数（株）': 'SELF_SHARE',
                               '他人名義所有株式数（株）': 'OTHER_SHARE',
                               '所有株式数の合計（株）': 'SHARE',
                               '発行済株式総数に対する所有株式数の割合（％）': 'RATIO'
                              }
                   )
            .assign(SHARE = lambda df: df['SHARE'])
            .assign(SEQNO = lambda df: range(1, len(df)+1))
            .assign(DOCID = docid)
        )

        officer = (
            edinet_yuho.officer
            .rename(columns = {'レコード番号': 'SEQNO',
                               '役職名': 'POSITION',
                               '氏名': 'NAME',
                               '所有株式数(普通株式)': 'SHARE'
                   })
            .assign(DOCID = docid)
        )

        stocksetc = (
            edinet_yuho.stocksetc
             .rename(columns = {'レコード番号': 'SEQNO',
                                '銘柄': 'COMPANY_NAME',
                                '株式数': 'SHARE',
                                '貸借対照表計上額': 'BOOK_VALUE',
                                '保有目的': 'PURPOSE',
                                '定量的な保有効果': 'QUANTITATIVE_EFFECT',
                                '株式数が増加した理由': 'INCDEC_REASON',
                                '保有目的、定量的な保有効果及び株式数が増加した理由': 'REASON',
                                '当該株式の発行者による提出会社の株式の保有の有無': 'HELD'})
        .assign(DOCID = docid)
        [['DOCID', 'LARGEST', 'SEQNO', 'COMPANY_NAME', 'SHARE', 'BOOK_VALUE', 'PURPOSE', 'QUANTITATIVE_EFFECT', 'INCDEC_REASON', 'REASON', 'HELD']]
        )
        
        issued_share = pd.DataFrame({'DOCID': [docid], 'ISSUED_SHARE': [edinet_yuho.issued_share]})

        RKPostgres().insert_dataframe('edinet.major_shareholder', major_shareholder)
        RKPostgres().insert_dataframe('edinet.treasury_share', treasury_share)
        RKPostgres().insert_dataframe('edinet.officer', officer)
        RKPostgres().insert_dataframe('edinet.stocksetc', stocksetc)
        RKPostgres().insert_dataframe('edinet.issued_share', issued_share)
    
    except:
        logger.info(docid)
        dame_docid.append(docid)
