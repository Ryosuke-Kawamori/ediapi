docid = 'S100OC13'
edinet_yuho = EdinetYuho(f'''./edinet/{docid}/''')

major_shareholder = (
    edinet_yuho.major_shareholder
    .rename(columns = {'氏名又は名称': 'NAME',
                       '住所': 'ADDRESS', 
                       '所有株式数(千株)': 'SHARE',
                       '発行済株式(自己株式を除く)の総数に対する所有株式数の割合(％)': 'RATIO'
                      })
    .assign(SHARE = lambda df: df['SHARE'].astype(int)*1000)
    .assign(SEQNO = lambda df: range(1, len(df)+1))
    .assign(DOCID = docid)
    )
display(major_shareholder)

treasury_share = (
    edinet_yuho.treasury_share
    .rename(columns = {'所有者の氏名又は名称': 'NAME',
                       '所有者の住所': 'ADDRESS',
                       '自己名義所有株式数(株)': 'SELF_SHARE',
                       '他人名義所有株式数(株)': 'OTHER_SHARE',
                       '所有株式数の合計(株)': 'SHARE',
                       '発行済株式総数に対する所有株式数の割合(％)': 'RATIO'
                      }
           )
    .assign(SHARE = lambda df: df['SHARE'].astype(int))
    .assign(SELF_SHARE = lambda df: df['SELF_SHARE'].replace({'―', 0}))
    .assign(OTHER_SHARE = lambda df: df['OTHER_SHARE'].replace('―', 0))
    .assign(SEQNO = lambda df: range(1, len(df)+1))
    .assign(DOCID = docid)
)
display(treasury_share)

officer = (
    edinet_yuho.officer
    .rename(columns = {'役職名': 'POSITION',
                       '氏名': 'NAME',
                       '生年月日': 'BIRTH',
                       '略歴': 'BIOGRAPHY',
                       '任期': 'TERMOFOFFICE',
                       '所有株式数(千株)': 'SHARE'
           })
    .assign(SHARE = lambda df: df['SHARE'].astype(int)*1000)
    .assign(SEQNO = lambda df: range(1, len(df)+1))
    .assign(DOCID = docid)
)
display(officer)


RKPostgres().insert_dataframe('edinet.major_shareholder', major_shareholder)
RKPostgres().insert_dataframe('edinet.treasury_share', treasury_share)
RKPostgres().insert_dataframe('edinet.officer', officer)
