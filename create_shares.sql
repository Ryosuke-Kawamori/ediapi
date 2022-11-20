CREATE TABLE edinet.treasury_share(
                                      DOCID char(8),
                                      SEQNO int,
                                      NAME varchar(100),
                                      ADDRESS varchar(300),
                                      SELF_SHARE bigint,
                                      OTHER_SHARE bigint,
                                      SHARE bigint,
                                      RATIO float,
                                      PRIMARY KEY (DOCID, SEQNO)
)

CREATE TABLE edinet.stocksetc(
                                 DOCID char(8),
                                 SEQNO int,
                                 COMPANY_NAME varchar(200),
                                 SHARE bigint,
                                 BOOK_VALUE bigint,
                                 PURPOSE varchar(200),
                                 QUANTITATIVE_EFFECT varchar(400),
                                 INCDEC_REASON varchar(200),
                                 REASON varchar(300),
                                 HELD varchar(30),
                                 PRIMARY KEY (DOCID, SEQNO)
)

CREATE TABLE edinet.officer(
                               DOCID char(8),
                               SEQNO int,
                               POSITION varchar(200),
                               NAME varchar(100),
                               SHARE bigint,
                               PRIMARY KEY (DOCID, SEQNO)
)


CREATE TABLE edinet.major_shareholder(
                                         DOCID char(8),
                                         RANK int,
                                         NAME varchar(200),
                                         ADDRESS varchar(200),
                                         SHARE bigint,
                                         RATIO float,
                                         PRIMARY KEY (DOCID, RANK)
)
