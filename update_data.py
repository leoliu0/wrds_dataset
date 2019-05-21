import pandas as pd
from glob import glob
import numpy as np


def get_fyear(df):
    if df.datadate.month > 6:
        return df.datadate.year
    else:
        return df.datadate.year - 1


# Process the link file and create daily link file
print('process link file ... takes about 10 mins')
ccm_link = pd.read_sas('ccmxpf_linktable.sas7bdat', encoding='latin1')
ccm = ccm_link[lambda df:df.linktype.isin(['LU', 'LC'])][lambda df:df.linkprim.isin(['P', 'C'])][
    ['gvkey', 'lpermno', 'linkdt', 'linkenddt']]
ccm.linkenddt = ccm.linkenddt.fillna(np.datetime64('today'))

link_full = []
for gvkey, permno, linkdt, linkenddt in ccm.values:
    d = linkdt
    while d <= linkenddt:
        link_full.append([gvkey, permno, d])
        d += np.timedelta64('1', 'D')

link_full = pd.DataFrame(link_full)

print('process funda .... takes about 5 mins')
funda = pd.read_sas('funda.sas7bdat', encoding='latin1')

funda['_fyear'] = funda.apply(get_fyear, axis=1)
funda['fyear'] = funda['fyear'].fillna(funda._fyear)
funda.drop('_fyear', axis=1)
funda = funda[lambda df:(df.datafmt == 'STD') & (df.consol == 'C') & (df.indfmt == 'INDL')
              ][lambda df:df['at'].notnull()]

ccm_linktable = pd.read_parquet('ccm_linktable.par')
ccm = funda.merge(ccm_linktable.rename({'date': 'datadate'}, axis=1))
fundaccm = funda.merge(ccm_linktable.rename(
    {'date': 'datadate'}, axis=1), how='left')

ccm.to_parquet('ccm.par')
fundaccm.to_parquet('fundaccm.par')

print('process msf and dsf ... takes about 1 hour')
msf = pd.read_sas('msf.sas7bdat',encoding='latin1')
dsf = pd.read_sas('dsf.sas7bdat',encoding='latin1')

msf['fyear'] = msf.DATE.map(get_fyear)
msf['year'] = msf.DATE.map(lambda s:s.year)
msf['month'] = msf.DATE.map(lambda s:s.month)
msf = msf.merge(ccm_linktable.rename({'permno':'PERMNO','date':'DATE'},axis=1),how='left')
msf_ccm = msf[lambda df:df.gvkey.notnull()]
msf_ccm.to_parquet('msf_ccm.par')
msf.to_parquet('msf.par')

dsf['fyear'] = dsf.DATE.map(get_fyear)
dsf['year'] = dsf.DATE.map(lambda s:s.year)
dsf['month'] = dsf.DATE.map(lambda s:s.month)
dsf = dsf.merge(ccm_linktable.rename({'permno':'PERMNO','date':'DATE'},axis=1),how='left')
dsf_ccm = dsf[lambda df:df.gvkey.notnull()]
dsf_ccm.to_parquet('dsf_ccm.par')
dsf.to_parquet('dsf.par')

