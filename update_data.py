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
link_full.columns = ['gvkey','permno','date']
link_full['gvkey'] = pd.to_numeric(link_full.gvkey)
print(f'writing link table')
link_full.to_parquet('ccm_linktable.par')

print('process funda .... takes about 5 mins')

comp_name = pd.read_sas('names.sas7bdat',encoding='latin1')
comp_name = comp_name[['gvkey','sic','naics','year1','year2']]
for col in comp_name.columns:
    comp_name[col] = pd.to_numeric(comp_name[col])

funda = pd.read_sas('funda.sas7bdat', encoding='latin1')
funda['gvkey'] = pd.to_numeric(funda.gvkey)
funda['cik'] = pd.to_numeric(funda.cik)
funda['_fyear'] = funda.apply(get_fyear, axis=1)
funda['fyear'] = funda['fyear'].fillna(funda._fyear)
funda.drop('_fyear', axis=1)
funda = funda[lambda df:(df.datafmt == 'STD') & (df.consol == 'C') & (df.indfmt == 'INDL')
              ][lambda df:df['at'].notnull()]

funda = funda.merge(comp_name,on='gvkey')

funda.columns = [x.lower() for x in funda.columns]

ccm_linktable = pd.read_parquet('ccm_linktable.par')
ccm = funda.merge(ccm_linktable.rename({'date': 'datadate'}, axis=1))
fundaccm = funda.merge(ccm_linktable.rename(
    {'date': 'datadate'}, axis=1), how='left')

print(f'writing ccm, total {len(ccm)} obs')
ccm.to_parquet('ccm.par')
print(f'writing fundaccm, total {len(fundaccm)} obs')
fundaccm.to_parquet('fundaccm.par')

print('process msf and dsf ... takes about 1 hour')
msf = pd.read_sas('msf.sas7bdat',encoding='latin1')
dsf = pd.read_sas('dsf.sas7bdat',encoding='latin1')

msf.columns = [x.lower() for x in msf.columns]
dsf.columns = [x.lower() for x in dsf.columns]

msf['fyear'] = msf.date.map(lambda s:s.year if s.month>6 else s.year-1)
msf['year'] = msf.date.map(lambda s:s.year)
msf['month'] = msf.date.map(lambda s:s.month)
msf = msf.merge(ccm_linktable,how='left')
msf_ccm = msf[lambda df:df.gvkey.notnull()]
print(f'writing msf_ccm, total {len(msf_ccm)} obs')
msf_ccm.to_parquet('msf_ccm.par')
print(f'writing msf, total {len(msf)} obs')
msf.to_parquet('msf.par')

dsf['fyear'] = dsf.date.map(lambda s:s.year if s.month>6 else s.year-1)
dsf['year'] = dsf.date.map(lambda s:s.year)
dsf['month'] = dsf.date.map(lambda s:s.month)
dsf = dsf.merge(ccm_linktable,how='left')
dsf_ccm = dsf[lambda df:df.gvkey.notnull()]
print(f'writing dsf_ccm, total {len(dsf_ccm)} obs')
dsf_ccm.to_parquet('dsf_ccm.par')
print(f'writing dsf, total {len(dsf)} obs')
dsf.to_parquet('dsf.par')

# KLD data
print('KLD data update')
kld = pd.read_sas('history.sas7bdat', encoding='latin1')
kld.to_parquet('kld.par')
