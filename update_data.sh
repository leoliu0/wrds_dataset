#!bin/bash

scp z3486253@wrds-cloud.wharton.upenn.edu:/wrds/crsp/sasdata/a_stock/dsf.sas7bdat .
scp z3486253@wrds-cloud.wharton.upenn.edu:/wrds/crsp/sasdata/a_stock/msf.sas7bdat .
scp z3486253@wrds-cloud.wharton.upenn.edu:/wrds/comp/sasdata/naa/funda.sas7bdat .
scp z3486253@wrds-cloud.wharton.upenn.edu:/wrds/crsp/sasdata/a_stock/msi.sas7bdat .
scp z3486253@wrds-cloud.wharton.upenn.edu:/wrds/crsp/sasdata/a_stock/dsi.sas7bdat .
scp z3486253@wrds-cloud.wharton.upenn.edu:/wrds/comp/sasdata/naa/names.sas7bdat .
scp z3486253@wrds-cloud.wharton.upenn.edu:/wrds/kld/sasdata/history.sas7bdat .
scp z3486253@wrds-cloud.wharton.upenn.edu:/wrds/crsp/sasdata/a_stock/stocknames.sas7bdat .

python3 update_data.py

rm *.sas7bdat
