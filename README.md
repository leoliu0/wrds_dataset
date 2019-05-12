# wrds_dataset
A unbeatable utility to pre-process popular WRDS datasets for finance, economics and management research

So far I only output parquet file, if you need other formats, you can change all "to_parquet" to "to_csv" or "to_stata" in python code.
python does not export SAS format. You may also want to change file extension in the code.

You need to have a WRDS account, change my username to yours.

I am lazy, so I did not optimise code. To use this utility, you need at least 64G ram, a unix system with bash installed (macOS and linux has built-in)
For windows users, you need to install and enable linux subsystem (Ubuntu recommended). If you do not know how to, you should google it.

Additionally, you should have python3.6+ installed, you can download and install Anaconda distribution of Python. Google it if you dont know how to.

You will get the following:
funda-ccm - Compustat CRSP merged database with non-missing assets, (gvkey-permno linked)
funda - whole compustat funda (gvkey-permno linked)

msf - msf with gvkey (dropped missing ret)
dsf - dsf with gvkey (dropped missing ret)
msf-ccm - msf with non-missing gvkey
dsf-ccm - dsf with non-missing gvkey

Each time you run the script, All database synced with WRDS.
