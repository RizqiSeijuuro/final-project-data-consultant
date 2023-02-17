from functions.ingestion import extract_from_spreadsheet, transform, load_to_spreadsheet

# Extract from different sources of spreadsheets
link_data_fuel = 'https://docs.google.com/spreadsheets/d/1SXm1q3nFtxYi4oh_p4DtHVp5oADrjbPM_yqGmB87Ezg/edit?usp=sharing'
link_data_isi_fuel = 'https://docs.google.com/spreadsheets/d/1Dah9BWXvJSfrn1FpveSS0i40aAFJTkiZrwhU-n5SOf0/edit?usp=sharing'
link_data_produksi = 'https://docs.google.com/spreadsheets/d/17VARhYVq21zlXADqyYKaE50tW4uN9FEqtqFe0MR-m2s/edit?usp=sharing'
link_data_ritase = 'https://docs.google.com/spreadsheets/d/1Y5hFGnRY3t1WMko9-wEkjSKJ3Ta6olAUBjvO-0KHeDw/edit?usp=sharing'
sheetname = 'Form responses 1'
data_fuel = extract_from_spreadsheet(link_data_fuel, sheetname)
data_isi_fuel = extract_from_spreadsheet(link_data_isi_fuel, sheetname)
data_produksi = extract_from_spreadsheet(link_data_produksi, sheetname)
data_ritase = extract_from_spreadsheet(link_data_ritase, sheetname)

# Transform and join all data into one
new_data = transform(data_isi_fuel, data_produksi, data_ritase)

# Load the data into new spreadsheet
link_data_warehouse = 'https://docs.google.com/spreadsheets/d/1qc25VHKs4oRaiMmVPdZeuedJbIoSAWjp62GIlfIksms/edit?usp=sharing'
sheetname = 'Sheet1'
load_to_spreadsheet(new_data, link_data_warehouse, sheetname)