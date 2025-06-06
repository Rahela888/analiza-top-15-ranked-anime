
print("Hello World")


import pandas as pd
PATH = r"C:\Users\rahel\Downloads\archive\top_anime_dataset.csv"
data_first_2000 = pd.read_csv(PATH, nrows=2000)


print("\nprvih 5 redova:")
print(data_first_2000.head())


data = pd.read_csv(PATH)


print("\ndimenzije skupa podataka::")
print(data.shape)


print("\nbroj nedostajućih vrijednosti po stupcima:")
print(data.isna().sum())


print("\nbroj jedinstvenih vrijednosti po stupcima:")
print(data.nunique())


print("\ntipovi podataka po stupcima:")
print(data.dtypes)



print("\ndetaljna distribucija vrijednosti po stupcima:")
for column in data:
    print(f"\n--- {column} ---")
    print(data[column].value_counts())
    input("Pritisni enter")


print("\nNazivi svih stupaca:")
print(data.columns.values)
