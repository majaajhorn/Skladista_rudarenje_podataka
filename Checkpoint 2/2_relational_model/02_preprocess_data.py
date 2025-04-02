import pandas as pd

# 2. Skripta za predprocesiranje skupa podataka

# Određivanje putanje do CSV datoteke
CSV_FILE_PATH = "Skladista_rudarenje_podataka\Checkpoint 1\SuperStoreOrders.csv"

# Učitavanje CSV datoteke, ispis broja redaka i stupaca
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print("CSV size before: ", df.shape)

# Na temelju analize iz Checkpointa 1, znamo da nema nedostajućih vrijednosti
# Iz analize vidimo da nemamo nedostajućih vrijednosti pa nije potrebno koristiti dropna()

# Možemo pretvoriti 'sales' iz string u float za lakšu analizu
df['sales'] = df['sales'].str.replace(',', '').astype(float)

# Pretvaranje datuma u standardni format
df['order_date'] = pd.to_datetime(df['order_date'], dayfirst=True, errors='coerce')
df['ship_date'] = pd.to_datetime(df['ship_date'], dayfirst=True, errors='coerce')

# Pretvori sve nazive stupaca u mala slova
df.columns = df.columns.str.lower()

# Zamjena razmaka u nazivima stupaca s donjom crtom
df.columns = df.columns.str.replace(' ', '_')

print("CSV size after: ", df.shape) # Ispis broja redaka i stupaca nakon predprocesiranja
print(df.head()) # Ispis prvih redaka dataframe-a

# Random dijeljenje skupa podataka na dva dijela 80:20 (trebat će nam kasnije)
df20 = df.sample(frac=0.2, random_state=1)
df80 = df.drop(df20.index)
print("CSV size 80: ", df80.shape)
print("CSV size 20: ", df20.shape)

# Spremanje predprocesiranog skupa podataka u novu CSV datoteku
df80.to_csv("Skladista_rudarenje_podataka/Checkpoint 2/2_relational_model/processed/SuperStoreOrders_PROCESSED_80.csv", index=False)
df20.to_csv("Skladista_rudarenje_podataka/Checkpoint 2/2_relational_model/processed/SuperStoreOrders_PROCESSED_20.csv", index=False)


"""
CSV size before:  (51290, 21)
CSV size after:  (51290, 21)
          order_id order_date  ship_date       ship_mode    customer_name      segment            state    country  ...                 product_name  sales quantity discount   profit shipping_cost  order_priority  year
0     AG-2011-2040 2011-01-01 2011-01-06  Standard Class  Toby Braunhardt     Consumer      Constantine    Algeria  ...          Tenex Lockers, Blue  408.0        2      0.0  106.140         35.46          Medium  2011
1    IN-2011-47883 2011-01-01 2011-01-08  Standard Class      Joseph Holt     Consumer  New South Wales  Australia  ...     Acme Trimmer, High Speed  120.0        3      0.1   36.036          9.72          Medium  2011
2     HU-2011-1220 2011-01-01 2011-01-05    Second Class    Annie Thurman     Consumer         Budapest    Hungary  ...      Tenex Box, Single Width   66.0        4      0.0   29.640          8.17            High  2011
3  IT-2011-3647632 2011-01-01 2011-01-05    Second Class     Eugene Moren  Home Office        Stockholm     Sweden  ...  Enermax Note Cards, Premium   45.0        3      0.5  -26.055          4.82            High  2011
4    IN-2011-47883 2011-01-01 2011-01-08  Standard Class      Joseph Holt     Consumer  New South Wales  Australia  ...   Eldon Light Bulb, Duo Pack  114.0        5      0.1   37.770          4.70          Medium  2011

[5 rows x 21 columns]
CSV size 80:  (51290, 21)
CSV size 20:  (10258, 21)
"""