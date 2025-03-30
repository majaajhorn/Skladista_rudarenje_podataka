# Imports
import pandas as pd
import json
import requests
import random
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

'''
3. Skripta za stvaranje sheme i import podataka u bazu podataka

Potrebno je modificirati skriptu za vlastiti skup podataka.
U ovom koraku stvaramo shemu baze podataka i importiramo podatke u bazu podataka.
'''

# Putanja do predprocesirane CSV datoteke
CSV_FILE_PATH = "Checkpoint 2/2_relational_model/processed/SuperStoreOrders_PROCESSED_80.csv"

# Učitavanje CSV datoteke u dataframe
df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print(f"CSV size: {df.shape}")  # Print dataset size
print(df.head())  # Preview first few rows

# Database Connection
Base = declarative_base()

# Definiranje sheme baze podataka
# --------------------------------------------------------------
class Country(Base):
    __tablename__ = 'country'
    id = Column(Integer, primary_key=True)
    name = Column(String(45), nullable=False, unique=True)
    region = Column(String(45), nullable=False)
    market = Column(String(45), nullable=False)

class Customer(Base):
    __tablename__ = 'customer'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    segment = Column(String(45), nullable=False)
    state = Column(String(100), nullable=False)
    country_fk = Column(Integer, ForeignKey('country.id'))

class ShipMode(Base):
    __tablename__ = 'ship_mode'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)

class OrderPriority(Base):
    __tablename__ = 'order_priority'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)

class Category(Base):
    __tablename__ = 'category'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)

class SubCategory(Base):
    __tablename__ = 'subcategory'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(45), nullable=False, unique=True)
    category_fk = Column(Integer, ForeignKey('category.id'))

class Product(Base):
    __tablename__ = 'product'
    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(String(45), nullable=False, unique=True)
    name = Column(String(255), nullable=False)
    subcategory_fk = Column(Integer, ForeignKey('subcategory.id'))

class Order(Base):
    __tablename__ = 'order'
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(45), nullable=False)
    order_date = Column(Date, nullable=False)
    ship_date = Column(Date, nullable=False)
    year = Column(Integer, nullable=False)
    customer_fk = Column(Integer, ForeignKey('customer.id'))
    ship_mode_fk = Column(Integer, ForeignKey('ship_mode.id'))
    order_priority_fk = Column(Integer, ForeignKey('order_priority.id'))

class OrderDetails(Base):
    __tablename__ = 'order_details'
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_fk = Column(Integer, ForeignKey('order.id'))
    product_fk = Column(Integer, ForeignKey('product.id'))
    quantity = Column(Integer, nullable=False)
    sales = Column(Float, nullable=False)
    discount = Column(Float, nullable=False)
    profit = Column(Float, nullable=False)
    shipping_cost = Column(Float, nullable=False)

# Database Connection
engine = create_engine('mysql+pymysql://root:root@localhost:3306/superstore_dw', echo=False)
Base.metadata.drop_all(engine)  # Brisanje postojećih tablica
Base.metadata.create_all(engine)  # Stvaranje tablica

Session = sessionmaker(bind=engine) # Stvaranje sesije
session = Session() # Otvori novu sesiju

# --------------------------------------------------------------
# Import podataka
# --------------------------------------------------------------

# **1. Umetanje zemalja**
countries = df[['country', 'region', 'market']].drop_duplicates('country') # Dohvatimo jedinstvene zemlje
countries_list = []

for i, row in countries.iterrows():
    country_entry = {
        "id": i + 1,
        "name": row['country'],
        "region": row['region'],
        "market": row['market']
    }
    countries_list.append(country_entry)

session.bulk_insert_mappings(Country, countries_list) # Bulk insert
session.commit() 

country_map = {c.name: c.id for c in session.query(Country).all()} # Stvori mapiranje zemalja koje će nam trebati kasnije za strane ključeve

# **2. Umetanje načina dostave**
ship_modes = df[['ship_mode']].drop_duplicates().rename(columns={'ship_mode': 'name'}) # Dohvatimo jedinstvene načine dostave
ship_modes_list = ship_modes.to_dict(orient="records") # Pretvori u listu rječnika

session.bulk_insert_mappings(ShipMode, ship_modes_list) # Bulk insert
session.commit()

ship_mode_map = {sm.name: sm.id for sm in session.query(ShipMode).all()} # Stvori mapiranje načina dostave

# **3. Umetanje prioriteta narudžbe**
order_priorities = df[['order_priority']].drop_duplicates().rename(columns={'order_priority': 'name'}) # Dohvatimo jedinstvene prioritete
order_priorities_list = order_priorities.to_dict(orient="records") # Pretvori u listu rječnika

session.bulk_insert_mappings(OrderPriority, order_priorities_list) # Bulk insert
session.commit()

order_priority_map = {op.name: op.id for op in session.query(OrderPriority).all()} # Stvori mapiranje prioriteta

# **4. Umetanje kategorija**
categories = df[['category']].drop_duplicates().rename(columns={'category': 'name'}) # Dohvatimo jedinstvene kategorije
session.bulk_insert_mappings(Category, categories.to_dict(orient="records")) # Bulk insert
session.commit()

category_map = {c.name: c.id for c in session.query(Category).all()} # Stvori mapiranje kategorija

# **5. Umetanje potkategorija**
subcategories = df[['sub_category', 'category']].drop_duplicates() # Dohvatimo jedinstvene potkategorije i kategorije
subcategories['category_fk'] = subcategories['category'].map(category_map) # Mapiraj kategorije iz teksta u ID
subcategories = subcategories.rename(columns={'sub_category': 'name'}).drop(columns=['category']) # Preimenuj stupce i izbaci nepotrebne stupce
session.bulk_insert_mappings(SubCategory, subcategories.to_dict(orient="records")) # Bulk insert
session.commit()

subcategory_map = {sc.name: sc.id for sc in session.query(SubCategory).all()} # Stvori mapiranje potkategorija

# **6. Umetanje proizvoda**
products = df[['product_id', 'product_name', 'sub_category']].drop_duplicates('product_id') # Dohvatimo jedinstvene proizvode
products['subcategory_fk'] = products['sub_category'].map(subcategory_map) # Mapiraj potkategorije iz teksta u ID
products = products.rename(columns={'product_name': 'name'}).drop(columns=['sub_category']) # Preimenuj stupce i izbaci nepotrebne stupce
session.bulk_insert_mappings(Product, products.to_dict(orient="records")) # Bulk insert
session.commit()

product_map = {p.product_id: p.id for p in session.query(Product).all()} # Stvori mapiranje proizvoda po product_id

# **7. Umetanje kupaca**
customers = df[['customer_name', 'segment', 'state', 'country']].drop_duplicates() # Dohvatimo jedinstvene kupce
customers['country_fk'] = customers['country'].map(country_map) # Mapiraj zemlje iz teksta u ID
customers = customers.rename(columns={'customer_name': 'name'}).drop(columns=['country']) # Preimenuj stupce i izbaci nepotrebne stupce
session.bulk_insert_mappings(Customer, customers.to_dict(orient="records")) # Bulk insert
session.commit()

customer_map = {c.name: c.id for c in session.query(Customer).all()} # Stvori mapiranje kupaca

# **8. Umetanje narudžbi**
orders = df[['order_id', 'order_date', 'ship_date', 'year', 'customer_name', 'ship_mode', 'order_priority']].drop_duplicates('order_id') # Dohvatimo jedinstvene narudžbe
orders['customer_fk'] = orders['customer_name'].map(customer_map) # Mapiraj kupce iz teksta u ID
orders['ship_mode_fk'] = orders['ship_mode'].map(ship_mode_map) # Mapiraj načine dostave iz teksta u ID
orders['order_priority_fk'] = orders['order_priority'].map(order_priority_map) # Mapiraj prioritete iz teksta u ID
orders = orders.drop(columns=['customer_name', 'ship_mode', 'order_priority']) # Izbaci nepotrebne stupce
orders = orders.dropna(subset=['order_date', 'ship_date']) # Ukloni redove s NaT vrijednostima

# Konvertiraj string datume u Date objekte ako već nisu
try:
    orders['order_date'] = pd.to_datetime(orders['order_date'])
    orders['ship_date'] = pd.to_datetime(orders['ship_date'])
except:
    print("Datumi su već u datetime formatu")

session.bulk_insert_mappings(Order, orders.to_dict(orient="records")) # Bulk insert
session.commit()

# Stvori mapiranje narudžbi po order_id (može biti više redaka s istim order_id)
order_map = {}
for o in session.query(Order).all():
    if o.order_id not in order_map:
        order_map[o.order_id] = []
    order_map[o.order_id].append(o.id)

# **9. Umetanje detalja narudžbi**
order_details = df[['order_id', 'product_id', 'quantity', 'sales', 'discount', 'profit', 'shipping_cost']].copy() # Dohvati potrebne stupce

# Stvaranje nove liste za detalje narudžbi
order_details_list = []

for _, row in order_details.iterrows():
    # Ako postoji više order_id-ova s istim imenom, uzmi prvi
    order_id = row['order_id']
    if order_id in order_map and len(order_map[order_id]) > 0:
        order_fk = order_map[order_id][0]
    else:
        print(f"Upozorenje: order_id {order_id} nije pronađen u bazi")
        continue
    
    # Ako postoji product_id u mapiranju, koristi ga, inače preskoči
    product_id = row['product_id']
    if product_id in product_map:
        product_fk = product_map[product_id]
    else:
        print(f"Upozorenje: product_id {product_id} nije pronađen u bazi")
        continue
    
    detail = {
        'order_fk': order_fk,
        'product_fk': product_fk,
        'quantity': row['quantity'],
        'sales': row['sales'],
        'discount': row['discount'],
        'profit': row['profit'],
        'shipping_cost': row['shipping_cost']
    }
    order_details_list.append(detail)

# Bulk insert detalja narudžbi
for i in range(0, len(order_details_list), 1000):  # Podijeli unos u manje skupine po 1000 redova
    batch = order_details_list[i:i+1000]
    session.bulk_insert_mappings(OrderDetails, batch)
    session.commit()
    print(f"Ubačeno {i+len(batch)} od {len(order_details_list)} detalja narudžbi")

print("Data imported successfully!")