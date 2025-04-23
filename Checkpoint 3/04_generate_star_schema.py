from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Date, DateTime, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

'''
Skripta za generiranje dimenzijskog modela podataka -> star schema
U ovom koraku generiramo dimenzijski model podataka (data mart) za SuperStore.
Dimenzijski model podataka je star schema koji se sastoji od jedne tablice činjenica 
i više tablica dimenzija.
'''

# Define the database connection
DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/superstore_dw"
engine = create_engine(DATABASE_URL, echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# Define Dimensional Model Tables

# Dimenzija vremena - hijerahija godina > kvartal > mjesec > dan
class DimDate(Base):
    __tablename__ = 'dim_date'
    date_tk = Column(Integer, primary_key=True, autoincrement=True)
    date_value = Column(Date, nullable=False)
    day = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    is_holiday = Column(Integer, nullable=False, default=0)
    day_of_week = Column(Integer, nullable=False)
    day_name = Column(String(10), nullable=False)
    month_name = Column(String(10), nullable=False)

# Dimenzija kupca - hijerarhija: country > region > state > customer
class DimCustomer(Base):
    __tablename__ = 'dim_customer'
    customer_tk = Column(BigInteger, primary_key=True)
    version = Column(Integer)
    date_from = Column(DateTime)
    date_to = Column(DateTime)
    customer_id = Column(Integer, index=True)
    customer_name = Column(String(100))
    segment = Column(String(45))
    state = Column(String(100))
    country = Column(String(45))
    region = Column(String(45))
    market = Column(String(45))

# Dimenzija proizvoda - hijerarhija: category > subcategory > product
class DimProduct(Base):
    __tablename__ = 'dim_product'
    product_tk = Column(BigInteger, primary_key=True)
    version = Column(Integer)
    date_from = Column(DateTime)
    date_to = Column(DateTime)
    product_id = Column(String(45), index=True)
    product_name = Column(String(255))
    subcategory = Column(String(45))
    category = Column(String(45))

# Dimenzija načina dostave
class DimShipMode(Base):
    __tablename__ = 'dim_ship_mode'
    ship_mode_tk = Column(BigInteger, primary_key=True)
    version = Column(Integer)
    date_from = Column(DateTime)
    date_to = Column(DateTime)
    ship_mode_id = Column(Integer, index=True)
    ship_mode_name = Column(String(45))

# Dimenzija prioriteta narudžbe
class DimOrderPriority(Base):
    __tablename__ = 'dim_order_priority'
    order_priority_tk = Column(BigInteger, primary_key=True)
    version = Column(Integer)
    date_from = Column(DateTime)
    date_to = Column(DateTime)
    order_priority_id = Column(Integer, index=True)
    order_priority_name = Column(String(45))

# Degenerirana dimenzija narudžbe
class DimOrder(Base):
    __tablename__ = 'dim_order'
    order_tk = Column(BigInteger, primary_key=True)
    order_id = Column(String(45), nullable=False, index=True)

# Tablica činjenica za prodaju
class FactSales(Base):
    __tablename__ = 'fact_sales'
    fact_sales_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # Foreign keys to dimension tables
    date_tk = Column(Integer, ForeignKey('dim_date.date_tk'))
    customer_tk = Column(BigInteger, ForeignKey('dim_customer.customer_tk'))
    product_tk = Column(BigInteger, ForeignKey('dim_product.product_tk'))
    ship_mode_tk = Column(BigInteger, ForeignKey('dim_ship_mode.ship_mode_tk'))
    order_priority_tk = Column(BigInteger, ForeignKey('dim_order_priority.order_priority_tk'))
    order_tk = Column(BigInteger, ForeignKey('dim_order.order_tk'))
    ship_date_tk = Column(Integer, ForeignKey('dim_date.date_tk'))
    
    # Measures
    quantity = Column(Integer, nullable=False)
    sales = Column(Float, nullable=False)
    discount = Column(Float, nullable=False)
    profit = Column(Float, nullable=False)
    shipping_cost = Column(Float, nullable=False)

# Create Tables in the Database
Base.metadata.drop_all(engine)  # Brisanje postojećih tablica
Base.metadata.create_all(engine)  # Stvaranje tablica

print("Dimensional model tables created successfully!")