# transform/pipeline.py
from transform.dimensions.customer_dim import transform_customer_dim
from transform.dimensions.ship_mode_dim import transform_ship_mode_dim
from transform.dimensions.order_priority_dim import transform_order_priority_dim
from transform.dimensions.order_dim import transform_order_dim
from transform.dimensions.date_dim import transform_date_dim
from transform.dimensions.product_dim import transform_product_dim
from transform.facts.fact_sales import transform_fact_sales

def run_transformations(raw_data):
    print("⏳ Transforming date dimension...")
    date_dim = transform_date_dim(raw_data["order"])
    print("✅ Date dimension transformed.")
    
    print("⏳ Transforming customer dimension...")
    customer_dim = transform_customer_dim(raw_data["customer"], raw_data["country"])
    print("✅ Customer dimension transformed.")
    
    print("⏳ Transforming ship mode dimension...")
    ship_mode_dim = transform_ship_mode_dim(raw_data["ship_mode"])
    print("✅ Ship mode dimension transformed.")
    
    print("⏳ Transforming order priority dimension...")
    order_priority_dim = transform_order_priority_dim(raw_data["order_priority"])
    print("✅ Order priority dimension transformed.")
    
    print("⏳ Transforming order dimension...")
    order_dim = transform_order_dim(raw_data["order"])
    print("✅ Order dimension transformed.")
    
    print("⏳ Transforming product dimension...")
    product_dim = transform_product_dim(
        raw_data["product"], raw_data["subcategory"], raw_data["category"]
    )
    print("✅ Product dimension transformed.")
    
    print("⏳ Transforming fact sales...")
    fact_sales = transform_fact_sales(
        raw_data["order_details"],
        raw_data["order"],
        order_dim,
        date_dim,
        customer_dim,
        product_dim,
        ship_mode_dim,
        order_priority_dim
    )
    print("✅ Fact sales transformed.")

    return {
        "dim_date": date_dim,
        "dim_customer": customer_dim,
        "dim_ship_mode": ship_mode_dim,
        "dim_order_priority": order_priority_dim,
        "dim_order": order_dim,
        "dim_product": product_dim,
        "fact_sales": fact_sales
    }