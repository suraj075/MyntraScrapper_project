from src.cloud_io import MongoIO

def fetch_product_names_from_cloud():
    """
    Fetch list of product names from MongoDB
    
    Returns:
        list: List of product names available in the database
    """
    try:
        mongo_con = MongoIO()
        return mongo_con.get_product_names()
    except Exception as e:
        print(f"Error fetching product names: {e}")
        return []