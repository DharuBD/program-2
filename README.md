import requests
import pandas as pd 

# Try different encodings to load employee data
try:
    data = pd.read_csv(r"C:\Program Files\Employee Sample Data 1.csv", encoding='ISO-8859-1')  # Common alternative encodings
except UnicodeDecodeError:
    print("UnicodeDecodeError: Trying a different encoding...")
    try:
        data = pd.read_csv(r"C:\Program Files\Employee Sample Data 1.csv", encoding='utf-16')  # Another common encoding
    except Exception as e:
        print(f"Error loading the CSV: {e}")
        raise  # Re-raise the exception if unable to load

# Define Solr server URL
SOLR_URL = "http://localhost:8983/solr"

# Function to create a Solr collection
def createCollection(p_collection_name):
    url = f"{SOLR_URL}/admin/collections?action=CREATE&name={p_collection_name}&numShards=1&replicationFactor=1"
    response = requests.get(url)
    return response.json()

# Function to index data, excluding a specified column
def indexData(p_collection_name, p_exclude_column):
    headers = {"Content-Type": "application/json"}
    collection_data = data.drop(columns=[p_exclude_column]).to_dict(orient='records')
    url = f"{SOLR_URL}/{p_collection_name}/update?commit=true"
    response = requests.post(url, json=collection_data, headers=headers)
    return response.json()

# Function to search by column
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    url = f"{SOLR_URL}/{p_collection_name}/select?q={p_column_name}:{p_column_value}"
    response = requests.get(url)
    return response.json()

# Function to get employee count
def getEmpCount(p_collection_name):
    url = f"{SOLR_URL}/{p_collection_name}/select?q=*:*&rows=0"
    response = requests.get(url)
    return response.json()["response"]["numFound"]

# Function to delete employee by ID
def delEmpById(p_collection_name, p_employee_id):
    url = f"{SOLR_URL}/{p_collection_name}/update?commit=true"
    headers = {"Content-Type": "application/json"}
    delete_query = {"delete": {"id": p_employee_id}}
    response = requests.post(url, json=delete_query, headers=headers)
    return response.json()

# Function to get department facet
def getDepFacet(p_collection_name):
    url = f"{SOLR_URL}/{p_collection_name}/select?q=*:*&rows=0&facet=true&facet.field=Department"
    response = requests.get(url)
    return response.json()["facet_counts"]["facet_fields"]["Department"]

# Create collections
v_nameCollection = "Hash_Dharani"
v_phoneCollection = "Hash_2057"
createCollection(v_nameCollection)
createCollection(v_phoneCollection)

# Execute function calls in specified order
print("Employee count in name collection:", getEmpCount(v_nameCollection))
indexData(v_nameCollection, "Department")
indexData(v_phoneCollection, "Gender")
print("Employee count in name collection after indexing:", getEmpCount(v_nameCollection))
delEmpById(v_nameCollection, "E02003")
print("Employee count in name collection after deletion:", getEmpCount(v_nameCollection))
print("Search by Department in name collection (IT):", searchByColumn(v_nameCollection, "Department", "IT"))
print("Search by Gender in name collection (Male):", searchByColumn(v_nameCollection, "Gender", "Male"))
print("Search by Department in phone collection (IT):", searchByColumn(v_phoneCollection, "Department", "IT"))
print("Department facet in name collection:", getDepFacet(v_nameCollection))
print("Department facet in phone collection:", getDepFacet(v_phoneCollection))
