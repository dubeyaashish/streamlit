from pymongo import MongoClient
import pandas as pd
from bson.objectid import ObjectId
import json
from datetime import datetime

# Custom JSON encoder to handle MongoDB specific types
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, ObjectId):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

# Connect to MongoDB
def connect_to_mongodb(connection_string, db_name):
    """
    Connect to MongoDB using the provided connection string and return the database
    """
    client = MongoClient(connection_string)
    db = client[db_name]
    return db, client

def get_flattened_job_data(db, limit=10):
    """
    Retrieve flattened job data with referenced collections joined up to 2 levels deep
    
    Parameters:
    - db: MongoDB database connection
    - limit: Limit the number of results (default 10)
    
    Returns:
    - List of flattened job documents
    """
    # Main aggregation pipeline
    pipeline = [
        # Limit to specified number of documents
        {"$limit": limit}
    ]
    
    try:
        # Get the initial job documents
        jobs = list(db.Job.aggregate(pipeline))
        
        # Process first level references
        for job in jobs:
            # Process each ID field in the Job document
            for key, value in list(job.items()):
                # Skip non-ID fields and _id
                if not (key.endswith('ID') and key != '_id') and not key.endswith('IDs'):
                    continue
                
                # Determine related collection name
                if key.endswith('IDs'):  # Array of IDs
                    base_name = key[:-3]  # Remove 'IDs'
                    is_array = True
                else:  # Single ID
                    base_name = key[:-2]  # Remove 'ID'
                    is_array = False
                
                # Capitalize first letter for collection name
                collection_name = base_name[0].upper() + base_name[1:]
                
                # Check if related collection exists
                if collection_name not in db.list_collection_names():
                    continue
                
                # Fetch the related document(s) - Level 1
                if is_array:
                    if not isinstance(value, list):
                        continue
                    level1_docs = list(db[collection_name].find({"_id": {"$in": value}}))
                    field_name = base_name.lower() + "s"
                    job[field_name] = level1_docs
                else:
                    if not isinstance(value, ObjectId):
                        continue
                    level1_doc = db[collection_name].find_one({"_id": value})
                    if level1_doc:
                        field_name = base_name.lower()
                        job[field_name] = level1_doc
                        
                        # Process Level 2 references for this Level 1 document
                        for l1_key, l1_value in list(level1_doc.items()):
                            # Skip non-ID fields and _id
                            if not (l1_key.endswith('ID') and l1_key != '_id') and not l1_key.endswith('IDs'):
                                continue
                            
                            # Determine Level 2 collection name
                            if l1_key.endswith('IDs'):  # Array of IDs
                                l2_base_name = l1_key[:-3]  # Remove 'IDs'
                                l2_is_array = True
                            else:  # Single ID
                                l2_base_name = l1_key[:-2]  # Remove 'ID'
                                l2_is_array = False
                            
                            # Capitalize first letter for collection name
                            l2_collection_name = l2_base_name[0].upper() + l2_base_name[1:]
                            
                            # Check if related collection exists
                            if l2_collection_name not in db.list_collection_names():
                                continue
                            
                            # Fetch the related document(s) - Level 2
                            if l2_is_array:
                                if not isinstance(l1_value, list):
                                    continue
                                level2_docs = list(db[l2_collection_name].find({"_id": {"$in": l1_value}}))
                                l2_field_name = l2_base_name.lower() + "s"
                                level1_doc[l2_field_name] = level2_docs
                            else:
                                if not isinstance(l1_value, ObjectId):
                                    continue
                                level2_doc = db[l2_collection_name].find_one({"_id": l1_value})
                                if level2_doc:
                                    l2_field_name = l2_base_name.lower()
                                    level1_doc[l2_field_name] = level2_doc
        
        return jobs
    except Exception as e:
        print(f"Error executing aggregation: {e}")
        return []
    
    try:
        # Execute the aggregation pipeline
        results = list(db.Job.aggregate(pipeline))
        return results
    except Exception as e:
        print(f"Error executing aggregation: {e}")
        return []

def flatten_nested_document(doc, prefix='', result=None):
    """
    Recursively flatten a nested document into a single-level dictionary
    with keys representing the path to each value
    
    Parameters:
    - doc: The document to flatten
    - prefix: Prefix for keys in the flattened result
    - result: The resulting flattened dictionary
    
    Returns:
    - Flattened dictionary
    """
    if result is None:
        result = {}
    
    if not isinstance(doc, dict):
        return result
    
    for key, value in doc.items():
        new_key = f"{prefix}_{key}" if prefix else key
        
        # Handle different types of values
        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            flatten_nested_document(value, new_key, result)
        elif isinstance(value, list):
            # For lists, include the length and flatten any dictionaries inside
            result[new_key + "_count"] = len(value)
            
            # If it's a list of dictionaries, flatten the first few items as examples
            if value and isinstance(value[0], dict):
                for i, item in enumerate(value[:3]):  # Limit to first 3 items
                    flatten_nested_document(item, f"{new_key}_{i}", result)
        else:
            # For simple values, just add them directly
            result[new_key] = value
    
    return result

def simplify_object(obj):
    """
    Simplify a MongoDB document for display by removing complex nested objects
    """
    if isinstance(obj, dict):
        simplified = {}
        for key, value in obj.items():
            # Skip complex objects and arrays for display purposes
            if key == "_id":
                simplified[key] = str(value)
            elif isinstance(value, dict):
                simplified[key] = "Object: " + ", ".join(value.keys())
            elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                simplified[key] = f"Array of {len(value)} objects"
            else:
                simplified[key] = value
        return simplified
    return obj

def main():
    # Use the provided MongoDB connection string
    connection_string = "mongodb://foxbith-dev:bcNyxh5caFazjTRg@atlas-sql-660bc9484af8305e6a15eb69-nqfeq.a.query.mongodb.net/prod?ssl=true&authSource=admin"
    db_name = "prod"
    
    try:
        # Connect to MongoDB
        db, client = connect_to_mongodb(connection_string, db_name)
        print("Successfully connected to MongoDB")
        
        # List available collections
        collections = db.list_collection_names()
        print(f"Available collections: {collections}")
        
        # Check if Job collection exists
        if "Job" in collections:
            # Get a sample to verify structure
            sample_doc = db.Job.find_one()
            print("\nSample Job document fields:")
            for key in sample_doc.keys():
                print(f"- {key}: {type(sample_doc[key]).__name__}")
            
            # Get flattened job data (10 records) with relationships resolved up to 2 levels deep
            print("\nRetrieving flattened job data (2 levels deep)...")
            flattened_jobs = get_flattened_job_data(db, limit=10)
            print(f"Retrieved {len(flattened_jobs)} flattened job records")
            
            # Display the flattened data for verification
            for i, job in enumerate(flattened_jobs):
                if i >= 3:  # Just show details for the first 3 jobs to keep output manageable
                    break
                    
                print(f"\n--- Job {i+1} ---")
                
                # Print key fields for quick verification
                print(f"Job No: {job.get('no', 'N/A')}")
                print(f"Status: {job.get('status', 'N/A')}")
                
                # Print summary of first-level relationships
                print("\nLevel 1 relationships:")
                level1_relations = []
                for key in job.keys():
                    if key not in ['_id', 'no', 'status', 'createdAt', 'updatedAt', 'type', 'priority',
                                  'appointmentTime', 'isManualFindTechnician', 'isSendRequest', 'isEditable',
                                  'isQcJob', 'isReview', 'isSlaInRisk', 'isSlaInFail', 'pauseTime', 'numOfHourSla',
                                  'customerContact'] and not key.endswith('ID') and not key.endswith('IDs'):
                        level1_relations.append(key)
                
                for rel in level1_relations:
                    rel_obj = job.get(rel)
                    if isinstance(rel_obj, dict):
                        print(f"- {rel}: Object with {len(rel_obj.keys())} fields")
                        # Sample a few fields from this related object
                        sample_fields = [k for k in rel_obj.keys() if k not in ['_id'] and not k.endswith('ID') and not k.endswith('IDs')][:3]
                        for field in sample_fields:
                            print(f"  * {field}: {rel_obj.get(field)}")
                    elif isinstance(rel_obj, list):
                        print(f"- {rel}: Array with {len(rel_obj)} items")
                
                # Check for level 2 relationships
                print("\nLevel 2 relationships (sample):")
                level2_count = 0
                for rel in level1_relations:
                    rel_obj = job.get(rel)
                    if isinstance(rel_obj, dict):
                        level2_rels = []
                        for k, v in rel_obj.items():
                            if not k.endswith('ID') and not k.endswith('IDs') and (isinstance(v, dict) or (isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict))):
                                level2_rels.append(k)
                        
                        if level2_rels:
                            print(f"- {rel} contains: {', '.join(level2_rels)}")
                            level2_count += len(level2_rels)
                
                if level2_count == 0:
                    print("  No level 2 relationships found")
            
            # Flatten and export to CSV
            print("\nPreparing flattened data for export...")
            flattened_data = [flatten_nested_document(job) for job in flattened_jobs]
            
            df = pd.DataFrame(flattened_data)
            csv_filename = "flattened_jobs.csv"
            df.to_csv(csv_filename, index=False)
            print(f"Exported {len(df.columns)} columns of flattened data to {csv_filename}")
            print(f"Sample columns: {', '.join(list(df.columns)[:5])}")
            
            # Export as JSON if needed for debugging
            json_filename = "jobs_with_relationships.json"
            with open(json_filename, "w", encoding="utf-8") as f:
                json.dump(flattened_jobs, f, cls=MongoJSONEncoder, ensure_ascii=False, indent=2)
            print(f"Exported nested structure to {json_filename}")
            
        else:
            print("Job collection not found in the database")
        
        # Close the connection
        client.close()
        print("Connection closed")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()



    