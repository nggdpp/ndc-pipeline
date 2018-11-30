from pynggdpp import processing as ndcProcessing
from pynggdpp import collection as ndcCollection
from datetime import datetime

ndcProcessing.set_env_variables('docker.env')

# Setup connection to mongodb on the DataDistillery platform
mongo_client_datadistillery = ndcProcessing.mongodb_client()
ndc_collections = mongo_client_datadistillery['ndc_collections']

doc_load = list()

for collection in ndcCollection.ndc_get_collections():
    logged_collection = dict()
    logged_collection['_id'] = collection['id']
    logged_collection['Source Metadata'] = collection
    logged_collection["collection_meta"] = ndcCollection.collection_metadata_summary(collection)
    logged_collection['Logging Metadata'] = dict()

    logged_collection['Logging Metadata']['Date Logged'] = datetime.utcnow().isoformat()
    logged_collection['Logging Metadata']['Number Processable WAFs'] = 0
    logged_collection['Logging Metadata']['Number Processable Files'] = 0

    print(logged_collection['Source Metadata']['title'], collection["id"])

    doc_load.append(logged_collection)

print(len(doc_load))

ndc_collections.insert_many(doc_load)


