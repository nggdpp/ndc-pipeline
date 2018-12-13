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
    logged_collection['collection_id'] = collection['id']
    logged_collection['date_logged'] = datetime.utcnow().isoformat()
    logged_collection['source_meta'] = collection
    logged_collection["collection_meta"] = ndcCollection.collection_metadata_summary(collection)

    doc_load.append(logged_collection)

    print(logged_collection['source_meta']['title'], collection["id"])

print(len(doc_load))

ndc_collections.insert_many(doc_load)


