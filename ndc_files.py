from pynggdpp import processing as ndcProcessing
from datetime import datetime
from pprint import pprint

ndcProcessing.set_env_variables('docker.env')

# Setup connection to mongodb on the DataDistillery platform
mongo_client_datadistillery = ndcProcessing.mongodb_client()
ndc_collections = mongo_client_datadistillery['ndc_collections']
ndc_files = mongo_client_datadistillery['ndc_files']

for collection in ndc_collections.find({"Source Metadata.files": {"$exists": True}}, {"Source Metadata.files": 1}):
    if ndc_files.find_one({"collection_id": collection["_id"]}) is None:
        file_container = dict()

        # Add context information to the container
        file_container["collection_id"] = collection["_id"]
        file_container["Container Date"] = datetime.utcnow().isoformat()

        # Process every file using the file_meta() function to return introspection information
        collection_files = ndcProcessing.file_meta(collection["_id"],
                                                                     collection["Source Metadata"]["files"])
        file_container["Collection Files"] = collection_files

        # Add the total number of processable file routes for later reference
        file_container["Processable File Number"] = len([cf for cf in collection_files if cf["Processable Route"]])

        ndc_files.insert_one(file_container)
        pprint(file_container)

