from pynggdpp import processing as ndcProcessing
from datetime import datetime
from pprint import pprint

ndcProcessing.set_env_variables('docker.env')

# Setup connection to mongodb on the DataDistillery platform
mongo_client_datadistillery = ndcProcessing.mongodb_client()
ndc_collections = mongo_client_datadistillery['ndc_collections']
ndc_weblinks = mongo_client_datadistillery['ndc_weblinks']

for collection in ndc_collections.find({"Source Metadata.webLinks": {"$exists": True}},
                                       {"Source Metadata.webLinks": 1}):

    if ndc_weblinks.find_one({"collection_id": collection["_id"]}) is None:
        weblink_container = dict()
        weblink_container["collection_id"] = collection["_id"]
        weblink_container["Container Date"] = datetime.utcnow().isoformat()
        weblink_container["Collection Links"] = ndcProcessing.link_meta(collection["_id"],
                                                                     collection["Source Metadata"]["webLinks"])

        ndc_weblinks.insert_one(weblink_container)
        pprint(weblink_container)

