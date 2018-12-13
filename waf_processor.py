from pynggdpp import processing as ndcProcessing
from pynggdpp import collection as ndcCollection
import requests
from pprint import pprint

ndcProcessing.set_env_variables('docker.env')

# Setup connection to mongodb on the DataDistillery platform
mongo_client_datadistillery = ndcProcessing.mongodb_client()
ndc_collections = mongo_client_datadistillery['ndc_collections']

for index, collection in enumerate(ndc_collections.find({"WAFs": {"$exists": True}})):
    thisCollection = mongo_client_datadistillery[collection["collection_id"]]

    for waf in collection["WAFs"]:
        for i, link_meta in enumerate(waf["url_list"]):
            existing_record = thisCollection.find_one({"properties.ndc_meta.file_url": link_meta["file_url"]})
            if existing_record is None:
                collection_meta = collection["collection_meta"]
                collection_meta["collection_id"] = collection["collection_id"]
                thisFeature = ndcProcessing.feature_from_metadata(collection_meta, link_meta)
                if isinstance(thisFeature, dict):
                    thisCollection.insert_one(thisFeature)
                    print(collection["collection_id"], thisFeature["properties"]["title"])
                else:
                    print(collection["collection_id"], thisFeature)
            else:
                print(collection["collection_id"], link_meta["file_url"])
