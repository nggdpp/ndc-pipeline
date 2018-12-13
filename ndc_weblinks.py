from pynggdpp import processing as ndcProcessing
from datetime import datetime
from pprint import pprint

ndcProcessing.set_env_variables('docker.env')

# Setup connection to mongodb on the DataDistillery platform
mongo_client_datadistillery = ndcProcessing.mongodb_client()
ndc_collections = mongo_client_datadistillery['ndc_collections']

pipeline = [
    {
        u"$match": {
            u"source_meta.webLinks": {
                u"$exists": True
            }
        }
    },
    {
        u"$unwind": {
            u"path": u"$source_meta.webLinks"
        }
    },
    {
        u"$match": {
            u"source_meta.webLinks.type": u"WAF"
        }
    },
    {
        u"$project": {
            u"collection_id": True,
            u"waf_link": u"$source_meta.webLinks"
        }
    },
    {
        u"$group": {
            u"_id": u"$collection_id",
            u"links": {
                u"$push": u"$waf_link"
            }
        }
    }
]

for waf_link_collection in ndc_collections.aggregate(pipeline):
    wafs = list()
    for link_meta in waf_link_collection["links"]:
        processed_waf = ndcProcessing.parse_waf(link_meta)
        if isinstance(processed_waf, dict):
            wafs.append(processed_waf)
        else:
            print(processed_waf)
    if len(wafs) > 0:
        ndc_collections.update_one({"collection_id": waf_link_collection["_id"]}, {"$set": {"WAFs": wafs}})
    print(waf_link_collection["_id"], len(wafs))
