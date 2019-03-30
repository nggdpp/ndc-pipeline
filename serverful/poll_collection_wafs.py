'''
This script comes back to the raw collection items to get WAF links for processing. It takes advantage of MongoDB's
aggregation methods to tease out the webLink types indicated as WAFs, uses a pynggdpp function to parse the WAF landing
page for its listed URLs, and then build the uniform file objects with infused collection and file metadata that we
store for processing along with ScienceBase files.

Eventually, we'll need to build in some other types of link processing for other routes to collections (e.g., APIs,
web services, etc.).
'''

import pynggdpp.sciencebase
import pynggdpp.serverful
import pynggdpp.item_process

sb_files = pynggdpp.sciencebase.Files()
serverful_infrastructure = pynggdpp.serverful.Infrastructure()
link_processor = pynggdpp.item_process.Links()

bis_db = serverful_infrastructure.connect_mongodb()

ndc_collections_raw_db = serverful_infrastructure.connect_mongodb(collection="ndc_collections")
ndc_collections_db = serverful_infrastructure.connect_mongodb(collection="ndc_collections")
ndc_files_db = serverful_infrastructure.connect_mongodb(collection="ndc_files")

current_files = list(ndc_files_db.find(
    {"ndc_harvest_source": {"$ne": "ScienceBase"}},
    {"ndc_collection_id": 1, "ndc_file_url": 1, "ndc_file_date": 1}
))

def check_existing_file(file_meta):
    return next((
        f for f in current_files if f["ndc_collection_id"] == file_meta["ndc_collection_id"]
                   and f["ndc_file_url"] == file_meta["ndc_file_url"]
                   and f["ndc_file_date"] == file_meta["ndc_file_date"]
    ), None)

link_pipeline = [
    {"$match":
        {
            "$and": [
                {"processed_links": {"$exists": False}},
                {"webLinks": {"$exists": True}}
            ]
        }
    },
    {"$project":
        {
            "id": 1,
            "webLinks": 1
        }
    },
    {"$unwind": "$webLinks"},
    {"$match":
        {
            "webLinks.type": "WAF"
        }
    },
    {"$project":
        {
            "_id": 0,
            "ndc_collection_id": "$id",
            "ndc_harvest_source": "$webLinks.uri",
        }
    }
]

for index, link_record in enumerate(bis_db.ndc_collections_raw.aggregate(link_pipeline)):
    if link_record["ndc_harvest_source"].split(":") != "ftp":
        parsed_waf = link_processor.parse_waf(link_record["ndc_harvest_source"])
        if parsed_waf is not None:
            for file_record in parsed_waf["url_list"]:
                file_record.update(link_record)
                if check_existing_file(file_record) is None:
                    collection_meta = ndc_collections_db.find_one(
                        {"ndc_collection_id": file_record["ndc_collection_id"]},
                        {"_id": 0, "ndc_collection_id": 0}
                    )
                    file_record.update(collection_meta)
                    ndc_files_db.insert_one(file_record)
                    print(f"File Record Inserted: {file_record['ndc_file_name']}")

        ndc_collections_raw_db.update_one(
            {"id": file_record["ndc_collection_id"]},
            {"$set": {"processed_files": True}}
        )
