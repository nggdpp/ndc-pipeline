'''
This script comes back to the raw collection items to get files for processing. It takes advantage of MongoDB's
aggregation methods to tease out the files that we can process at this time and build the stub of the uniform file
objects that we then infuse with collection and file metadata and store for processing.
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


def check_existing_file(file_meta):
    return ndc_files_db.find_one({
        "$and": [
            {
                "ndc_collection_id": file_meta["ndc_collection_id"]
            },
            {
                "ndc_file_url": file_meta["ndc_file_url"]
            },
            {
                "ndc_file_date": file_meta["ndc_file_date"]
            }
        ]
    })

files_pipeline = [
    {"$match":
        {
            "$and": [
                {"processed_files": {"$exists": False}},
                {"files": {"$exists": True}}
            ]
        }
    },
    {"$project":
        {
            "id": 1,
            "files": 1
        }
    },
    {"$unwind": "$files"},
    {"$match":
        {
            "$and": [
                {"files.processed": True},
                {"files.name": {"$ne": "metadata.xml"}},
                {"files.contentType": {"$ne": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}}
            ]
        }
    },
    {"$project":
        {
            "_id": 0,
            "ndc_collection_id": "$id",
            "ndc_file_name": "$files.name",
            "ndc_file_url": "$files.url",
            "ndc_file_size": "$files.size",
            "ndc_file_date": "$files.dateUploaded",
            "ndc_file_content_type": "$files.contentType",
            "ndc_harvest_source": "ScienceBase"
        }
    }
]

for file_record in bis_db.ndc_collections_raw.aggregate(files_pipeline):
    if check_existing_file(file_record) is None:
        collection_meta = ndc_collections_db.find_one(
            {"ndc_collection_id": file_record["ndc_collection_id"]},
            {"_id": 0, "ndc_collection_id": 0}
        )
        file_record.update(collection_meta)
        ndc_files_db.insert_one(file_record)
        ndc_collections_raw_db.update_one(
            {"id": file_record["ndc_collection_id"]},
            {"$set": {"processed_files": True}}
        )
        print(file_record["ndc_file_name"])
