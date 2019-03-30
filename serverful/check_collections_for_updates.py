'''
This script works through all of the raw collection records dumped out of ScienceBase, figures out if anything has
changed and takes appropriate action. It keeps track of what it's doing by writing a property back into the raw
collection records it has processed. It's essentially like processing a message out of a queue, but I come back to
the collection record for files and webLinks from other processes. This script will move previous versions of
updated collections to another MongoDB collection (to decide how to deal with later) and creates new homogenized and
simplified collection records for exposure via the API.
'''

import pynggdpp.sciencebase
import pynggdpp.serverful
import pynggdpp.item_process

sb_collections = pynggdpp.sciencebase.Collections()
serverful_infrastructure = pynggdpp.serverful.Infrastructure()

ndc_collections_db = serverful_infrastructure.connect_mongodb(collection="ndc_collections")
ndc_collections_raw_db = serverful_infrastructure.connect_mongodb(collection="ndc_collections_raw")
ndc_collection_versions_db = serverful_infrastructure.connect_mongodb(collection="ndc_collection_versions")


collection_to_process = ndc_collections_raw_db.find_one({
    "processed_collection": {"$exists": False}
})


while collection_to_process is not None:
    insert_new_collection_record = False

    existing_collection = ndc_collections_db.find_one(
        {
            "ndc_collection_id": collection_to_process["id"]
        }
    )

    if existing_collection is not None:
        collection_last_updated = next((d["dateString"] for d in
                                        collection_to_process["dates"] if d["type"] == "lastUpdated"), None)

        if collection_last_updated != existing_collection["ndc_collection_last_updated"]:
            ndc_collection_versions_db.insert_one(existing_collection)
            ndc_collections_db.delete_one({"_id": existing_collection["_id"]})
            insert_new_collection_record = True
            print(f'Updated Collection: {existing_collection["ndc_collection_title"]}')

    else:
        insert_new_collection_record = True

    if insert_new_collection_record:
        collection_meta = sb_collections.ndc_collection_meta(
            collection_record=collection_to_process
        )
        ndc_collections_db.insert_one(collection_meta)
        print(f'Inserted Collection: {collection_meta["ndc_collection_title"]}')

    ndc_collections_raw_db.update_one(
        {"_id": collection_to_process["_id"]},
        {
            "$set": {"processed_collection": True}
        }
    )

    collection_to_process = ndc_collections_raw_db.find_one({
        "processed_collection": {"$exists": False}
    })

