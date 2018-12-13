from pynggdpp import processing as ndcProcessing
from pprint import pprint

ndcProcessing.set_env_variables('docker.env')

# Setup connection to mongodb on the DataDistillery platform
mongo_client_datadistillery = ndcProcessing.mongodb_client()
ndc_collections = mongo_client_datadistillery['ndc_collections']
ndc_files = mongo_client_datadistillery['ndc_files']
process_log = mongo_client_datadistillery['process_log']

pipeline = [
    {
        "$unwind": {
            "path": "$files"
        }
    },
    {
        "$match": {
            "$and": [
                {
                    "files.process_meta.Processable Route": True
                },
                {
                    "processable_files": 1.0
                }
            ]
        }
    }
]

num_processed = 0
num_to_process = 200

for index, collection in enumerate(ndc_files.aggregate(pipeline)):
    if num_processed == num_to_process:
        break

    log_entry = process_log.find_one({"collection_id": collection["collection_id"]})

    if log_entry is None:
        print(collection["collection_id"])

        process_errors = list()

        file_meta = collection["files"]

        if file_meta["response_meta"]["Content-Type"] == "application/xml":
            source_data = ndcProcessing.nggdpp_xml_to_dicts(file_meta)
        else:
            source_data = ndcProcessing.nggdpp_text_to_dicts(file_meta)

        if isinstance(source_data, list):
            processed_data = ndcProcessing.nggdpp_record_list_to_geojson(source_data, file_meta)

            if "Feature Collection" in processed_data.keys():
                db_collection = mongo_client_datadistillery[collection['collection_id']]
                db_collection.insert_many(processed_data["Feature Collection"]["features"])

            logging_metadata = processed_data["processing_meta"]

        else:
            process_errors.append(source_data)
            logging_metadata = file_meta

        process_log_entry = ndcProcessing.process_log_entry(collection["collection_id"],
                                                            logging_metadata,
                                                            process_errors)
        num_processed += 1

        process_log.insert_one(process_log_entry)
        pprint(process_log_entry)
        print("==================================================")

