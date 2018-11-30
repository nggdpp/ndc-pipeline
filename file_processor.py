from pynggdpp import processing as ndcProcessing
from pynggdpp import collection as ndcCollection
from pprint import pprint
import time

ndcProcessing.set_env_variables('docker.env')

# Setup connection to mongodb on the DataDistillery platform
mongo_client_datadistillery = ndcProcessing.mongodb_client()
ndc_collections = mongo_client_datadistillery['ndc_collections']
ndc_files = mongo_client_datadistillery['ndc_files']
process_log = mongo_client_datadistillery['process_log']

pipeline = [
    {
        "$unwind": {
            "path": "$Collection Files"
        }
    },
    {
        "$match": {
            "$and": [
                {
                    "Collection Files.Processable Route": True
                },
                {
                    "Processable File Number": 1.0
                }
            ]
        }
    }
]

for index, collection in enumerate(ndc_files.aggregate(pipeline)):
    log_entry = process_log.find_one({"collection_id": collection["_id"]})

    if log_entry is None:
        process_errors = list()
        processed_data = None
        records_success = 0
        records_errors = 0

        file_meta = collection["Collection Files"]

        # Retrieve a cache of the collection metadata and build summary to infuse into each record
        collection_cache = ndc_collections.find_one({"_id": collection["collection_id"]}, {"collection_meta": 1})

        start_time = time.clock()

        if file_meta["Content-Type"] == "application/xml":
            source_data = ndcProcessing.nggdpp_xml_to_dicts(file_meta)
            if isinstance(source_data, list) and isinstance(source_data[0], dict):
                processed_data = \
                    ndcProcessing.nggdpp_record_list_to_geojson(source_data, file_meta,
                                                                collection_cache["collection_meta"])
            else:
                this_error = dict()
                this_error["type"] = "fatal"
                this_error["message"] = "Problem in XML source data processing"
                this_error["function"] = "pynggdpp.processing.nggdpp_xml_to_dicts()"
                this_error["error_from_function"] = source_data
                process_errors.append(this_error)
        else:
            source_data = ndcProcessing.nggdpp_text_to_dicts(file_meta)
            if isinstance(source_data, list) and isinstance(source_data[0], dict):
                processed_data = \
                    ndcProcessing.nggdpp_record_list_to_geojson(source_data, file_meta,
                                                                collection_cache["collection_meta"])
            else:
                this_error = dict()
                this_error["type"] = "fatal"
                this_error["message"] = "Problem in TEXT source data processing"
                this_error["function"] = "pynggdpp.processing.nggdpp_text_to_dicts()"
                this_error["error_from_function"] = source_data
                process_errors.append(this_error)

        if processed_data is not None:
            if "Feature Collection" in processed_data.keys():
                db_collection = mongo_client_datadistillery[collection['collection_id']]
                try:
                    db_collection.insert_many(processed_data["Feature Collection"]["features"])
                except Exception as e:
                    this_error = dict()
                    this_error["type"] = "fatal"
                    this_error["message"] = "Failed to insert data into MongoDB"
                    this_error["exception"] = str(e)
                    process_errors.append(this_error)

                if len(processed_data["Feature Collection"]["features"]) != db_collection.find({}).count():
                    db_collection.drop()
                    this_error = dict()
                    this_error["type"] = "fatal"
                    this_error["message"] = "Insert process did not insert the number of records from the source"
                    process_errors.append(this_error)
                else:
                    records_success = processed_data["Processing Report"]["Number of Records"]
                    records_errors = processed_data["Processing Report"]["Number of Errors"]

            else:
                this_error = dict()
                this_error["type"] = "fatal"
                this_error["message"] = "Feature collection was not in the processed data"
                process_errors.append(this_error)

        if "Feature Collection" in file_meta.keys():
            del (file_meta["Feature Collection"])

        time_to_run = time.clock() - start_time

        process_log_entry = ndcProcessing. \
            process_log_entry(collection["collection_id"], file_meta, time_to_run, process_errors)

        process_log.insert_one(process_log_entry)
        pprint(process_log_entry)
        print("==================================================")

