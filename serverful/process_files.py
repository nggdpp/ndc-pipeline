'''
This script works through the uniform file objects stashed for either actionable ScienceBase files or WAF-harvestable
files. The type and source of file triggers the appropriate function to build it into a record or records and then
pushes the records into the appropriate collection. Right now, this operates in a serial manner. The serverless approach
with Lambda and file objects presented from an SQS queue is much more efficient as the process logically breaks up into
individual file processing anyway. If we end up needing to deploy the serverful solution, I'll come back and make this a
parallel approach to max out what the processing machine can afford.
'''

import pynggdpp.serverful
import pynggdpp.item_process

from pprint import pprint

serverful_infrastructure = pynggdpp.serverful.Infrastructure()
file_processor = pynggdpp.item_process.Files()

ndc_files_db = serverful_infrastructure.connect_mongodb(collection="ndc_files")

for index, file_record in enumerate(ndc_files_db.find(
        {"processing_metadata": {"$exists": False}}
    )
):
    ndc_file_record_id = file_record["_id"]
    del file_record["_id"]

    if file_record["ndc_harvest_source"] == "ScienceBase":
        if file_record["ndc_file_content_type"] == "application/xml":
            file_data = file_processor.clean_dict_from_nggdpp_xml(file_record)
        else:
            file_data = file_processor.clean_dict_from_csv(file_record)
    else:
        file_data = file_processor.ndc_item_from_metadata(file_record)

    ndc_files_db.update_one(
        {"_id": ndc_file_record_id},
        {"$set": {"processing_metadata": file_data["processing_metadata"]}}
    )
    pprint(file_data["processing_metadata"])

    if len(file_data["recordset"]) > 0:
        collection_collection = serverful_infrastructure.connect_mongodb(
            collection=file_record["ndc_collection_id"]
        )
        collection_collection.insert_many(file_data["recordset"])
        print(f"Inserted {len(file_data['recordset'])} records into collection")

