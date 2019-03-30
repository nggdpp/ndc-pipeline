from pynggdpp import ndc
import json


def process_cache(event=None, context=None):
    queued_collection = ndc.get_message("queued_collections")

    if queued_collection is None:
        response = {
            "statusCode": 204
        }
        return response
    else:
        body = dict()
        index_name = f'ndc_collection_{queued_collection["Body"]["collection_meta"]["ndc_collection_id"]}'
        op_dict = {
            "index": {
                "_index": index_name,
                "_type": "ndc_collection_item"
            }
        }
        bulk_data = list()
        collection_records = list()
        s3_files_to_delete = list()

        if "queued_files" in queued_collection["Body"].keys():
            for file in queued_collection["Body"]["queued_files"]:
                if not ndc.check_s3_file(file["key_name"], "ndc-collection-files"):
                    body["DLQ Collections Response"] = ndc.post_message("dlq_collections",
                                                    queued_collection["Body"]["collection_meta"]["ndc_collection_id"],
                                                    queued_collection["Body"])
                    body["Delete Message Response"] = ndc.delete_message("queued_collections",
                                                                         queued_collection["ReceiptHandle"])
                    response = {
                        "statusCode": 200,
                        "body": json.dumps(body)
                    }
                    return response

                if file["contentType"] == "application/xml":
                    processed_file = ndc.nggdpp_xml_to_recordset(file["key_name"],
                                                                queued_collection["Body"]["collection_meta"])
                else:
                    processed_file = ndc.nggdpp_text_to_recordset(file["key_name"],
                                                                  queued_collection["Body"]["collection_meta"])

                if processed_file["errors"] is not None:
                    body["File Processing Errors"] = processed_file["errors"]

                s3_files_to_delete.append(file["key_name"])
                collection_records = collection_records + processed_file["recordset"]

        if "queued_waf" in queued_collection["Body"].keys():
            records_from_metadata = list()
            for waf_package in queued_collection["Body"]["queued_waf"]:
                for link_package in waf_package["waf_listing"]:
                    records_from_metadata.append(
                        ndc.feature_from_metadata(queued_collection["Body"]["collection_meta"], link_package)
                    )
                    s3_files_to_delete.append(link_package["key_name"])
            collection_records = collection_records + records_from_metadata

        for record_chunk in [collection_records[i:i + 10000] for i in range(0, len(collection_records), 10000)]:
            this_list = list()
            for item in record_chunk:
                this_list.append(op_dict)
                this_list.append(item)
            bulk_data.append(this_list)

        body["Index Name"] = index_name
        body["Number Items"] = len(collection_records)

        if len(bulk_data) > 0:
            body["Create Index Response"] = ndc.create_es_index(index_name)
            body["Index Mapping Response"] = ndc.map_index(index_name)
            for data_chunk in bulk_data:
                ndc.bulk_build_es_index(index_name, data_chunk)
            body["Delete Message Response"] = ndc.delete_message("queued_collections", queued_collection["ReceiptHandle"])
            for file_key in s3_files_to_delete:
                ndc.remove_s3_object(file_key)

        response = {
            "statusCode": 200,
            "body": json.dumps(body)
        }
        return response

