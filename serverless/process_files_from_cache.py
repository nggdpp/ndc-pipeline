import pynggdpp.aws
import pynggdpp.item_process
import inspect
import json

def process_files_from_cache(event=None, context=None):
    aws_messaging = pynggdpp.aws.Messaging()
    source_queue = "cached_files"
    queued_item = aws_messaging.get_message(source_queue)

    if queued_item is None:
        response = {
            "statusCode": 204
        }
        return response
    else:
        body = dict()

        aws_storage = pynggdpp.aws.Storage()
        aws_search = pynggdpp.aws.Search()
        log = pynggdpp.item_process.Log()
        file_processor = pynggdpp.item_process.Files()

        file_collection_metadata = queued_item["Body"]

        processed_file_package = file_processor.source_file_to_recordset(
            file_collection_metadata=file_collection_metadata
        )

        if processed_file_package["success"]:
            body["file_processing_report"] = log.log_process_step(
                identifier=file_collection_metadata["file_metadata"]["aws_s3_key"],
                entry_type="successful_file_processing_report",
                log=processed_file_package["file_report"],
                source_file=inspect.stack()[0][1].split("/")[-1],
                source_function=inspect.stack()[0][3],
            )
            body["indexing_response"] = aws_search.bulk_build_es_index(
                index_name=file_collection_metadata["collection_metadata"]["ndc_collection_id"],
                doc_type="ndc_collection_item",
                bulk_data=processed_file_package["recordset"]
            )
            body["file_deletion_response"] = \
                aws_storage.remove_s3_object(file_collection_metadata["file_metadata"]["aws_s3_key"])
        else:
            body["file_processing_report"] = log.log_process_step(
                identifier=file_collection_metadata["file_metadata"]["aws_s3_key"],
                entry_type="failed_file_processing_report",
                log=processed_file_package,
                source_file=inspect.stack()[0][1].split("/")[-1],
                source_function=inspect.stack()[0][3],
                message_queue_packet={
                    "message_queue": "failed_file_processing",
                    "identifier": file_collection_metadata["file_metadata"]["aws_s3_key"]
                }
            )

        body["message_deletion_response"] = \
            aws_messaging.delete_message(source_queue, queued_item["ReceiptHandle"])

        response = {
            "statusCode": 200,
            "body": json.dumps(body)
        }
        return response


if __name__ == "__main__":
    from pprint import pprint
    r = process_files_from_cache()
    pprint(json.loads(r["body"]))

