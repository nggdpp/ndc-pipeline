import pynggdpp.aws
import pynggdpp.item_process
import pynggdpp.rest_api
import inspect
import json


def cache_files(event=None, context=None):
    '''
    This function picks up a uniform file metadata message from the files_to_cache queue, retrieves the file (in
    memory), and loads the file to an S3 bucket. This gets all source data together onto disc for processing,
    alleviating some problems encountered with larger files, and dealing with sometimes flaky source repositories.

    :param event:
    :param context:
    :return:
    '''
    aws_messaging = pynggdpp.aws.Messaging()
    source_queue = "files_to_cache"
    queued_item = aws_messaging.get_message(source_queue)

    if queued_item is None:
        response = {
            "statusCode": 204
        }
        return response
    else:
        aws_storage = pynggdpp.aws.Storage()
        rest_search = pynggdpp.rest_api.Search()
        log = pynggdpp.item_process.Log()

        file_metadata = queued_item["Body"]
        collection_item = rest_search.query_collections_by_id(file_metadata["ndc_collection_id"],
                                                        filter_path='hits.hits._source.collection_metadata')
        collection_metadata = collection_item["hits"]["hits"][0]["_source"]["collection_metadata"]

        body = {"ndc_collection_id": file_metadata["ndc_collection_id"],
                "File Transfer Response": aws_storage.transfer_file_to_s3(
                    file_metadata["file_url"],
                    bucket_name='ndc-file-cache',
                    key_name=file_metadata['aws_s3_key']
                ),
                "Processing Log Response": log.log_process_step(
                    identifier=file_metadata["aws_s3_key"],
                    entry_type="cached_source_file",
                    log={
                        "file_metadata": file_metadata,
                        "collection_metadata": collection_metadata
                    },
                    source_file=inspect.stack()[0][1].split("/")[-1],
                    source_function=inspect.stack()[0][3],
                    message_queue_packet={
                        "message_queue": "cached_files",
                        "identifier": file_metadata["aws_s3_key"]
                    }
                ),
                "Deleted Message Response": aws_messaging.delete_message(source_queue, queued_item["ReceiptHandle"])
                }

        response = {
            "statusCode": 200,
            "body": json.dumps(body)
        }
        return response


if __name__ == "__main__":
    from pprint import pprint

    r = cache_files()
    pprint(json.loads(r["body"]))


