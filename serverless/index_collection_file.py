import pynggdpp.aws
import pynggdpp.item_process
import json
import inspect


def index_collection_file(event=None, context=None):
    aws_messaging = pynggdpp.aws.Messaging()
    source_queue = "files_to_index"
    queued_item = aws_messaging.get_message(source_queue)

    if queued_item is None:
        response = {
            "statusCode": 204
        }
        return response
    else:
        file_processor = pynggdpp.item_process.Files()
        aws_search = pynggdpp.aws.Search()
        log = pynggdpp.item_process.Log()

        body = {
            "Process Log": list()
        }

        file_object = queued_item["Body"]

        if file_object["ndc_content_type"] == "application/xml":
            if file_object["ndc_harvest_source"] == "ScienceBase":
                index_data = file_processor.clean_dict_from_nggdpp_xml(file_object)
                doIndex = True
            elif file_object["ndc_harvest_source"] == "waf":
                index_data = file_processor.ndc_item_from_metadata(file_object)
                doIndex = True
        else:
            index_data = file_processor.clean_dict_from_csv(file_object)
            doIndex = True

        if doIndex:
            if len(index_data["recordset"]) == 0:
                body["Process Log"].append(
                    log.log_process_step(
                        identifier=f'{file_object["ndc_collection_id"]}:{file_object["ndc_file_url"]}',
                        entry_type="recordset_build_failure",
                        log={
                            "file_object": file_object,
                            "processing_metadata": index_data["processing_metadata"]
                        },
                        source_file=inspect.stack()[0][1].split("/")[-1],
                        source_function=inspect.stack()[0][3],
                        message_queue_packet={
                            "message_queue": "recordset_build_failure",
                            "identifier": f'{file_object["ndc_collection_id"]}:{file_object["ndc_file_url"]}'
                        }
                    )
                )
            else:
                body["Process Log"].append(
                    log.log_process_step(
                        identifier=f'{file_object["ndc_collection_id"]}:{file_object["ndc_file_url"]}',
                        entry_type="indexed_source_url",
                        log=index_data["processing_metadata"],
                        source_file=inspect.stack()[0][1].split("/")[-1],
                        source_function=inspect.stack()[0][3],
                    )
                )

                # Index the recordset
                aws_search.bulk_build_es_index(
                    file_object["ndc_collection_id"],
                    "ndc_collection_item",
                    index_data["recordset"]
                )

            aws_messaging.delete_message(source_queue, queued_item["ReceiptHandle"])

        response = {
            "statusCode": 200,
            "body": json.dumps(body)
        }
        return response


if __name__ == "__main__":
    from pprint import pprint

    r = index_collection_file()
    pprint(json.loads(r["body"]))

