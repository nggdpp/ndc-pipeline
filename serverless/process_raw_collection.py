import pynggdpp.sciencebase
import pynggdpp.item_process
import json
import inspect


def process_raw_collection(event=None, context="serverless"):
    '''
    This function picks up a raw collection (ScienceBase Item JSON) from an SQS queue, builds a structured meta document
    that will be used in other parts of the pipeline to infuse collection metadata into collection items. The process
    of building collection metadata determines whether or not there is anything processable for items within the
    collection metadata (files stashed in ScienceBase or actionable web links at this point). If there are actionable
    elements, then the collection document is sent to another SQS queue for further processing. The ElasticSearch
    index created for collections in this process powers the /collection/ API.

    :param event:
    :param context:
    :return: JSON document reporting on status of the process
    '''

    source_queue = "raw_collections"

    if context == "serverless":
        import pynggdpp.aws
        aws_messaging = pynggdpp.aws.Messaging()
        queue_item = aws_messaging.get_message(source_queue)
        if queue_item is not None:
            queued_item = queue_item["Body"]
            receipt_handle = queue_item["ReceiptHandle"]
    elif context == "serverful":
        import pynggdpp.serverful
        serverful_infrastructure = pynggdpp.serverful.Infrastructure()
        queue_item = serverful_infrastructure.connect_mongodb(collection=source_queue)
        if queue_item is not None:
            queued_item = queue_item.find_one({})
            receipt_handle = queued_item["_id"]

    if queue_item is None:
        response = {
            "statusCode": 204
        }
        return response
    else:
        sb_collections = pynggdpp.sciencebase.Collections()
        log = pynggdpp.item_process.Log()

        if context == "serverless":
            aws_connect = pynggdpp.aws.Connect()
            es_client = aws_connect.elastic_client()

        body = {
            "Process Log": list()
        }

        doc = sb_collections.ndc_collection_meta(collection_record=queued_item)
        ndc_collection_id = doc["collection_metadata"]["ndc_collection_id"]

        if context == "serverless":
            r_index = es_client.index(
                index="processed_collections",
                doc_type="ndc_collection",
                id=ndc_collection_id,
                body=doc
            )

        elif context == "serverful":
            doc["_id"] = ndc_collection_id
            processed_collections = serverful_infrastructure.connect_mongodb(collection="processed_collections")
            insert_response = processed_collections.insert_one(doc)
            if insert_response.acknowledged:
                r_index = {
                    "result": "created"
                }
            else:
                r_index = {
                    "result": "error"
                }

        body["Process Log"].append(
            log.log_process_step(
                identifier=ndc_collection_id,
                entry_type="collection_indexed",
                log=r_index,
                context=context,
                source_file=inspect.stack()[0][1].split("/")[-1],
                source_function=inspect.stack()[0][3]
            )
        )

        # Tee up actionable collections for more work
        if r_index["result"] in ["created", "updated"]:
            if "actionable_files" in doc["introspection_metadata"].keys() or \
                    "actionable_weblinks" in doc["introspection_metadata"].keys():
                body["Process Log"].append(
                    log.log_process_step(
                        identifier=ndc_collection_id,
                        entry_type="actionable_files_found",
                        log=doc,
                        context=context,
                        source_file=inspect.stack()[0][1].split("/")[-1],
                        source_function=inspect.stack()[0][3],
                        message_queue_packet={
                            "message_queue": "actionable_queue",
                            "identifier": ndc_collection_id
                        }
                    )
                )

            # Delete the original message from the queue
            if context == "serverless":
                body["Deleted Message Response"] = aws_messaging.delete_message(source_queue, receipt_handle)
            elif context == "serverful":
                body["Deleted Message Response"] = queue.delete_one({"_id": receipt_handle})

            response = {
                "statusCode": 200,
                "body": body
            }
            return response

        else:
            body["Process Log"].append(
                log.log_process_step(
                    identifier=ndc_collection_id,
                    entry_type="collection_indexing_error",
                    log=queued_item,
                    context=context,
                    source_file=inspect.stack()[0][1].split("/")[-1],
                    source_function=inspect.stack()[0][3],
                    message_queue_packet={
                        "message_queue": "failed_collection_indexing",
                        "identifier": ndc_collection_id
                    }
                )
            )
            response = {
                "statusCode": 500,
                "body": body
            }
            return response


if __name__ == "__main__":
    from pprint import pprint
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("-c", "--context", dest="context",
                        help="Supply a context - serverless or serverful",
                        metavar="CONTEXT_NAME")

    args = parser.parse_args()

    r = process_raw_collection(context=args.context)
    pprint(r["body"])


