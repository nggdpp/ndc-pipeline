import pynggdpp.item_process
import pynggdpp.rest_api
import json
import inspect


def process_actionable(event=None, context="serverless"):
    '''
    This function picks up a collection from the actionable_queue that has some kind of element that can be processed
    for items (files or links). The process works through either a WAF or ScienceBase files and builds a uniform
    file metadata object that goes onto another queue for further processing. In the current pipeline, we are
    essentially breaking the process up into its various points to progressively achieve a relatively standard set
    of input material to build out item indexes for each collection. Each item index may be slightly different in terms
    of what fields each document contains - common fields infused from the collections and then some variation depending
    on what is presented in source material.

    :param event:
    :param context:
    :return:
    '''
    source_queue = "actionable_queue"

    if context == "serverless":
        import pynggdpp.aws
        aws_messaging = pynggdpp.aws.Messaging()
        queue_item = aws_messaging.get_message(source_queue)
        queued_item = queue_item["Body"]
        receipt_handle = queue_item["ReceiptHandle"]
    elif context == "serverful":
        import pynggdpp.serverful
        serverful_infrastructure = pynggdpp.serverful.Infrastructure()
        queue = serverful_infrastructure.connect_mongodb(collection=source_queue)
        queued_item = queue.find_one({})
        receipt_handle = queued_item["_id"]

    if queued_item is None:
        response = {
            "statusCode": 204
        }
        return response
    else:
        link_processor = pynggdpp.item_process.Links()
        file_processor = pynggdpp.item_process.Files()
        log = pynggdpp.item_process.Log()

        body = {
            "Process Log": list()
        }

        collection_record = queued_item
        ndc_collection_id = collection_record["collection_metadata"]["ndc_collection_id"]

        file_items = list()

        if "actionable_weblinks" in collection_record["introspection_metadata"].keys():
            for link in collection_record["introspection_metadata"]["actionable_weblinks"]:
                waf_scrape = link_processor.parse_waf(link["uri"])
                if waf_scrape is None:
                    body["Process Log"].append(
                        log.log_process_step(
                            identifier=ndc_collection_id,
                            entry_type="waf_scrape_failure",
                            log=link,
                            context=context,
                            source_file=inspect.stack()[0][1].split("/")[-1],
                            source_function=inspect.stack()[0][3],
                            message_queue_packet={
                                "message_queue": "failed_waf_link",
                                "identifier": ndc_collection_id
                            }
                        )
                    )
                else:
                    for index, url in enumerate(waf_scrape["url_list"]):
                        # Temporary representative sample
                        if index > 9:
                            break
                        file_items.append(
                            file_processor.uniform_file_object(
                                harvest_url=url,
                                collection_metadata=collection_record["collection_metadata"]
                            )
                        )

        if "actionable_files" in collection_record["introspection_metadata"].keys():
            for file_metadata in collection_record["introspection_metadata"]["actionable_files"]:
                file_items.append(
                    file_processor.uniform_file_object(
                        file_metadata=file_metadata,
                        collection_metadata=collection_record["collection_metadata"]
                    )
                )

        if len(file_items) > 0:
            for file_item in file_items:
                body["Process Log"].append(
                    log.log_process_step(
                        identifier=ndc_collection_id,
                        entry_type="queued_source_file",
                        log=file_item,
                        context=context,
                        source_file=inspect.stack()[0][1].split("/")[-1],
                        source_function=inspect.stack()[0][3],
                        message_queue_packet={
                            "message_queue": "files_to_index",
                            "identifier": file_item["ndc_file_url"]
                        }
                    )
                )

        body["Updated Collection ID"] = ndc_collection_id

        if context == "serverless":
            body["Deleted Message Response"] = aws_messaging.delete_message(source_queue, receipt_handle)
        elif context == "serverful":
            body["Deleted Message Response"] = queue.delete_one({"_id": receipt_handle})

        response = {
            "statusCode": 200,
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

    r = process_actionable(context=args.context)
    pprint(r["body"])

