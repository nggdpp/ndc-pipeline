import pynggdpp.aws
import pynggdpp.serverful
import pynggdpp.sciencebase
import json


def queue_collections(event=None, context="serverless"):
    '''
    queue_collections retrieves all ScienceBase Items from within the NDC container (based on a tag name and regardless
    of where they are in the hierarchy) and puts the ScienceBase Item JSON on a "raw_collections" message queue for
    further processing. From pynggdpp, this function relies on the ndc_collections() function to get items and the
    post_message() function to put the messages on the SQS queue.

    :param event:
    :param context:
    :return: JSON document reporting on the collections queued.
    '''
    sb_collections = pynggdpp.sciencebase.Collections()
    current_collections = sb_collections.ndc_collections()

    body = dict()

    if context == "serverless":
        aws_messaging = pynggdpp.aws.Messaging()
        body["Collections Queued"] = list()
        for collection in current_collections:
            aws_messaging.post_message("raw_collections",
                             collection["id"],
                             collection)

            body["Collections Queued"].append({
                "ndc_collection_title": collection["title"],
                "ndc_collection_id": collection["id"]
            })
            body["Number of Collections Queued"] = len(body["Collections Queued"])

    elif context == "serverful":
        serverful_infrastructure = pynggdpp.serverful.Infrastructure()
        queue = serverful_infrastructure.connect_mongodb(collection="raw_collections")
        queue.delete_many({})
        queue.insert_many(current_collections)

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
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

    r = queue_collections(context=args.context)
    pprint(json.loads(r["body"]))

