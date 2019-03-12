from pynggdpp import ndc

processable_collection = ndc.get_message("processable_collections")

if processable_collection is not None:
    queued_collection = {
        "collection_meta": processable_collection["Body"]["collection_meta"]
    }

    if "collection_files" in processable_collection["Body"].keys():
        queued_collection["queued_files"] = list()
        for collection_file in processable_collection["Body"]["collection_files"]:
            collection_file["key_name"] = f"{ndc.url_to_s3_key(collection_file['url'])}/{collection_file['name']}"
            if ndc.check_s3_file(collection_file["key_name"], "ndc-collection-files") is False:
                ndc.transfer_file_to_s3(collection_file['url'], key_name=collection_file["key_name"])
            queued_collection["queued_files"].append(collection_file)

    if "collection_links" in processable_collection["Body"].keys():
        queued_collection["queued_waf"] = list()
        for link in processable_collection["Body"]["collection_links"]:
            link["waf_listing"] = list()
            for index, record in enumerate(ndc.parse_waf(link['uri'])["url_list"]):
                if index > 4:
                    break
                record["key_name"] = ndc.url_to_s3_key(record['file_url'])
                ndc.transfer_file_to_s3(record["file_url"],
                                        bucket_name="ndc-collection-files",
                                        key_name=record["key_name"])
                link["waf_listing"].append(record)
            queued_collection["queued_waf"].append(link)

    print(queued_collection["collection_meta"]["ndc_collection_title"])

    if "queued_files" in queued_collection.keys():
        print("Number Files", len(queued_collection["queued_files"]))
    elif "queued_waf" in queued_collection.keys():
        print("Number Links", len(queued_collection["queued_waf"]))

    print(ndc.post_message("queued_collections",
                           queued_collection["collection_meta"]["ndc_collection_id"],
                           queued_collection)
          )
    print(ndc.delete_message("processable_collections", processable_collection["ReceiptHandle"]))
