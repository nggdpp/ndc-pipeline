from pynggdpp import ndc

def representative_sample():
    representative_sample = {
        "csv_files": [
            "5c4e0145e4b0708288f78a3e",
            "4f4e49cce4b07f02db5d914b",
            "4f4e4acae4b07f02db67d228"
        ],
        "xml_files": [
            "55ce5b1ee4b01487cbfc7104",
            "57c71fb3e4b0f2f0cebed0f0",
            "4f4e4b32e4b07f02db6b4a55"
        ],
        "waf_links": [
            "57520032e4b053f0edd03e54",
            "511ab167e4b084e2824d6a18",
            "5141e4c2e4b0eefcba208e52"
        ],
        "corner_cases": [
            "57bb5f55e4b03fd6b7dd0532",
            "4f4e4b32e4b07f02db6b4a48",
            "4f4e496fe4b07f02db5a3df0",
            "4f4e49d8e4b07f02db5df325"
        ]
    }

    for k, v in representative_sample.items():
        print(k)
        for item in v:
            processed_collection = ndc.build_processable_routes(collection_id=item)
            print(processed_collection["collection_meta"]["ndc_collection_title"])
            print(ndc.post_message(processed_collection["process_queue"],
                                   processed_collection["collection_meta"]["ndc_collection_id"],
                                   processed_collection))


def full_queue():
    if not ndc.check_s3_file("ndc_collections.json", "ndc-cache"):
        ndc.put_file_to_s3(ndc.ndc_get_collections(), "ndc_collections.json", "ndc-cache")

    ndc_collections = ndc.get_s3_file("ndc_collections.json", bucket_name="ndc-cache", return_type="dict")

    for index, collection in enumerate(ndc_collections):
        processed_collection = ndc.build_processable_routes(collection_record=collection)
        print(processed_collection["collection_meta"]["ndc_collection_title"])
        print(ndc.post_message(processed_collection["process_queue"],
                               processed_collection["collection_meta"]["ndc_collection_id"],
                               processed_collection)
              )


full_queue()