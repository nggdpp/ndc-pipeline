from pynggdpp import processing as ndcProcessing

# Set local environment variables to make database connections
ndcProcessing.set_env_variables('docker.env')

# Setup connection to mongodb on the DataDistillery platform
mongo_client_datadistillery = ndcProcessing.mongodb_client()
ndc_collections = mongo_client_datadistillery['ndc_collections']

"""
This aggregation pipeline looks at the cached collection file information to build out a set of file objects that
can be evaluated for further processing. This could be rewritten to work directly against ScienceBase, but is more
efficiently operated against the MongoDB cache of collection source information. In the first stage, we look for
collections that do have source files but do not yet have a set of processable file objects that have been evaluated
and set up for processing. We exclude a couple of file types that we know we can't do anything with at this point -
zip files that we're not unpacking at this point and the older metadata.xml files that we determined are all just the
collection survey responses. We then regroup the result on the collection ID, so that there is a list of file objects
for evaluation in this process.
"""

pipeline = [
    {
        u"$match": {
            u"$and": [
                {
                    u"source_meta.files": {
                        u"$exists": True
                    }
                },
                {
                    u"processable_files": {
                        u"$exists": False
                    }
                }
            ]
        }
    },
    {
        u"$unwind": {
            u"path": u"$source_meta.files"
        }
    },
    {
        u"$match": {
            u"$and": [
                {
                    u"source_meta.files.name": {
                        u"$ne": u"metadata.xml"
                    }
                },
                {
                    u"source_meta.files.contentType": {
                        u"$ne": u"application/zip"
                    }
                }
            ]
        }
    },
    {
        u"$project": {
            u"collection_id": True,
            u"file": u"$source_meta.files"
        }
    },
    {
        u"$group": {
            u"_id": u"$collection_id",
            u"potentially_processable_files": {
                u"$push": u"$file"
            }
        }
    }
]

"""
In this workflow, we simply run through the potentially processable files and run the file_meta() function on them
to determine whether they are files we can actually process. This is a fairly expensive operation in that it needs to
request every file and run through a series of steps to evaluate whether or not it can be dealt with. However, breaking
things up this way and caching the evaluation details, including things like the set of field names discovered in the 
files, helps to set things up for more efficient file processing later. It will also serve as value-added metadata
about the files that can be used in other ways such as determining which collections contain properties that can be
built upon with data fusion operations, and it flags collections that have files on board but that can't be processed
for some reason and need data management attention.
"""

for index, collection in enumerate(ndc_collections.aggregate(pipeline)):
    if index > 0:
        break
    processable_files = ndcProcessing.file_meta(collection["_id"], collection["potentially_processable_files"])
    if not isinstance(processable_files, list):
        print(processable_files)
    else len(processable_files) > 0:
        ndc_collections.update_one({"collection_id": collection["_id"]}, {"$set": {"processable_files": processable_files}})
    print(collection["_id"], len(processable_files))

