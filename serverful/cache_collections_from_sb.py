'''
This script simply grabs up all ScienceBase Items that serve as NDC Collections and throws them in a MongoDB collection
for further processing. This was more convenient than working directly against ScienceBase because of ScienceBase API
timeout issues and other potential hiccups. Each time the collection of collections is refreshed, we can basically
kick off a whole process to check for updates and artifacts that need to be processed, so this script first wipes out
any existing raw collection records and builds fresh.
'''

import pynggdpp.sciencebase
import pynggdpp.serverful

sb_collections = pynggdpp.sciencebase.Collections()
serverful_infrastructure = pynggdpp.serverful.Infrastructure()

ndc_collections_raw_db = serverful_infrastructure.connect_mongodb(collection="ndc_collections_raw")
ndc_collections_raw_db.delete_many({})
response = ndc_collections_raw_db.insert_many(sb_collections.ndc_collections())
print(response.inserted_ids)

