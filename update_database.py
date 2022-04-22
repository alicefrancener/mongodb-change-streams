###
# Libraries
#
import os

import pymongo
from dotenv import load_dotenv

###
# Update database
###
load_dotenv()

client = pymongo.MongoClient(os.environ['CHANGE_STREAM_DB'])

new_data = client.changestream.collection.insert_one({'grade': '9.0'})
print(new_data.inserted_id)
