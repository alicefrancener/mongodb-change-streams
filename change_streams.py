###
# Libraries
###
import json
import os

import pymongo
from dotenv import load_dotenv


###
# Resume a change stream
###

# Connect to mongodb
load_dotenv()
client = pymongo.MongoClient(os.environ['CHANGE_STREAM_DB'])

# Get last token locally
with open('last_token.json') as f:
    last_resume_token = json.load(f)

# Start to watch changes after last saved token client.<database_name>.<collection_name>.watch()
change_stream = client.changestream.collection.watch(resume_after=last_resume_token)

# Save change streams to Data Lake
while True:
    change = change_stream.try_next()
    if change is None:
        break

    # Save collection changes
    with open('mongodb_changes.txt', 'a') as f:
        f.write(json.dumps(change, default=str))
        f.write('\n')

    # Save token
    with open('last_token.json', 'w') as f:
        json.dump(change_stream._resume_token, f)
