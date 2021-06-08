from google.cloud import storage
import os
import pandas as pd


def update_returns(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")
    client = storage.Client()
    bucket = client.get_bucket("bd_predictions")
    blobs = list(client.list_blobs(bucket))

    for blob in blobs:
      tmp = pd.read_csv(blob.open('r'))
      name = blob.name.split('_')[0].upper()
      print("Getting Returns")
      tmp["returns"] = tmp["close"].pct_change()
      tmp["crypto"] = name
      tmp.dropna(inplace = True)
      try:
        tmp.drop(columns=["Unnamed: 0", "Unnamed: 0.1"], inplace = True)
      except:
        print("surplus columns not found")
      filename = "/tmp/{}_returns.csv".format(name)
      tmp.to_csv(filename, index = None)
      with open(filename, 'rb') as fl:
        blob.upload_from_file(fl)
      os.remove(filename)
      print("file updated")