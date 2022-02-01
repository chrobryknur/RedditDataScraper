def trigger_dataflow(event, context):
  from googleapiclient.discovery import build
  #replace with your projectID
  file = event

  project = "redditwebscraper"
  job = project + " " + str(file['timeCreated'])
  #path of the dataflow template on google storage bucket
  template = "gs://reddit-web-scraper/templates/Pipeline"
  inputFile = "gs://reddit-web-scraper/input/" + str(file['name'])
  outputFile = "gs://reddit-web-scraper/output/output_" + str(file['name'])
  #user defined parameters to pass to the dataflow pipeline job
  parameters = {
    'input': inputFile,
    'output': outputFile
  }
  #tempLocation is the path on GCS to store temp files generated during the dataflow job
  environment = {'tempLocation': 'gs://reddit-web-scraper/temp'}

  service = build('dataflow', 'v1b3', cache_discovery=False)
  #below API is used when we want to pass the location of the dataflow job
  request = service.projects().locations().templates().launch(
    projectId=project,
    gcsPath=template,
    location='europe-central2',
    body={
      'jobName': job,
      'parameters': parameters,
      'environment':environment
    },
  )
  response = request.execute()
  print(str(response))
