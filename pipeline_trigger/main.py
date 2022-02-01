def trigger_dataflow(event, context):
  from googleapiclient.discovery import build
  file = event

  if "input" not in str(file['name']):
    print("Skipping file" + str(file['name']))
    return

  project = "redditwebscraper"
  job = project + " " + str(file['timeCreated'])

  template = "gs://reddit-web-scraper/templates/Pipeline"
  inputFile = "gs://reddit-web-scraper/input/" + str(file['name'])
  outputFile = "gs://reddit-web-scraper/output/" + str(file['name']).replace("input", "output")

  parameters = {
    'input': inputFile,
    'output': outputFile
  }

  environment = {'tempLocation': 'gs://reddit-web-scraper/temp'}

  service = build('dataflow', 'v1b3', cache_discovery=False)

  request = service.projects().locations().templates().launch(
    projectId=project,
    gcsPath=template,
    location='europe-central2',
    body={
      'jobName': job,
      'parameters': parameters,
      'environment': environment
    },
  )
  response = request.execute()
  print(str(response))
