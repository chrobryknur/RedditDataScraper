def trigger_dataflow(event, context):
  from googleapiclient.discovery import build
  file = event

  if "input" not in str(file['name']):
    print("Skipping file" + str(file['name']))
    return

  print("Preparing file" + str(file['name']))

  project = "redditwebscraper"
  job = project + " " + str(file['timeCreated'])

  print(str(file['name']))
  print(str(file['name']).replace('input', 'output'))

  template = "gs://reddit-web-scraper/templates/Pipeline"
  inputFile = "gs://reddit-web-scraper/" + str(file['name'])
  outputFile = "gs://reddit-web-scraper/" + str(file['name']).replace("input", "output")

  parameters = {
    'input': inputFile,
    'output': outputFile
  }

  print(parameters)

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
