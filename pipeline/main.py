import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
    """Returns an iterator over the words of this element.
    The element is a line of text.  If the line is blank, note that, too.
    Args:
      element: the element being processed
    Returns:
      The processed element.
    """
    return re.findall(r'[\w\']+', element, re.UNICODE)


def run(file):

  def format_result(word, count):
    return '%s,%d' % (word, count)

  def to_lower(list_of_words):
    return [y.lower() for y in list_of_words]

  def remove_chars(list_of_words):
    chars = ',.\"\''
    for char in chars:
        list_of_words = [y.replace(char,'') for y in list_of_words]
    return list_of_words

  filePath = "gs://reddit-web-scraper/" + file['name']

  with beam.Pipeline() as p:
    file = ( p | 'Read' >> ReadFromText(filePath)
        | 'SplitData' >> beam.Map(lambda x: x.split(','))
        | 'FormatToDict' >> beam.Map(lambda x: {"title": x[0], "comment": x[1], "downs": x[2], "ups": x[3], "controversiality": x[4], "awards": x[5]})
        | 'Split' >> beam.Map(lambda x: x['comment'].split(' '))
        | 'RemovePeriods' >> beam.Map(to_lower)
        | 'ToLower' >> beam.Map(remove_chars)
        | 'Flatten' >> beam.FlatMap(lambda elements: elements)
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
        | 'Format' >> beam.MapTuple(format_result)
        | 'Stringify' >> beam.ToString.Element()
    )
    upload_data_to_storage(file, "output_" + file['name'])

def upload_data_to_storage(file, file_name):
    from google.cloud import storage

    storage_client = storage.Client()
    bucket = storage_client.get_bucket("reddit-web-scraper")
    blob = bucket.blob(file_name)
    blob.upload_from_string(file)

def run_dataflow_pipeline(event, context):
  file = event
  print("Processing file" + file['name'])
  run(file)