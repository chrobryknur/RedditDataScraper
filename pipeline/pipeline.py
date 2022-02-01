import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_value_provider_argument(
      '--input',
      dest='input',
      default='gs://reddit-web-scraper/input/example.csv',
      help='Input file to process.')
  parser.add_value_provider_argument(
      '--output',
      dest='output',
      default='gs://reddit-web-scraper/output/example.csv',
      help='Output file to write results to.')

  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  def format_result(word, count):
    return '%s,%d' % (word, count)

  def to_lower(list_of_words):
    return [y.lower() for y in list_of_words]

  def remove_chars(list_of_words):
    chars = ',.\"\''
    for char in chars:
        list_of_words = [y.replace(char,'') for y in list_of_words]
    return list_of_words

  with beam.Pipeline(options=pipeline_options) as p:
    file = ( p | 'Read' >> ReadFromText(known_args.input)
        | 'SplitData' >> beam.Map(lambda x: x.split(','))
        | 'FormatToDict' >> beam.Map(lambda x: {"title": x[0], "comment": x[1], "downs": x[2], "ups": x[3], "controversiality": x[4], "awards": x[5]})
        | 'Split' >> beam.Map(lambda x: x['comment'].split(' '))
        | 'RemovePeriods' >> beam.Map(to_lower)
        | 'ToLower' >> beam.Map(remove_chars)
        | 'Flatten' >> beam.FlatMap(lambda elements: elements)
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
        | 'Format' >> beam.MapTuple(format_result)
        | 'Write' >> WriteToText(known_args.output)
        )

if __name__ == '__main__':
  run()