import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

class WordcountOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
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

def run():
  def format_result(word, count):
    return '%s,%d' % (word, count)

  def to_lower(list_of_words):
    return [y.lower() for y in list_of_words]

  def remove_chars(list_of_words):
    chars = ',.\"\''
    for char in chars:
        list_of_words = [y.replace(char,'') for y in list_of_words]
    return list_of_words

  pipeline_options = PipelineOptions()

  with beam.Pipeline(options=pipeline_options) as p:
    wordcount_options = pipeline_options.view_as(WordcountOptions)
    print("Input: " + str(wordcount_options.input))
    print("Output: " + str(wordcount_options.output))
    ( p | 'Read' >> ReadFromText(wordcount_options.input)
        | 'SplitData' >> beam.Map(lambda x: x.split('~'))
        | 'FormatToDict' >> beam.Map(lambda x: {"title": x[0], "comment": x[1], "downs": x[2], "ups": x[3], "controversiality": x[4], "awards": x[5]})
        | 'Split' >> beam.Map(lambda x: x['comment'].split(' '))
        | 'RemovePeriods' >> beam.Map(to_lower)
        | 'ToLower' >> beam.Map(remove_chars)
        | 'Flatten' >> beam.FlatMap(lambda elements: elements)
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
        | 'Format' >> beam.MapTuple(format_result)
        | 'Write' >> WriteToText(wordcount_options.output)
        )

if __name__ == '__main__':
  run()