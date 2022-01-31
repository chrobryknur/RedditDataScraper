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


def run(file, argv=None, save_main_session=True):
  # The pipeline will be run on exiting the with block.
  with beam.Pipeline() as p:

    # Read the text file[pattern] into a PCollection.
    lines = p | 'Read' >> ReadFromText(file)

    counts = (
        lines
        | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))

    # Format the counts into a PCollection of strings.
    def format_result(word, count):
      return '%s: %d' % (word, count)

    output = counts | 'Format' >> beam.MapTuple(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'Write' >> WriteToText('output_'+file)

def run_dataflow_pipeline(event, context):
  file = event
  print("Processing file" + file)
  run(file)