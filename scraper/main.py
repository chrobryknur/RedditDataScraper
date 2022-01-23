from pprint import pp


def get_data_from_subreddit(request):
  import praw
  import ftfy
  import os
  import pprint

  reddit = praw.Reddit( user_agent    = 'GCP Reddit web-scraper',
                        client_id     = os.environ['reddit_client_id'],
                        client_secret = os.environ['reddit_client_secret'],
                        username      = os.environ['reddit_username'],
                        password      = os.environ['reddit_password'] )

  posts_to_scrape = 1
  subreddit = 'poland'

  request_json = request.get_json()
  if request.args and 'subreddit' in request.args:
    subreddit = request.args.get('subreddit')
    posts_to_scrape = request.args.get('posts_to_scrape')
  elif request_json and 'subreddit' in request_json:
    subreddit = request_json['subreddit']
    posts_to_scrape = request_json['posts_to_scrape']

  comm_list = []
  header_list = []

  for submission in reddit.subreddit(subreddit).hot(limit=posts_to_scrape):
    submission.comments.replace_more(limit=None)
    comment_queue = submission.comments[:]


    while comment_queue:
      header_list.append(submission.title)
      comment = comment_queue.pop(0)

      pprint.pprint(vars(comment))

      comm_list.append(comment.body)
      t = []
      t.extend(comment.replies)
      while t:
        header_list.append(submission.title)
        reply = t.pop(0)
        comm_list.append(reply.body)

  header_list = [ftfy.fix_encoding(header) for header in header_list]
  comm_list   = [ftfy.fix_encoding(comment) for comment in comm_list]

  file_name = generate_filename(subreddit)
  df = wrap_data_in_data_frame(comm_list, header_list)
  upload_data_to_storage(df, file_name)

  return "DONE"

def upload_data_to_storage(df, file_name):
    from google.cloud import storage
    import pandas

    storage_client = storage.Client()
    bucket = storage_client.get_bucket("reddit-web-scraper")
    blob = bucket.blob(file_name)

    blob.upload_from_string(df.to_csv(index=False))

def wrap_data_in_data_frame(comm_list, header_list):
  import pandas
  df = pandas.DataFrame(header_list)
  df['comm_list'] = comm_list
  df.columns = ['header','comments']
  df['comments'] = df['comments'].apply(lambda x : x.replace('\n',''))
  return df

def generate_filename(subreddit):
  from datetime import datetime

  return "{}_{}.csv".format(subreddit, datetime.today().strftime('%H:%M:%S_%d-%m-%Y'))