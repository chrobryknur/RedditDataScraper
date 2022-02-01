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

  comments_list = []
  titles_list = []
  downs_list = []
  ups_list = []
  controversiality_list = []
  awards_list = []

  for submission in reddit.subreddit(subreddit).hot(limit=posts_to_scrape):
    submission.comments.replace_more(limit=None)
    comment_queue = submission.comments[:]


    while comment_queue:
      titles_list.append(submission.title)
      comment = comment_queue.pop(0)

      comments_list.append(comment.body)
      downs_list.append(comment.downs)
      ups_list.append(comment.ups)
      controversiality_list.append(comment.controversiality)
      awards_list.append(len(comment.all_awardings))

      t = []
      t.extend(comment.replies)
      while t:
        titles_list.append(submission.title)
        reply = t.pop(0)
        comments_list.append(reply.body)
        downs_list.append(reply.downs)
        ups_list.append(reply.ups)
        controversiality_list.append(reply.controversiality)
        awards_list.append(len(reply.all_awardings))

  titles_list = [ftfy.fix_encoding(title) for title in titles_list]
  comments_list = [ftfy.fix_encoding(comment) for comment in comments_list]

  file_name = generate_filename(subreddit)
  df = wrap_data_in_data_frame(titles_list, comments_list, downs_list, ups_list, controversiality_list, awards_list)
  upload_data_to_storage(df, file_name)

  return "DONE"

def upload_data_to_storage(df, file_name):
    from google.cloud import storage
    import pandas

    storage_client = storage.Client()
    bucket = storage_client.get_bucket("reddit-web-scraper")
    blob = bucket.blob(file_name)

    blob.upload_from_string(df.to_csv(index=False))

def wrap_data_in_data_frame(titles_list, comments_list, downs_list, ups_list, controversiality_list, awards_list):
  import pandas
  df = pandas.DataFrame(titles_list)

  df['comm_list'] = comments_list
  df['downs_list'] = downs_list
  df['ups_list'] = ups_list
  df['controversiality_list'] = controversiality_list
  df['awards_list'] = awards_list

  df.columns = ['title','comment', 'downs', 'ups', 'controversiality', 'awards']
  df['title'] = df['title'].apply(lambda x : x.replace('\n',''))
  df['comment'] = df['comment'].apply(lambda x : x.replace('\n',''))
  return df

def generate_filename(subreddit):
  from datetime import datetime

  return "input/input_{}_{}.csv".format(subreddit, datetime.today().strftime('%H:%M:%S_%d-%m-%Y'))