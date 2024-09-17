# https://www.kaggle.com/code/alexisbcook/nested-and-repeated-data/tutorial
# --------------- 1) Who had the most commits in 2016? ---------------
max_commits_query = """
                    SELECT committer.name AS committer_name,
                        COUNT(*) AS num_commits
                    FROM bigquery-public-data.github_repos.sample_commits
                    WHERE EXTRACT(YEAR FROM committer.date) = 2016
                    GROUP BY committer_name
                    ORDER BY num_commits DESC
                    """


# --------------- 2) Look at languages! ---------------
num_rows = 6


# --------------- 3) What's the most popular programming language? ---------------
pop_lang_query = """
                 SELECT l.name AS language_name,
                     COUNT(*) AS num_repos
                 FROM bigquery-public-data.github_repos.languages,
                     UNNEST(language) AS l
                 GROUP BY language_name
                 ORDER BY num_repos DESC
                 """


# --------------- 4) Which languages are used in the repository with the most languages? ---------------
all_langs_query = """
                  SELECT l.name AS name,
                      SUM(l.bytes) AS bytes
                  FROM bigquery-public-data.github_repos.languages,
                      UNNEST(language) AS l
                  WHERE repo_name = 'polyrabbit/polyglot'
                  GROUP BY name
                  ORDER BY bytes DESC 
                  """