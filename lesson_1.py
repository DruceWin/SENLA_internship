
# -------------------- 1) How long does it take for questions to receive answers? --------------------
# Your code here
correct_query = """
              SELECT q.id AS q_id,
                  MIN(TIMESTAMP_DIFF(a.creation_date, q.creation_date, SECOND)) as time_to_answer
              FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                  LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
              ON q.id = a.parent_id
              WHERE q.creation_date >= '2018-01-01' and q.creation_date < '2018-02-01'
              GROUP BY q_id
              ORDER BY time_to_answer
              """

# Check your answer
q_1.check()

# Run the query, and return a pandas DataFrame
correct_result = client.query(correct_query).result().to_dataframe()
print("Percentage of answered questions: %s%%" % \
      (sum(correct_result["time_to_answer"].notnull()) / len(correct_result) * 100))
print("Number of questions:", len(correct_result))


# -------------------- 2) Initial questions and answers, Part 1 --------------------
# Your code here
q_and_a_query = """
                SELECT q.owner_user_id AS owner_user_id,
                    MIN(q.creation_date) AS q_creation_date,
                    MIN(a.creation_date) AS a_creation_date
                FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                    FULL JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                ON q.owner_user_id = a.owner_user_id
                WHERE q.creation_date >= '2019-01-01' AND q.creation_date < '2019-02-01'
                    AND a.creation_date >= '2019-01-01' AND a.creation_date < '2019-02-01'
                GROUP BY owner_user_id
                """

# Check your answer
q_2.check()


# -------------------- 3) Initial questions and answers, Part 2 --------------------
# Your code here
three_tables_query = """
                SELECT u.id as id,
                    MIN(q.creation_date) AS q_creation_date,
                    MIN(a.creation_date) AS a_creation_date
                FROM `bigquery-public-data.stackoverflow.users` AS u
                    FULL JOIN `bigquery-public-data.stackoverflow.posts_questions` AS q
                ON u.id = q.owner_user_id
                    LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
                ON u.id = a.owner_user_id
                WHERE u.creation_date >= '2019-01-01' AND u.creation_date < '2019-02-01'
                GROUP BY id
                """

# Check your answer
q_3.check()


# -------------------- 4) How many distinct users posted on January 1, 2019? --------------------
# Your code here
all_users_query = """
                SELECT q.owner_user_id AS owner_user_id,
                FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                WHERE q.creation_date >= '2019-01-01' AND q.creation_date < '2019-01-02'
                UNION DISTINCT
                SELECT a.owner_user_id AS owner_user_id,
                FROM `bigquery-public-data.stackoverflow.posts_answers` AS a
                WHERE a.creation_date >= '2019-01-01' AND a.creation_date < '2019-01-02'
                """

# Check your answer
q_4.check()