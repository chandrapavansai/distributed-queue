# import os
# import psycopg2


# DATABASE_NAME = os.getenv('DB_NAME') or '<dbname>'
# USER = os.getenv('DB_USER') or 'postgres'
# PASSWORD = os.getenv('DB_PASSWORD') or 'postgres'
# HOST = os.getenv('DB_HOST') or 'localhost'
# PORT = os.getenv('DB_PORT') or '5432'

# db = psycopg2.connect(database=DATABASE_NAME,
#                       host=HOST,
#                       user=USER,
#                       password=PASSWORD,
#                       port=PORT)


# def init_db():
#     with db.cursor() as cursor:
#         # cursor.execute(open(<database_export_file>, "r").read())
#         db.commit()


# def clear_db():
#     with db.cursor() as cursor:
#         # cursor.execute("DROP TABLE IF EXISTS <tablename>;")
#         db.commit()


# init_db()
