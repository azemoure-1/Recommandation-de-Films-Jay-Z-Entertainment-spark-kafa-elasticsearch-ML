import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
SparkSession.builder.config(conf=SparkConf())

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS, ALSModel
from elasticsearch import Elasticsearch
import pandas as pd
pd.DataFrame.iteritems = pd.DataFrame.items
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
import json
from flask import Flask, jsonify, request
from flask_cors import CORS
import datetime
from flask import render_template


spark = (SparkSession.builder
            .appName("ElasticsearchSparkIntegration")
            .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-20_2.12:7.17.14,")
            .getOrCreate())


app = Flask(__name__, template_folder='templates', static_folder='static')
CORS(app)


# Elasticsearch configuration
es_host = 'http://localhost:9200/'
es = Elasticsearch(hosts=[es_host])
index_name_user_data = 'user_data'
index_name_movie_data = 'movie_data'
index_name_user_ratings = 'user_ratings'

# Load the ALS model
model_path = "best_model"
model = ALSModel.load(model_path)

# Endpoint for the landing page
@app.route('/', methods=['GET'])
def landing_page():
    return render_template('landing_page.html')

# Endpoint to get all user data
@app.route('/api/user_data', methods=['GET'])
def get_all_user_data():
    query = {
        "_source": ["user_id", "age", "gender", "occupation", "zip_code", "age_group"],
        "query": {
            "match_all": {}
        }
    }

    result = es.search(index=index_name_user_data, body=query, size=2000)
    redata = map(lambda x: x['_source'], result['hits']['hits'])

    user_data = list(redata)

    return jsonify(user_data)

# Endpoint to get movie_id by movie title
@app.route('/api/movie_id', methods=['GET'])
def get_movie_id_by_title():
    movie_title = request.args.get('title')

    if not movie_title:
        return jsonify({"error": "Movie title is required"}), 400

    query = {
        "_source": ["movieId", "movie_title"],
        "query": {
            "match": {
                "movie_title": movie_title
            }
        }
    }

    result = es.search(index=index_name_movie_data, body=query, size=1)

    if not result['hits']['hits']:
        return jsonify({"error": "Movie not found"}), 404

    movie_id = result['hits']['hits'][0]['_source']['movieId']
    
    return jsonify({"movie_id": movie_id})


# Endpoint to get users who rated a specific movie
@app.route('/api/movie_ratings', methods=['GET'])
def get_users_for_movie():
    movie_id = request.args.get('movie_id')

    if not movie_id:
        return jsonify({"error": "Movie ID is required"}), 400

    query = {
        "_source": ["user_id", "rating"],
        "query": {
            "term": {
                "movieId": movie_id
            }
        }
    }

    result = es.search(index=index_name_user_ratings, body=query, size=2000)
    redata = map(lambda x: x['_source'], result['hits']['hits'])

    users_ratings = list(redata)

    return jsonify(users_ratings)


def get_users_ids_for_movie_review(es, index_name_user_ratings, movie_id):
    query = {
        "_source": ["user_id"],
        "query": {
            "term": {
                "movieId": movie_id
            }
        }
    }

    result = es.search(index=index_name_user_ratings, body=query, size=2000)
    user_ids = [hit['_source']['user_id'] for hit in result['hits']['hits']]
    return user_ids



# # Endpoint to get movie recommendations for a user or based on movie title
# @app.route('/api/recommendations', methods=['GET'])
# def get_recommendations():
#     user_id = request.args.get('user_id')
#     movie_title = request.args.get('title')

#     if user_id:
#         return get_user_recommendations(user_id)
#     elif movie_title:
#         return get_movie_recommendations(movie_title)
#     else:
#         return jsonify({"error": "User ID or movie title is required"}), 400
    
# Endpoint to get movie recommendations for a user or based on movie title
@app.route('/api/recommendations', methods=['GET'])
def get_recommendations():
    user_id = request.args.get('user_id')
    movie_title = request.args.get('title')

    if user_id:
        return get_user_recommendations(user_id)
    elif movie_title:
        return get_movie_recommendations(movie_title)
    else:
        return jsonify({"error": "User ID or movie title is required"}), 400

def get_user_recommendations(user_id):
    user_id = int(user_id)
    user_df = spark.createDataFrame([(user_id,)], ["user_id"])

    user_recommendations = model.recommendForUserSubset(user_df, 10)

    if user_recommendations.count() == 0:
        return jsonify({"user_id": user_id, "recommendations": []})

    recommendations = user_recommendations.select("recommendations.movie_id").rdd.flatMap(lambda x: x[0]).collect()

    # Get movie titles for the recommended movie IDs
    recommended_movie_titles = get_movie_titles_by_ids(es, index_name_movie_data, recommendations)

    return jsonify({"user_id": user_id, "recommendations": list(recommended_movie_titles.values())})

#===================================================================

# def get_movie_titles_by_ids(es, index_name_movie_data, movie_ids):
#     query = {
#         "_source": ["movieId", "movie_title"],
#         "query": {
#             "terms": {
#                 "movieId": movie_ids
#             }
#         }
#     }

#     result = es.search(index=index_name_movie_data, body=query, size=len(movie_ids))

#     movie_titles = {hit['_id']: hit['_source']['movie_title'] for hit in result['hits']['hits']}
#     return movie_titles
#==================================================

def format_release_date(timestamp):
    if timestamp:
        return datetime.datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
    return None

def get_movie_titles_by_ids(es, index_name_movie_data, movie_ids):
    query = {
        "_source": ["movieId", "movie_title", "genre", "release_date"],
        "query": {
            "terms": {
                "movieId": movie_ids
            }
        }
    }

    result = es.search(index=index_name_movie_data, body=query, size=len(movie_ids))

    movie_info = {}
    for hit in result['hits']['hits']:
        movie_id = hit['_id']
        movie_info[movie_id] = {
            "movie_title": hit['_source']['movie_title'],
            "genre": hit['_source']['genre'],
            "release_date": format_release_date(hit['_source']['release_date'])
        }

    return movie_info

# def get_movie_recommendations(movie_title):
#     query = {
#         "_source": ["movieId", "movie_title"],
#         "query": {
#             "match": {
#                 "movie_title": movie_title
#             }
#         }
#     }

#     result = es.search(index=index_name_movie_data, body=query, size=1)

#     if not result['hits']['hits']:
#         return jsonify({"error": "Movie not found"}), 404

#     movie_id = result['hits']['hits'][0]['_source']['movieId']

#     user_ids = get_users_ids_for_movie_review(es, index_name_user_ratings, movie_id)

#     if not user_ids:
#         return jsonify({"error": "No user reviews found for the movie"}), 404

#     user_df = spark.createDataFrame([(int(user_id),) for user_id in user_ids], ["user_id"])

#     movie_recommendations = model.recommendForUserSubset(user_df, 10)

#     if movie_recommendations.count() == 0:
#         return jsonify({"movie_title": movie_title, "recommendations": []})

#     recommendations = movie_recommendations.select("recommendations.movie_id").rdd.flatMap(lambda x: x[0]).collect()

#     # Get movie titles for the recommended movie IDs
#     recommended_movie_titles = get_movie_titles_by_ids(es, index_name_movie_data, recommendations)

#     return jsonify({"movie_title": movie_title, "recommendations": list(recommended_movie_titles.values())})

#==============================================================================

def get_movie_recommendations(movie_title):
    query = {
        "_source": ["movieId", "movie_title"],
        "query": {
            "match": {
                "movie_title": movie_title
            }
        }
    }

    result = es.search(index=index_name_movie_data, body=query, size=1)

    if not result['hits']['hits']:
        return jsonify({"error": "Movie not found"}), 404

    movie_id = result['hits']['hits'][0]['_source']['movieId']

    user_ids = get_users_ids_for_movie_review(es, index_name_user_ratings, movie_id)

    if not user_ids:
        return jsonify({"error": "No user reviews found for the movie"}), 404

    user_df = spark.createDataFrame([(int(user_id),) for user_id in user_ids], ["user_id"])

    movie_recommendations = model.recommendForUserSubset(user_df, 10)

    if movie_recommendations.count() == 0:
        return jsonify({"movie_title": movie_title, "recommendations": []})

    recommendations = movie_recommendations.select("recommendations.movie_id").rdd.flatMap(lambda x: x[0]).collect()

    # Get movie info for the recommended movie IDs
    recommended_movie_info = get_movie_titles_by_ids(es, index_name_movie_data, recommendations)

    return jsonify({"movie_title": movie_title, "recommendations": recommended_movie_info})


if __name__ == '__main__':
    app.run(debug=True)



