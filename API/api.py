from flask import Flask, jsonify

app = Flask(__name__)

# Load data from files
def load_item_data():
    item_data = {}
    with open('ml-100k/item', 'r', encoding='ISO-8859-1') as file:
        for line in file:
            fields = line.strip().split('|')
            item_id = int(fields[0])
            item_data[item_id] = {
                "movieId": item_id,
                "movie_title": fields[1],
                "release_date": fields[2],
                "IMDb_URL": (fields[4]),
                "unknown": (fields[5]),
                "Action": (fields[6]),
                "Adventure": (fields[7]),
                "Animation": (fields[8]),
                "Children": (fields[9]),
                "Comedy": (fields[10]),  # Corrected field name to "Comedy"
                "Crime": (fields[11]),
                "Documentary": (fields[12]),
                "Drama": (fields[13]),
                "Fantasy": (fields[14]),
                "Film-Noir": (fields[15]),
                "Horror": (fields[16]),
                "Musical": (fields[17]),
                "Mystery": (fields[18]),
                "Romance": (fields[19]),
                "Sci-Fi": (fields[20]),
                "Thriller": (fields[21]),
                "War": (fields[22]),
                "Western": (fields[23])
            }
    return item_data

# ["movieId","movie_title","release_date","video_release_date","IMDb_URL","unknown","Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror"
#                  ,"Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"]

def load_user_data():
    user_data = {}
    with open('ml-100k/user', 'r') as file:
        for line in file:
            fields = line.strip().split('|')
            user_id = int(fields[0])
            user_data[user_id] = {
                'user_id': user_id,
                'age': int(fields[1]),
                'gender': fields[2],
                'occupation': fields[3],
                'zip_code': fields[4]
            }
    return user_data

def load_user_rating_data():
    user_rating_data = []
    with open('ml-100k/data', 'r') as file:
        for line in file:
            fields = line.strip().split('\t')
            if len(fields) >= 4:
                user_rating_data.append({
                    'user_id': int(fields[0]),
                    'movieId': int(fields[1]),
                    'rating': int(fields[2]),
                    'timestamp': int(fields[3])
                })
    return user_rating_data

def load_genre_data():
    genre_data = {}
    with open('ml-100k/genre', 'r') as file:
        for line in file:
            fields = line.strip().split('|')
            if len(fields) >= 2:
                genre_name = fields[0]
                genre_id = int(fields[1])
                genre_data[genre_name] = genre_id
    return genre_data

# API endpoints
@app.route('/movies')
def get_all_movies():
    return jsonify(load_item_data())

@app.route('/movie/<int:movie_id>')
def get_movie(movie_id):
    item_data = load_item_data()
    if movie_id in item_data:
        return jsonify(item_data[movie_id])
    else:
        return jsonify({'error': 'Movie not found'}), 404

@app.route('/users')
def get_all_users():
    return jsonify(load_user_data())

@app.route('/user/<int:user_id>')
def get_user(user_id):
    user_data = load_user_data()
    if user_id in user_data:
        return jsonify(user_data[user_id])
    else:
        return jsonify({'error': 'User not found'}), 404

@app.route('/genres')
def get_genres():
    return jsonify(load_genre_data())

@app.route('/user_ratings')
def get_all_user_ratings():
    return jsonify(load_user_rating_data())

# @app.route('/user_ratings/<int:user_id>')
# def get_user_ratings(user_id):
#     user_ratings = [rating for rating in load_user_rating_data() if rating['user_id'] == user_id]
#     if user_ratings:
#         return jsonify(user_ratings)
#     else:
#         return jsonify({'error': 'User ratings not found'}), 404


# @app.route('/user_ratings/<int:route_number>')
# def get_user_rating_by_route_number(route_number):
#     user_ratings = load_user_rating_data()

#     if not user_ratings or route_number < 1 or route_number > len(user_ratings):
#         return jsonify({'error': 'Invalid route number or user ratings not found'}), 404

#     # Get the user rating for the specified route number
#     selected_rating = user_ratings[route_number - 1]

#     # Format the response with the specified route number
#     formatted_rating = {str(route_number): selected_rating}

#     return jsonify(formatted_rating)


@app.route('/user_ratings/<int:route_number>')
def get_user_rating_by_route_number(route_number):
    user_ratings = load_user_rating_data()

    if not user_ratings or route_number < 1 or route_number > len(user_ratings):
        return jsonify({'error': 'Invalid route number or user ratings not found'}), 404

    # Get the user rating for the specified route number
    selected_rating = user_ratings[route_number - 1]

    return jsonify(selected_rating)


if __name__ == '__main__':
    app.run(debug=True)
