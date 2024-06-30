from flask import Flask, request, jsonify, send_file
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
import sqlite3
import os
import zipfile

app = Flask(__name__)
app.config['JWT_SECRET_KEY'] = os.environ.get('JWT_SECRET_KEY', 'default_secret_key')  # Default key for local development
jwt = JWTManager(app)
USERS_DATABASE = os.path.join(os.getcwd(), 'data', 'users.db')
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'data')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
ALLOWED_EXTENSIONS = {'db'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_db(database):
    conn = sqlite3.connect(database)
    return conn

def init_users_db():
    os.makedirs(os.path.dirname(USERS_DATABASE), exist_ok=True)
    conn = get_db(USERS_DATABASE)
    conn.execute('''CREATE TABLE IF NOT EXISTS users
                   (id INTEGER PRIMARY KEY AUTOINCREMENT,
                   username TEXT UNIQUE NOT NULL,
                   password TEXT NOT NULL);''')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM users')
    if cursor.fetchone()[0] == 0:  # If no users exist
        cursor.execute('INSERT INTO users (username, password) VALUES (?, ?)', ('user', 'password'))
    conn.commit()
    conn.close()

@app.route('/')
def hello():
    return "Hello, vercel!"
@app.route('/user/login', methods=['POST'])
def login():
    """
    Authenticates a user and generates a JWT access token.

    Parameters:
    - request.json: A JSON object containing the 'username' and 'password' fields.

    Returns:
    - flask.Response: A Flask response object containing a JSON object with the 'access_token' field.
      If the username and password are valid, the JSON object will contain the access token.
      If the username and password are invalid, a JSON object with an error message will be returned.

    Raises:
    - None
    """
    username = request.json.get('username')
    password = request.json.get('password')
    conn = get_db(USERS_DATABASE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = ? AND password = ?", (username, password))
    user = cursor.fetchone()
    if user:
        access_token = create_access_token(identity=username)
        return jsonify(access_token=access_token)
    else:
        return jsonify({"msg": "Bad username or password"}), 401

@app.route('/user/register', methods=['POST'])
@jwt_required()
def register():
    """
    Registers a new user in the users database.

    Parameters:
    - request.json: A JSON object containing the 'username' and 'password' fields.

    Returns:
    - flask.Response: A Flask response object containing a JSON object with a success message.
      If the username or password is missing, a JSON object with an error message will be returned.

    Raises:
    - None
    """
    username = request.json.get('username')
    password = request.json.get('password')
    if not username or not password:
        return jsonify({"msg": "Missing username or password"}), 400
    conn = get_db(USERS_DATABASE)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO users (username, password) VALUES (?, ?)", (username, password))
    conn.commit()
    conn.close()
    return jsonify({"msg": "User registered successfully"}), 200

@app.route('/user/<username>', methods=['DELETE'])
@jwt_required()
def delete_user(username):
    conn = get_db(USERS_DATABASE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM users WHERE username = ?", (username,))
    if cursor.rowcount == 0:
        return jsonify({"msg": "User not found"}), 404
    conn.commit()
    conn.close()
    return jsonify({"msg": "User deleted successfully"}), 200

@app.route('/database/create', methods=['POST'])
@jwt_required()
def create_database():
    """
    Creates a new SQLite database with the given name.

    Parameters:
    db_name (str): The name of the database to be created. This is obtained from the request JSON.

    Returns:
    flask.Response: A Flask response object containing a JSON message indicating success or failure.
    If the database name is missing, a JSON response with an error message is returned.
    If the database already exists, a JSON response with an error message is returned.

    Raises:
    None
    """
    db_name = request.json.get('db_name')
    if not db_name:
        return jsonify({"msg": "Missing database name"}), 400
    db_path = os.path.join(os.getcwd(), 'data', f'{db_name}.db')
    if os.path.exists(db_path):
        return jsonify({"msg": "Database already exists"}), 400
    conn = get_db(db_path)
    conn.close()
    return jsonify({"msg": f"Database {db_name} created successfully"}), 200


@app.route('/database/download', methods=['GET'])
@jwt_required()
def download_db():
    """
    Download a specified database as a zip file.

    Parameters:
    db_name (str): The name of the database to download.

    Returns:
    flask.Response: A Flask response object containing the zipped database file.
    If the database name is missing, a JSON response with an error message is returned.

    Raises:
    None

    """
    db_name = request.args.get('db_name')
    if not db_name:
        return jsonify({"msg": "Missing database name"}), 400

    # Construct the path to the database file
    db_path = os.path.join(os.getcwd(), 'data', f'{db_name}.db')

    # Construct the path to the zip file
    zip_path = os.path.join(os.getcwd(), 'data', f'{db_name}.zip')

    # Create a new zip file and add the database file to it
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        zipf.write(db_path, os.path.basename(db_path))

    # Return the zipped database file as a downloadable file
    return send_file(zip_path, as_attachment=True)

@app.route('/database/upload', methods=['POST'])
@jwt_required()
def upload_database():
    if 'file' not in request.files:
        return jsonify({"msg": "No file part"}), 400
    file = request.files['file']
    db_name = request.form.get('db_name')
    force = request.form.get('force', 'false').lower() == 'true'
    
    if file.filename == '':
        return jsonify({"msg": "No selected file"}), 400
    if not allowed_file(file.filename):
        return jsonify({"msg": "File type not allowed"}), 400
    if not db_name:
        return jsonify({"msg": "Missing database name"}), 400
    
    db_path = os.path.join(app.config['UPLOAD_FOLDER'], f'{db_name}.db')
    
    if os.path.exists(db_path) and not force:
        return jsonify({"msg": "Database already exists. Use force option to overwrite."}), 400
    
    file.save(db_path)
    return jsonify({"msg": f"Database {db_name} uploaded successfully"}), 201

@app.route('/tables', methods=['GET'])
@jwt_required()
def list_tables():
    """
    Retrieves the list of tables in a specified database.

    Parameters:
    db_name (str): The name of the database from which to retrieve the tables. This is obtained from the request arguments.

    Returns:
    flask.Response: A Flask response object containing a JSON array of table names.
    If the database name is missing, a JSON response with an error message is returned.

    Raises:
    None
    """
    db_name = request.args.get('db_name')
    if not db_name:
        return jsonify({"msg": "Missing database name"}), 400
    db_path = os.path.join(os.getcwd(), 'data', f'{db_name}.db')
    conn = get_db(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    conn.close()
    return jsonify([table[0] for table in tables])

@app.route('/table/<table_name>/columns', methods=['GET'])
@jwt_required()
def list_columns(table_name):
    """
    Retrieves the list of columns in a specified table from a database.

    Parameters:
    table_name (str): The name of the table from which to retrieve the columns. This is obtained from the URL path.
    db_name (str): The name of the database containing the table. This is obtained from the request arguments.

    Returns:
    flask.Response: A Flask response object containing a JSON array of column names.
    If the database name is missing, a JSON response with an error message is returned.

    Raises:
    None
    """
    db_name = request.args.get('db_name')
    if not db_name:
        return jsonify({"msg": "Missing database name"}), 400
    db_path = os.path.join(os.getcwd(), 'data', f'{db_name}.db')
    conn = get_db(db_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    conn.close()
    return jsonify([column[1] for column in columns])

@app.route('/table', methods=['POST'])
@jwt_required()
def create_table():
    """
    Creates a new table in a specified database.

    Parameters:
    db_name (str): The name of the database where the table will be created. This is obtained from the request JSON.
    table_name (str): The name of the table to be created. This is obtained from the request JSON.
    columns (str): The columns to be included in the table, formatted as a comma-separated string. This is obtained from the request JSON.

    Returns:
    flask.Response: A Flask response object containing a JSON message indicating success or failure.
    If the database name, table name, or columns are missing, a JSON response with an error message is returned.

    Raises:
    None
    """
    db_name = request.json.get('db_name')
    table_name = request.json.get('table_name')
    columns = request.json.get('columns')
    if not db_name or not table_name or not columns:
        return jsonify({"msg": "Missing database name, table name or columns"}), 400
    db_path = os.path.join(os.getcwd(), 'data', f'{db_name}.db')
    conn = get_db(db_path)
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
    conn.commit()
    conn.close()
    return jsonify({"msg": f"Table {table_name} created successfully"}), 200

@app.route('/table/<table_name>', methods=['DELETE'])
@jwt_required()
def delete_table(table_name):
    """
    Deletes a specified table from a database.

    Parameters:
    table_name (str): The name of the table to delete. This is obtained from the URL path.
    db_name (str): The name of the database containing the table. This is obtained from the request arguments.

    Returns:
    flask.Response: A Flask response object containing a JSON message indicating success or failure.
    If the database name is missing, a JSON response with an error message is returned.

    Raises:
    None
    """
    db_name = request.args.get('db_name')
    if not db_name:
        return jsonify({"msg": "Missing database name"}), 400
    db_path = os.path.join(os.getcwd(), 'data', f'{db_name}.db')
    conn = get_db(db_path)
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    conn.close()
    return jsonify({"msg": f"Table {table_name} deleted successfully"}), 200

@app.route('/data/<table_name>', methods=['POST'])
@jwt_required()
def insert_data(table_name):
    """
    Inserts data into a specified table in a database.

    Parameters:
    table_name (str): The name of the table into which to insert data.
    db_name (str): The name of the database containing the table. This is obtained from the request JSON.
    data (dict): A dictionary containing the column names and their corresponding values to insert.

    Returns:
    flask.Response: A Flask response object containing a JSON message indicating success or failure,
    along with the ID of the last inserted row.
    If the database name or data are missing, a JSON response with an error message is returned.

    Raises:
    None
    """
    db_name = request.json.get('db_name')
    data = request.json.get('data')
    if not db_name or not data:
        return jsonify({"msg": "Missing database name or data"}), 400
    db_path = os.path.join(os.getcwd(), 'data', f'{db_name}.db')
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['?'] * len(data))
    values = list(data.values())
    conn = get_db(db_path)
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", values)
    last_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return jsonify({"msg": "Data inserted successfully", "last_id": last_id}), 200

@app.route('/data/<table_name>', methods=['GET'])
@jwt_required()
def get_data(table_name):
    """
    Retrieves data from a specified table in a database based on a given query.

    Parameters:
    table_name (str): The name of the table from which to retrieve data.
    db_name (str): The name of the database containing the table. This is obtained from the request arguments.
    query (str, optional): The SQL query to specify which rows to retrieve. If not provided, all rows will be retrieved.
    columns (str, optional): The columns to retrieve. If not provided, all columns will be retrieved.

    Returns:
    flask.Response: A Flask response object containing the retrieved data in JSON format.
    If the database name is missing, a JSON response with an error message is returned.

    Raises:
    None
    """
    db_name = request.args.get('db_name')
    query = request.args.get('query')
    columns = request.args.get('columns', '*')

    # Check if the required parameters are provided
    if not db_name:
        return jsonify({"msg": "Missing database name"}), 400

    # Construct the path to the database file
    db_path = os.path.join(os.getcwd(), 'data', f'{db_name}.db')

    # Connect to the database and execute the query
    conn = get_db(db_path)
    cursor = conn.cursor()
    if query:
        cursor.execute(f"SELECT {columns} FROM {table_name} WHERE {query}")
    else:
        cursor.execute(f"SELECT {columns} FROM {table_name}")

    # Fetch all rows from the query result
    rows = cursor.fetchall()

    # Close the database connection
    conn.close()

    # Return the retrieved data as a JSON response
    return jsonify(rows)

@app.route('/data/<table_name>', methods=['PUT'])
@jwt_required()
def update_data(table_name):
    """
    Updates data in a specified table in a database based on a given query.

    Parameters:
    table_name (str): The name of the table in which to update data.
    db_name (str): The name of the database containing the table. This is obtained from the request JSON.
    query (str): The SQL query to specify which rows to update. This is obtained from the request arguments.
    updates (dict): A dictionary containing the column names and their new values to update.

    Returns:
    flask.Response: A Flask response object containing a JSON message indicating success or failure.
    If the database name, query, or updates are missing, a JSON response with an error message is returned.

    Raises:
    None
    """
    db_name = request.json.get('db_name')
    query = request.args.get('query')
    updates = request.json.get('data')

    # Check if the required parameters are provided
    if not db_name or not updates:
        return jsonify({"msg": "Missing database name or updates"}), 400

    # Construct the path to the database file
    db_path = os.path.join(os.getcwd(), 'data', f'{db_name}.db')

    # Prepare the SQL update statement
    update_str = ', '.join([f"{key} = ?" for key in updates])
    values = list(updates.values())

    # Connect to the database and execute the update query
    conn = get_db(db_path)
    cursor = conn.cursor()
    cursor.execute(f"UPDATE {table_name} SET {update_str} WHERE {query}", values)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    # Return a success message
    return jsonify({"msg": "Data updated successfully"}), 200

@app.route('/data/<table_name>', methods=['DELETE'])
@jwt_required()
def delete_data(table_name):
    """
    Deletes data from a specified table in a database based on a given query.

    Parameters:
    table_name (str): The name of the table from which to delete data.
    db_name (str): The name of the database containing the table. This is obtained from the request arguments.
    query (str): The SQL query to specify which rows to delete. This is obtained from the request arguments.

    Returns:
    flask.Response: A Flask response object containing a JSON message indicating success or failure.
    If the database name or query is missing, a JSON response with an error message is returned.

    Raises:
    None
    """
    db_name = request.args.get('db_name')
    query = request.args.get('query')

    # Check if the required parameters are provided
    if not db_name or not query:
        return jsonify({"msg": "Missing database name or query"}), 400

    # Construct the path to the database file
    db_path = os.path.join(os.getcwd(), 'data', f'{db_name}.db')

    # Connect to the database and execute the delete query
    conn = get_db(db_path)
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM {table_name} WHERE {query}")

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    # Return a success message
    return jsonify({"msg": "Data deleted successfully"}), 200

if __name__ == '__main__':
    init_users_db()
    app.run(host='0.0.0.0', port=8080)