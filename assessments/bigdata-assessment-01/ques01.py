# Connect to MySQL server
mydb = mysql.connector.connect(
  host="localhost",
  user="yourusername",
  password="yourpassword",
  database="movielens"
)

# Function to add a new movie to the movies table
def add_movie():
    title = input("Enter the title of the movie: ")
    release_year = input("Enter the release year of the movie: ")
    imdb_url = input("Enter the IMDb URL of the movie: ")

    cursor = mydb.cursor()
    sql = "INSERT INTO movies (title, release_year, imdb_url) VALUES (%s, %s, %s)"
    val = (title, release_year, imdb_url)
    cursor.execute(sql, val)
    mydb.commit()
    print(cursor.rowcount, "movie added successfully")

# Function to view movie details by movie id
def view_movie():
    movie_id = input("Enter the movie id: ")

    cursor = mydb.cursor()
    sql = "SELECT * FROM movies WHERE movie_id = %s"
    val = (movie_id,)
    cursor.execute(sql, val)
    result = cursor.fetchone()
    if result:
        print("Movie ID:", result[0])
        print("Title:", result[1])
        print("Release Year:", result[2])
        print("IMDb URL:", result[3])
    else:
        print("No movie found with ID", movie_id)

# Function to update movie details by movie id
def update_movie():
    movie_id = input("Enter the movie id: ")
    title = input("Enter the new title of the movie: ")
    release_year = input("Enter the new release year of the movie: ")
    imdb_url = input("Enter the new IMDb URL of the movie: ")

    cursor = mydb.cursor()
    sql = "UPDATE movies SET title = %s, release_year = %s, imdb_url = %s WHERE movie_id = %s"
    val = (title, release_year, imdb_url, movie_id)
    cursor.execute(sql, val)
    mydb.commit()
    print(cursor.rowcount, "movie updated successfully")

# Function to delete movie details by movie id
def delete_movie():
    movie_id = input("Enter the movie id: ")

    cursor = mydb.cursor()
    sql = "DELETE FROM movies WHERE movie_id = %s"
    val = (movie_id,)
    cursor.execute(sql, val)
    mydb.commit()
    print(cursor.rowcount, "movie deleted successfully")

# Function to list all movie details
def list_movies():
    cursor = mydb.cursor()
    cursor.execute("SELECT * FROM movies")
    results = cursor.fetchall()
    if results:
        for result in results:
            print("Movie ID:", result[0])
            print("Title:", result[1])
            print("Release Year:", result[2])
            print("IMDb URL:", result[3])
            print("")
    else:
        print("No movies found")

# Main program loop
while True:
    print("")
    print("Please select an option:")
    print("1. Add new movie")
    print("2. View movie details by movie id")
    print("3. Update movie details by movie id")
    print("4. Delete movie details by movie id")
    print("5. List all movie details")
    print("6. Exit")
    choice = input()

    if choice == "1":
        add_movie()
    elif choice == "2":
        view_movie()
    elif choice == "3":
        update_movie()
