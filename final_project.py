from bs4 import BeautifulSoup
import requests
import time
import json
import webbrowser as web
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sqlite3
import os
from flask import Flask, request, render_template


CACHE_FILE_NAME = 'box_Scrape.json'
CACHE_DICT = {}
DATABASE_NAME = 'movies_data.sqlite'
"""
Below are functions used for caching.
Same as prof used in class, so I won't add docstrings to them.
"""


def load_cache():
    try:
        cache_file = open(CACHE_FILE_NAME, 'r')
        cache_file_contents = cache_file.read()
        cache = json.loads(cache_file_contents)
        cache_file.close()
    except:
        cache = {}
    return cache


def save_cache(cache):
    cache_file = open(CACHE_FILE_NAME, 'w')
    contents_to_write = json.dumps(cache)
    cache_file.write(contents_to_write)
    cache_file.close()


def make_url_request_using_cache(url, cache):
    if (url in cache.keys()):  # the url is our unique key
        print("Using cache")
        return cache[url]
    else:
        print("Fetching")
        time.sleep(1)
        response = requests.get(url)
        cache[url] = response.text
        save_cache(cache)
        return cache[url]


def save_to_database(data_in_rows, table_name="Movie_Rank"):
    """
    Save data to database. If data already exists, replace it.

    params:
    List : data in rows
    String: Table name

    return:
    None
    """
    print("Saving data to database...")
    conn = sqlite3.connect(DATABASE_NAME)
    data_in_rows.to_sql(table_name, conn, index=False, if_exists='replace')
    conn.close()
    print("Data has been successfully saved.")


def save_to_database_append(data_in_rows, table_name="Movie_Info"):
    """
    Save data to database. If data already exists, append to it.

    params:
    List : data in rows
    String: Table name

    return:
    None
    """
    print("Saving data to database...")
    conn = sqlite3.connect(DATABASE_NAME)
    data_in_rows.to_sql(table_name, conn, index=False, if_exists='append')
    conn.close()
    print("Data has been successfully saved.")


def read_from_rank(function=1):
    """
    Read from Movie_Rank table.
    Function1: Just draw out the name, year, cumulative gross of top 10 movies
    Function2: Draw out all the data

    params:
    Int: function = 1/2

    return:
    DataFrame: rank
    """
    with sqlite3.connect(DATABASE_NAME) as con:
        if function == 1:
            rank = pd.read_sql("SELECT name, year, cumulative_gross FROM Movie_Rank "
                               "ORDER BY cumulative_gross DESC LIMIT 10", con=con)
            rank.cumulative_gross = np.int64(rank.cumulative_gross)
        elif function == 2:
            rank = pd.read_sql("SELECT * FROM Movie_Rank ", con=con)
        else:
            pass

    return rank


def read_from_info():
    """
    Read from Movie_Info table. Draw out all the data

    return:
    DataFrame: info
    """
    with sqlite3.connect(DATABASE_NAME) as con:
        info = pd.read_sql("SELECT * FROM Movie_Info", con=con)

    return info


def extract_box_office_data(quarter='q1'):
    """
    Extract box office data of a quarter.

    params:
    String : quarter

    return:
    Information list: data in rows
    """
    box_URL = 'https://www.boxofficemojo.com/quarter/' + quarter + '/?grossesOption=calendarGrosses'

    # Load the cache, save in global variable
    CACHE_DICT = load_cache()
    url_text = make_url_request_using_cache(box_URL, CACHE_DICT)
    soup = BeautifulSoup(url_text, 'html.parser')
    listing_divs = soup.find_all('tr')
    listing_divs.remove(listing_divs[0])

    data_in_rows = []
    print('-' * 50)  # separator
    print(quarter.upper() + ' Historical Box Office')
    print('-' * 50)  # separator
    for listing_div in listing_divs:
        alist = listing_div.find_all('a')
        year = alist[0].string
        name = alist[1].string.title()
        rank_of_year_path = alist[0]['href']
        rank_of_year_link = "https://www.boxofficemojo.com" + rank_of_year_path
        print("The #1 release of " + year + " " + quarter + " is " + name)

        money_list = listing_div.find_all('td', class_="a-text-right mojo-field-type-money")
        cumulative_gross = int(money_list[0].string.replace("$", "").replace(',', ''))
        per_release_average_gross = int(money_list[1].string.replace("$", "").replace(',', ''))
        data_in_rows.append([year, quarter, name, cumulative_gross, per_release_average_gross, rank_of_year_link])

    return data_in_rows


def draw_graph(x, y):
    """
    Given data of x, y, draw a bar graph of the top 10 movies.

    params:
    List: x, y

    return:
    None
    """
    plt.figure(figsize=(25, 9))
    plt.bar(x=x, height=y, label='cumulative_gross',
            color='blue', alpha=0.6)
    plt.xticks(rotation=-30)
    plt.tick_params(axis='x', labelsize=6.5)
    plt.title('top 10 movies')
    plt.ylabel('gross($)')
    plt.show()


def print_query_result(raw_query_result):
    """ Pretty prints raw query result

    Parameters
    List: a list of tuples that represent raw query result

    Returns
    None
    """
    # TODO Implement function
    row = len(raw_query_result)
    column = max(len(item) for item in raw_query_result)
    row0 = "+" + 20 * "-" + "+" + 50 * "-" + "+"
    newform = row0

    for i in range(row):
        newform = newform + "\n" + "|"
        for j in range(column):
            if j == 0:
                if len(str(raw_query_result[i][j])) <= 20:
                    s = "{:^20}".format(raw_query_result[i][j]) + "|"
                else:
                    s = "{:^20}".format(raw_query_result[i][j])[0:17] + "...|"
                newform += s
            else:
                if len(str(raw_query_result[i][j])) <= 50:
                    s = "{:^50}".format(raw_query_result[i][j]) + "|"
                else:
                    s = "{:^50}".format(raw_query_result[i][j])[0:47] + "...|"
                newform += s

    newform = newform + "\n" + row0
    print(newform)


def form_tuple_list(data1):
    """
    Turn data into list of tuple, for the use of print_query_result()

    params:
    List: data1   eg.['tom and jerry', '2021', ...]

    return:
    List: list of tuple   eg.[('Title', 'tom and jerry'), ('Released', '2021'), ...]
    """
    list_of_tuple = []
    list_of_tuple.append(('Title', data1[0].title()))
    list_of_tuple.append(('Released', data1[1]))
    list_of_tuple.append(('Runtime', data1[2]))
    list_of_tuple.append(('Genre', data1[3]))
    list_of_tuple.append(('Director', data1[4]))
    list_of_tuple.append(('Actors', data1[5]))
    list_of_tuple.append(('Language', data1[6]))
    list_of_tuple.append(('Country', data1[7]))
    list_of_tuple.append(('Awards', data1[8]))

    return list_of_tuple


def get_movie_info(name):
    """
    Extract movie info data.

    params:
    String : name

    return:
    List: data    eg.['tom and jerry', '2021', ...]
    List: list of tuple    eg.[('Title', 'tom and jerry'), ('Released', '2021'), ...]
    """
    omdb_url = "http://www.omdbapi.com"
    CACHE_DICT = load_cache()
    detail_link = omdb_url + "/?t=" + name.replace(" ", "+") + "&apikey=4cf891f"
    url_text = make_url_request_using_cache(detail_link, CACHE_DICT)
    content1 = json.loads(url_text)
    list_of_tuple = []

    if content1['Response'] == 'False':
        print("Sorry, no valid data for " + name)
        pass
    else:
        datattt = [content1['Title'].title(), content1['Released'], content1['Runtime'], content1['Genre'],
                 content1['Director'], content1['Actors'], content1['Language'], content1['Country'], content1['Awards']]
        list_of_tuple = form_tuple_list(datattt)

    data = [tp[1] for tp in list_of_tuple]

    return data, list_of_tuple


class Filter:
    def __init__(self, data_df):
        self.data = data_df

    def choose_by_genres(self, genre_list=['Drama']):
        if len(genre_list) == 0:
            return self.data.released == self.data.released
        temp = self.data.genre.apply(lambda r: genre_list[0] in r)
        if len(genre_list) > 1:
            for genre in genre_list[1:]:
                temp = np.logical_or(temp, self.data.genre.apply(lambda r: genre in r))

        return temp


# save data into 2 tables
if os.path.exists(DATABASE_NAME):
    pass
else:
    quarters = ['q1', 'q2', 'q3', 'q4']
    data_in_rows = []
    for q in quarters:
        d = extract_box_office_data(q)
        data_in_rows.extend(d)
    df = pd.DataFrame(data_in_rows, columns=['year', 'quarter', 'name', 'cumulative_gross',
                                             'per_release_average_gross', 'rank_of_year_link'])
    save_to_database(df, table_name="Movie_Rank")
    r = read_from_rank(2)
    fetch_list = [r.name[i] for i in range(len(r))]
    data_in_rows1 = []
    for t in fetch_list:
        data1, list_of_tuple1 = get_movie_info(t)
        data_in_rows1.append(data1)
        df = pd.DataFrame(data_in_rows1, columns=['name', 'released', 'runtime', 'genre', 'director',
                                                  'actors', 'language', 'country', 'awards'])
        save_to_database(df, table_name="Movie_Info")


# interaction starts
print('-' * 50)  # separator
g = str(input('Would you like to check the bar chart of the top 10 movies with highest cumulative_gross \n'
              'to help you to pick a movie you may be interested in? (answer: yes/no):'))
if g == 'yes':
    rank = read_from_rank(1)
    hori = [f"{rank.name[i]}\n({rank.year[i]})" for i in range(len(rank))]
    draw_graph(hori, rank.cumulative_gross)
else:
    pass

while 1:
    print('-' * 50)  # separator
    x = str(input('Did you find a movie you are interested in? (answer: yes/no/leave):'))
    if x == "yes":
        print('-' * 50)  # separator
        y = str(input('Enter the name of the movie, I will show you more info:')).title()
        print('-' * 50)  # separator
        with sqlite3.connect(DATABASE_NAME) as con:
            table = pd.read_sql("SELECT * FROM Movie_Info", con=con)
            if y in table.name.values:
                info = pd.read_sql("SELECT * FROM Movie_Info WHERE name='" + y + "'", con=con)
                data3 = list(info.iloc[0, ].values)
                data_q = form_tuple_list(data3)
                print_query_result(data_q)
            else:
                data2, list_of_tuple2 = get_movie_info(y)
                df = pd.DataFrame(np.array(data2).reshape((1, 9)), columns=['name', 'released', 'runtime', 'genre',
                                                                            'director', 'actors', 'language', 'country',
                                                                            'awards'])
                save_to_database_append(df, table_name="Movie_Info")
                print_query_result(list_of_tuple2)
        exit()

    elif x == 'no':
        print("Maybe I can help you to find one :)")
        while 1:
            print('-' * 50)  # separator
            a = str(input('Enter a year that you want to know more about the rank of release(range: 1978-2021)\n'
                          'or "leave" if you do not need that:'))
            if a == 'leave':
                break
            else:
                with sqlite3.connect(DATABASE_NAME) as con:
                    link = pd.read_sql("SELECT rank_of_year_link FROM Movie_Rank WHERE (year="
                                       + a + " AND quarter='q1')", con=con)
                    link = link.values[0][0]
                    web.open(link)
                    print('-' * 50)  # separator
                    print("Launching \n" + link + "\nin web browser...")
    else:
        break


print('-' * 50)  # separator
print("Congratulations if you've find a movie you like!\n"
      "Sorry if you don't, maybe you can check the link below and try the genre filter\n"
      "Detailed info of some movies will be provided!")
print('-' * 50)  # separator

# flask app
GENRES = ['Action', 'Adventure', 'Animation', 'Biography', 'Comedy', 'Crime', 'Drama', 'Family', 'Fantasy', 'Film-Noir',
          'History', 'Horror', 'Music', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Sport', 'Thriller', 'War', 'Western']
movies = read_from_info()
f1 = Filter(movies)
app = Flask(__name__)


@app.route('/')
def my_form():
    return render_template('pick.html')


@app.route("/", methods=['POST'])
def test():
    """
    Extract movies of selected genres from data.

    params:
    None

    return:
    HTML: Detailed information of movies selected will be displayed in the web page.
    """
    checked_genres = []
    for genre in GENRES:
        if request.form.get(genre):
            checked_genres.append(genre)
    mask = f1.choose_by_genres(checked_genres)
    subset = movies[mask].reset_index(drop=True)

    return subset.to_html()


app.run()