from flask import Flask, render_template, request, redirect, url_for
from flask_wtf import Form
from wtforms import StringField
from PySparkReport import *

app = Flask(__name__)
app.config['SECRET_KEY'] = 'our very hard to guess secretfir'

# strona z formularzem do wpisania id artysty
@app.route('/search', methods=['GET', 'POST'])
def search():
    error = ""
    if request.method == 'POST': 
        # pobranie danych z formularza
        artist_id = request.form['artist_id']

        # walidacja - sprawdzenie długości id
        if len(artist_id) != 22 :
            # niepoprawny id
            error = "ID too short. Please supply valid artist ID"
        else:
            # poprawne dane - przekierowanie do raportu
            return redirect(url_for('report'))

    return render_template('search.html', message=error)

@app.route('/report', methods=['GET', 'POST'])
def report():
    # wywołanie funkcji generujących raport dla artysty
    genres = generate_genres(request.form['artist_id'])
    albums = generate_albums(request.form['artist_id'])
    tracks = generate_tracks(request.form['artist_id'])
    return render_template('report.html',  tables=[genres.to_html(classes='data', header="true"),
                                                     albums.to_html(classes='data', header="true"),
                                                     tracks.to_html(classes='data', header="true")])

# uruchomienie aplikacji
# app.run(debug=True)
