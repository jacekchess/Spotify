from flask import Flask, render_template, request, redirect, url_for
from flask_wtf import Form
from wtforms import StringField
from PySparkReport import *

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret_key'

@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'POST':
        # przekierowanie do strony z raportem
        return redirect(url_for('report'))

    return render_template('search.html', message=error)

@app.route('/report', methods=['GET', 'POST'])
def report():
    artist_id = request.form['artist_id']
    # obsługa identyfikatora zbyt krótkiego/długiego
    if len(artist_id) != 22:
        return render_template('report.html')
    genres = generate_genres(artist_id) # artyści tworzący w tych samych gatunkach
    albums = generate_albums(artist_id) # albumy
    tracks = generate_tracks(artist_id) # utwory
    # obsługa identyfikatora którego nie ma w bazie (nie ma gwarancji, że jest poprawny; ma tylko odpowiednią długość)
    if isinstance(genres, int): 
    	return render_template('report.html', artist_id=artist_id)
    else: 
        # obsługa identyfikatora będącego w bazie
        albums.reset_index(inplace=True, drop=True)
        return render_template('report.html',  tables=[genres.to_html(classes='data', header="true", index=False),
                                                       albums.to_html(classes='data', header="true"),
                                                       tracks.to_html(classes='data', header="true")])

# app.run(debug=True)
