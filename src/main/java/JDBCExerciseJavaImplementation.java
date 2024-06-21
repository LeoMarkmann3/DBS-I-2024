import de.hpi.dbs1.ChosenImplementation;
import de.hpi.dbs1.ConnectionConfig;
import de.hpi.dbs1.JDBCExercise;
import de.hpi.dbs1.entities.Actor;
import de.hpi.dbs1.entities.Movie;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

@ChosenImplementation(true)
public class JDBCExerciseJavaImplementation implements JDBCExercise {

	Logger logger = Logger.getLogger(this.getClass().getSimpleName());

	@Override
	public Connection createConnection(@NotNull ConnectionConfig config) throws SQLException {
		String URL = "jdbc:postgresql://" + config.getHost() + ":" + config.getPort() + "/" + config.getDatabase();
		String name = config.getUsername();
		String pw = config.getPassword();

		Connection con = null;
		try {
			con = DriverManager.getConnection(URL, name, pw);
			System.out.println("Verbindung erfolgreich!");
		} catch (SQLException e) {
			System.out.println("Verbindung fehlgeschlagen!");
			e.printStackTrace();
			throw e;
		}
		//throw new UnsupportedOperationException("Not yet implemented");
		return con;
	}

	@Override
	public List<Movie> queryMovies(
		@NotNull Connection connection,
		@NotNull String keywords
	) throws SQLException {
		logger.info(keywords);
		List<Movie> movies = new ArrayList<>();

		// movie query
		String query1 = "SELECT \"tconst\", \"primaryTitle\", \"startYear\", \"genres\" FROM tmovies LIMIT 3;"; // Statement umschreiben
		PreparedStatement moviesQuery = connection.prepareStatement(query1);
		ResultSet rsMovies = moviesQuery.executeQuery();

		// actors query
		String queryTemplate = "SELECT \"nconst\", \"primaryName\" FROM nbasics WHERE %s"; // %s ist ein Platzhalter, Satement umschreiben
		Statement actorQuery = connection.createStatement();

		while(rsMovies.next()){
			String tconst = rsMovies.getString("tconst");
			String primaryTitle = rsMovies.getString("primaryTitle");
			int startYear = rsMovies.getInt("startYear");

			String[] genresArray = (String[]) rsMovies.getArray("genres").getArray();
			Set<String> genres = new HashSet<>(Arrays.asList(genresArray));
			Movie movie = new Movie(tconst, primaryTitle, startYear, genres);

			//add actors
			String query2 = String.format(queryTemplate, tconst);
			ResultSet rsActors = actorQuery.executeQuery(query2);

			while(rsActors.next()){
				String primaryName = rsActors.getString("primaryName");
				movie.actorNames.add(primaryName);
			}

			System.out.println(movie);
			movies.add(movie);

		}

		//throw new UnsupportedOperationException("Not yet implemented");
		return movies;
	}

	@Override
	public List<Actor> queryActors(
		@NotNull Connection connection,
		@NotNull String keywords
	) throws SQLException {
		logger.info(keywords);
		List<Actor> actors = new ArrayList<>();

		// top 5 actors
		String query1 = null;
		PreparedStatement top5 = connection.prepareStatement(query1);
		ResultSet rsTop5 = top5.executeQuery();

		// top 5 movies
		String queryTemplate1 = null;
		Statement movieQuery = connection.createStatement();

		// co-actors
		String queryTemplate2 = null;
		Statement actorQuary = connection.createStatement();

		while(rsTop5.next()){
			String nconst = rsTop5.getString("nconst");
			String primaryName = rsTop5.getString("primaryName");
			Actor actor = new Actor(nconst,primaryName);

			// add top 5 movies
			String query2 = String.format(queryTemplate1, nconst);
			ResultSet rsMovies = movieQuery.executeQuery(query2);
			while(rsMovies.next()){
				String primaryTitle = rsMovies.getString("primaryTitle");
				actor.playedIn.add(primaryTitle);
			}

			// add co-actors
			String query3 = String.format(queryTemplate2, nconst);
			ResultSet rsActors = actorQuary.executeQuery(query3);
			while(rsActors.next()){
				String primaryNameCo = rsActors.getString("primaryName");
				int count = rsActors.getInt("count");
				actor.costarNameToCount.put(primaryNameCo,count);
			}

			actors.add(actor);
		}

		//throw new UnsupportedOperationException("Not yet implemented");
		return actors;
	}
}
