from pgevents.app import App


def test_app():
    dsn = "dbname=postgres user=postgres password=postgres host=integration_db"
    app = App(dsn)
    cursor = app.connection.cursor()
    cursor.execute("SELECT 1")
