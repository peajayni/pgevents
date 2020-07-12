import pathlib


class MockApp:
    """
    Mock app to be used with the click cli test runner.

    As the runner calls the cli in a different process we cannot
    use unittest.mock. Instead each method will create a file when
    it is called which can be used to assert that they were indeed
    called.
    """

    def run(self):
        pathlib.Path("run").touch()

    def init_db(self):
        pathlib.Path("init_db").touch()

    def create_event(self, topic, payload):
        pathlib.Path("create_event").touch()

    def assert_called(self, method):
        name = method.__name__
        assert pathlib.Path(name).exists(), f"Method {name} was not called"


app = MockApp()
