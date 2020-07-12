from pgevents import data_access


class Context:
    def __init__(self, event, cursor):
        self.event = event
        self.cursor = cursor

    def create_event(self, event):
        return data_access.create_event(self.cursor, event)
