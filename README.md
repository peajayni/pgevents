# pgevents 

[![Build Status](https://travis-ci.com/peajayni/pgevents.svg?branch=master)](https://travis-ci.com/peajayni/pgevents)
[![Coverage Status](https://coveralls.io/repos/github/peajayni/pgevents/badge.svg?branch=master&kill_cache=1)](https://coveralls.io/github/peajayni/pgevents?branch=master)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Python event framework using PostgreSQL listen/notify

## Example Usage

```python
from pgevents import App

dsn = "dbname=test user=test password=test host=localhost"
channel = "foo"     # Postgres channel to listen for notifications on
topic = "bar"       # Event topic for handler to respond to

app = App(dsn, channel)


@app.register(topic)
def handler(context):
    print("Handling event")


app.run()
```

Create an event entry

```sql
INSERT INTO events (topic)
VALUES('bar');
```

Then send a notification by running the following SQL:

```sql
NOTIFY foo;
```
