# pgevents 

[![Build Status](https://travis-ci.com/peajayni/pgevents.svg?branch=master)](https://travis-ci.com/peajayni/pgevents)
[![Coverage Status](https://coveralls.io/repos/github/peajayni/pgevents/badge.svg?branch=master&kill_cache=1)](https://coveralls.io/github/peajayni/pgevents?branch=master)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Python event framework using PostgreSQL listen/notify

## Example Usage

```
dsn = "dbname=test user=test password=test host=localhost"
channel = "foo"

app = App(dsn)

@app.register(channel)
def handler(payload):
    print(f"Received payload {payload}"

app.run()
```

Then send a notification by running the following SQL:

```
notify foo
```
