CREATE TYPE event_status AS ENUM ('PENDING', 'PROCESSED');

CREATE TABLE events (
    id bigserial PRIMARY KEY,
    topic varchar(50) NOT NULL,
    status event_status NOT NULL DEFAULT 'PENDING',
    payload json,
    created_at timestamp NOT NULL DEFAULT now(),
    processed_at timestamp
);
