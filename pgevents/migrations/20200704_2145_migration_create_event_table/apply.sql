CREATE TYPE event_status AS ENUM ('PENDING', 'PROCESSED');

CREATE TABLE events (
    id bigserial PRIMARY KEY,
    topic varchar(50) NOT NULL,
    status event_status NOT NULL DEFAULT 'PENDING',
    payload jsonb,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    process_after timestamp with time zone NOT NULL DEFAULT now(),
    processed_at timestamp with time zone
);
