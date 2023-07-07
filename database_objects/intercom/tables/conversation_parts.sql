create table conversation_parts
( conversation_id text
, type            text
, id              text
, part_type       text
, body            text
, created_at      integer
, updated_at      integer
, notified_at     integer
, assigned_to     text
, author          text
, attachments     text
, external_id     text
, redacted        boolean
, constraint pk_conversation_parts primary key (conversation_id, id)
)
;
