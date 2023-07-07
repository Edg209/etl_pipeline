create table conversation_sources
( conversation_id text
, type            text
, id              text
, delivered_as    text
, subject         text
, body            text
, author          text
, attachments     text
, url             text
, redacted        boolean
, constraint pk_conversation_sources primary key (conversation_id)
)
;
