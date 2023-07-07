create table conversation_first_contact_replies
( conversation_id text
, created_at      integer
, type            text
, url             text
, constraint pk_conversation_first_contact_replies primary key (conversation_id)
)
;
