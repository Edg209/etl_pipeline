create table conversation_ratings
( conversation_id text
, rating          integer
, remark          text
, created_at      integer
, contact         text
, teammate        text
, constraint pk_conversation_ratings primary key (conversation_id)
)
;
