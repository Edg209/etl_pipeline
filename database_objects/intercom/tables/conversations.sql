create table conversations
( type              text
, id                text
, title             text
, created_at        integer
, updated_at        integer
, waiting_since     integer
, snoozed_until     integer
, open              boolean
, state             text
, read              boolean
, priority          text
, admin_assignee_id integer
, team_assignee_id  text
, constraint pk_conversations primary key (id)
)
;
