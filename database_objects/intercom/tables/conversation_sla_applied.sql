create table conversation_sla_applied
( conversation_id text
, type            text
, sla_name        text
, sla_status      text
, constraint pk_conversation_sla_applied primary key (conversation_id)
)
;
