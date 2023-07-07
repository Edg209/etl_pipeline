create table conversation_statistics
( conversation_id                text
, type                           text
, time_to_assignment             integer
, time_to_admin_reply            integer
, time_to_first_close            integer
, time_to_last_close             integer
, median_time_to_reply           integer
, first_contact_reply_at         integer
, first_assignment_at            integer
, first_admin_reply_at           integer
, first_close_at                 integer
, last_assignment_at             integer
, last_assignment_admin_reply_at integer
, last_contact_reply_at          integer
, last_admin_reply_at            integer
, last_close_at                  integer
, last_closed_by_id              text
, count_reopens                  integer
, count_assignments              integer
, count_conversation_parts       integer
, constraint pk_conversation_statistics primary key (conversation_id)
)
;
