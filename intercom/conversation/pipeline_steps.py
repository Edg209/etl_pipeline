from pipeline.pipeline import Payload, PipelineStep
from intercom.api import get_conversation_details
from database.database_operations import load_dataframe_to_table, merge_dataframe_into_table
import pandas as pd


class RetrieveApiData(PipelineStep):
    def execute(self, payload: Payload):
        payload.api_data = get_conversation_details(payload.conversation_id)


class ParseApiData(PipelineStep):
    def execute(self, payload: Payload):
        # We will be constructing dataframes that correspond to the data we want to load in each table
        # The majority of these dataframes will have a single record
        conversations = [{'type': payload.api_data['type'],
                          'id': payload.api_data['id'],
                          'title': payload.api_data['title'],
                          'created_at': payload.api_data['created_at'],
                          # For updated_at, we have to limit it to no greater than end_timestamp to ensure that future pipeline runs are successful
                          'updated_at': min(payload.api_data['updated_at'], payload.end_timestamp),
                          'waiting_since': payload.api_data['waiting_since'],
                          'snoozed_until': payload.api_data['snoozed_until'],
                          'open': payload.api_data['open'],
                          'state': payload.api_data['state'],
                          'read': payload.api_data['read'],
                          'priority': payload.api_data['priority'],
                          'admin_assignee_id': payload.api_data['admin_assignee_id'],
                          'team_assignee_id': payload.api_data['team_assignee_id']
                          }]
        payload.conversations = pd.DataFrame(conversations)
        # The api data contains several complex objects that may contain complex data
        # These will be loaded to their own tables
        # In this current version of the pipeline, we will flatten these to a single text field containing the object as a json
        # Future developments are anticipated where we will also process these other objects, unflattening these will be part of those future developments
        # Not all of these are guaranteed to exist, so we will have to check for existence first
        if payload.api_data['tags'] is not None:
            conversation_tags = [{'conversation_id': payload.conversation_id,
                                  'type': payload.api_data['tags']['type'],
                                  'tags': str(payload.api_data['tags']['tags'])
                                  }]
        else:
            conversation_tags = []
        payload.conversation_tags = pd.DataFrame(conversation_tags)
        if payload.api_data['conversation_rating'] is not None:
            conversation_ratings = [{'conversation_id': payload.conversation_id,
                                     'rating': payload.api_data['conversation_rating']['rating'],
                                     'remark': payload.api_data['conversation_rating']['remark'],
                                     'created_at': payload.api_data['conversation_rating']['created_at'],
                                     'contact': str(payload.api_data['conversation_rating']['contact']),
                                     'teammate': str(payload.api_data['conversation_rating']['teammate'])
                                     }]
        else:
            conversation_ratings = []
        payload.conversation_ratings = pd.DataFrame(conversation_ratings)
        if payload.api_data['source'] is not None:
            conversation_sources = [{'conversation_id': payload.conversation_id,
                                     'type': payload.api_data['source']['type'],
                                     'id': payload.api_data['source']['id'],
                                     'delivered_as': payload.api_data['source']['delivered_as'],
                                     'subject': payload.api_data['source']['subject'],
                                     'body': payload.api_data['source']['body'],
                                     'author': str(payload.api_data['source']['author']),
                                     'attachments': str(payload.api_data['source']['attachments']),
                                     'url': payload.api_data['source']['url'],
                                     'redacted': payload.api_data['source']['redacted'],
                                     }]
        else:
            conversation_sources = []
        payload.conversation_sources = pd.DataFrame(conversation_sources)
        if payload.api_data['contacts'] is not None:
            conversation_contacts = [{'conversation_id': payload.conversation_id,
                                      'type': payload.api_data['contacts']['type'],
                                      'contacts': str(payload.api_data['contacts']['contacts'])
                                      }]
        else:
            conversation_contacts = []
        payload.conversation_contacts = pd.DataFrame(conversation_contacts)
        if payload.api_data['teammates'] is not None:
            conversation_teammates = [{'conversation_id': payload.conversation_id,
                                       'teammates': str(payload.api_data['teammates'])
                                       }]
        else:
            conversation_teammates = []
        payload.conversation_teammates = pd.DataFrame(conversation_teammates)
        if payload.api_data['first_contact_reply'] is not None:
            conversation_first_contact_replies = [{'conversation_id': payload.conversation_id,
                                                   'created_at': payload.api_data['first_contact_reply']['created_at'],
                                                   'type': payload.api_data['first_contact_reply']['type'],
                                                   'url': payload.api_data['first_contact_reply']['url']
                                                   }]
        else:
            conversation_first_contact_replies = []
        payload.conversation_first_contact_replies = pd.DataFrame(conversation_first_contact_replies)
        if payload.api_data['sla_applied'] is not None:
            conversation_sla_applied = [{'conversation_id': payload.conversation_id,
                                         'sla_name': payload.api_data['sla_applied']['sla_name'],
                                         'sla_status': payload.api_data['sla_applied']['sla_status']
                                         }]
        else:
            conversation_sla_applied = []
        payload.conversation_sla_applied = pd.DataFrame(conversation_sla_applied)
        if payload.api_data['statistics'] is not None:
            conversation_statistics = [{'conversation_id': payload.conversation_id,
                                        'time_to_assignment': payload.api_data['statistics']['time_to_assignment'],
                                        'time_to_admin_reply': payload.api_data['statistics']['time_to_admin_reply'],
                                        'time_to_first_close': payload.api_data['statistics']['time_to_first_close'],
                                        'time_to_last_close': payload.api_data['statistics']['time_to_last_close'],
                                        'median_time_to_reply': payload.api_data['statistics']['median_time_to_reply'],
                                        'first_contact_reply_at': payload.api_data['statistics']['first_contact_reply_at'],
                                        'first_assignment_at': payload.api_data['statistics']['first_assignment_at'],
                                        'first_admin_reply_at': payload.api_data['statistics']['first_admin_reply_at'],
                                        'first_close_at': payload.api_data['statistics']['first_close_at'],
                                        'last_assignment_at': payload.api_data['statistics']['last_assignment_at'],
                                        'last_assignment_admin_reply_at': payload.api_data['statistics']['last_assignment_admin_reply_at'],
                                        'last_contact_reply_at': payload.api_data['statistics']['last_contact_reply_at'],
                                        'last_admin_reply_at': payload.api_data['statistics']['last_admin_reply_at'],
                                        'last_close_at': payload.api_data['statistics']['last_close_at'],
                                        'last_closed_by_id': payload.api_data['statistics']['last_closed_by_id'],
                                        'count_reopens': payload.api_data['statistics']['count_reopens'],
                                        'count_assignments': payload.api_data['statistics']['count_assignments'],
                                        'count_conversation_parts': payload.api_data['statistics']['count_conversation_parts']
                                        }]
        else:
            conversation_statistics = []
        payload.conversation_statistics = pd.DataFrame(conversation_statistics)
        # conversation_parts is unlike the others as the dataframe may contain more than one record
        if payload.api_data['conversation_parts'] is not None:
            conversation_parts = [{
                'conversation_id': payload.conversation_id,
                'type': part['type'],
                'id': part['id'],
                'part_type': part['part_type'],
                'body': part['body'],
                'created_at': part['created_at'],
                'updated_at': part['updated_at'],
                'notified_at': part['notified_at'],
                'assigned_to': part['assigned_to'],
                'author': str(part['author']),
                'attachments': part['attachments'],
                'external_id': part['external_id'],
                'redacted': part['redacted']
            } for part in payload.api_data['conversation_parts']['conversation_parts']]
        else:
            conversation_parts = []
        payload.conversation_parts = pd.DataFrame(conversation_parts)


class LoadToDatabase(PipelineStep):
    def execute(self, payload: Payload):
        # For the majority of the data, we expect the API data to completely contain all the data
        # Deleting based on conversation_id and reinserting will guarantee that we have the most recent data and not any old data remaining
        payload.cur.execute(f"delete from {payload.db_schema}.conversations where id = %s", (payload.conversation_id,))
        load_dataframe_to_table(schema_name=payload.db_schema, table_name="conversations", df=payload.conversations, cur=payload.cur)
        payload.cur.execute(f"delete from {payload.db_schema}.conversation_contacts where conversation_id = %s", (payload.conversation_id,))
        load_dataframe_to_table(schema_name=payload.db_schema, table_name="conversation_contacts", df=payload.conversation_contacts, cur=payload.cur)
        payload.cur.execute(f"delete from {payload.db_schema}.conversation_first_contact_replies where conversation_id = %s", (payload.conversation_id,))
        load_dataframe_to_table(schema_name=payload.db_schema, table_name="conversation_first_contact_replies", df=payload.conversation_first_contact_replies, cur=payload.cur)
        payload.cur.execute(f"delete from {payload.db_schema}.conversation_ratings where conversation_id = %s", (payload.conversation_id,))
        load_dataframe_to_table(schema_name=payload.db_schema, table_name="conversation_ratings", df=payload.conversation_ratings, cur=payload.cur)
        payload.cur.execute(f"delete from {payload.db_schema}.conversation_sla_applied where conversation_id = %s", (payload.conversation_id,))
        load_dataframe_to_table(schema_name=payload.db_schema, table_name="conversation_sla_applied", df=payload.conversation_sla_applied, cur=payload.cur)
        payload.cur.execute(f"delete from {payload.db_schema}.conversation_sources where conversation_id = %s", (payload.conversation_id,))
        load_dataframe_to_table(schema_name=payload.db_schema, table_name="conversation_sources", df=payload.conversation_sources, cur=payload.cur)
        payload.cur.execute(f"delete from {payload.db_schema}.conversation_statistics where conversation_id = %s", (payload.conversation_id,))
        load_dataframe_to_table(schema_name=payload.db_schema, table_name="conversation_statistics", df=payload.conversation_statistics, cur=payload.cur)
        payload.cur.execute(f"delete from {payload.db_schema}.conversation_tags where conversation_id = %s", (payload.conversation_id,))
        load_dataframe_to_table(schema_name=payload.db_schema, table_name="conversation_tags", df=payload.conversation_tags, cur=payload.cur)
        payload.cur.execute(f"delete from {payload.db_schema}.conversation_teammates where conversation_id = %s", (payload.conversation_id,))
        load_dataframe_to_table(schema_name=payload.db_schema, table_name="conversation_teammates", df=payload.conversation_teammates, cur=payload.cur)
        # conversation_parts is more complicated, as we do not expect the api to return the entire conversation history, only a segment of the most recent parts
        # For this reason, we cannot delete all the previous rows before reinserting, as this will lose data
        # Instead, we will insert, and use the primary key of the table to update if the inserted row matches one in the table on the primary ket columns
        merge_dataframe_into_table(schema_name=payload.db_schema, table_name='conversation_parts', df=payload.conversation_parts, cur=payload.cur)
