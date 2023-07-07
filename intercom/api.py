"""This file contains each of the """
import os
import requests
import time
import logging

logger = logging.getLogger('Intercom API')


class APIErrorException(Exception):
    """An exception that is raised when we are unable to query the API"""


def get_conversation_max_updated_timestamp(start_timestamp: str, retries: int = 5, backoff=5) -> str:
    """Get the most recent value of updated_at in the first page of search results."""
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {os.environ['INTERCOM_API_TOKEN']}"
    }
    url = "https://api.intercom.io/conversations/search"
    # If we fail when querying the API, we will try again until we reach the retry limit
    failed_attempts = 0
    while failed_attempts <= retries:
        payload = {
            "query": {
                "field": "updated_at",
                "operator": ">",
                "value": start_timestamp
            }
        }
        response = requests.post(url, json=payload, headers=headers)
        if response.headers['Status'] == "200 OK":
            break
        else:
            logger.info("API query failed, backing off and trying again.")
            failed_attempts += 1
            time.sleep(backoff)
    else:
        logger.error("Unable to query API, raising exception.")
        raise APIErrorException
    response_json = response.json()
    updated_at_values = [conv["updated_at"] for conv in response_json["conversations"] if conv["source"]["type"] == "conversation"]
    max_updated_at = max(updated_at_values) if len(updated_at_values) > 0 else None
    return max_updated_at


def get_conversation_ids_last_updated_between_timestamps(start_timestamp: str, end_timestamp: str, retries: int = 5, backoff=5) -> list[str]:
    """List all conversations that have a last_updated strictly greater than the start timestamp and less than or equal to the end timestamp."""
    # We will potentially be making several API calls
    # The URL and headers are consistent between API calls
    # The payload will vary only on what page is requested
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {os.environ['INTERCOM_API_TOKEN']}"
    }
    url = "https://api.intercom.io/conversations/search"

    def get_payload(page):
        return {"query": {
            "operator": "AND",
            "value": [
                {
                    "field": "updated_at",
                    "operator": ">",
                    "value": start_timestamp
                },
                {
                    "operator": "OR",
                    "value": [
                        {
                            "field": "updated_at",
                            "operator": "=",
                            "value": end_timestamp
                        },
                        {
                            "field": "updated_at",
                            "operator": "<",
                            "value": end_timestamp
                        }
                    ]
                }
            ]
        },
            "pagination": {"page": page}
        }

    # There exists a limit of the number of conversations that can be retrieved per conversation, so we will need to potentially make several API calls
    # We will make API calls until one of two conditions is met
    # If we have reached our limit of failed attempts, we will raise an error and give up
    # If we have successfully retrieved all pages, we will extract the list of conversation ids and return it.
    failed_attempts = 0
    conversation_ids = []
    # There is a corner case where conversations can be updated in between API calls
    # This may cause the data set to shrink between API calls
    # We will therefore start at the last page and end at the first
    # This may cause a single conversation to appear in the list more than once, but that is safe to happen
    while failed_attempts <= retries:
        payload = get_payload(1)
        response = requests.post(url, json=payload, headers=headers)
        if response.headers['Status'] == "200 OK":
            response_json = response.json()
            total_pages = response_json["pages"]["total_pages"]
            logger.debug(f"{total_pages} API page(s) found")
            break
        else:
            logger.info("API query failed, backing off and trying again.")
            failed_attempts += 1
            time.sleep(backoff)
    else:
        logger.error("Unable to query API, raising exception.")
        raise APIErrorException
    # Now that we have our total number of pages, we start going from the last page to the first
    current_page = total_pages
    while failed_attempts <= retries:
        payload = get_payload(current_page)
        response = requests.post(url, json=payload, headers=headers)
        if response.headers['Status'] == "200 OK":
            logger.debug(f"Retrieved data for page {current_page}")
            # We parse the results of the API call to retrieve the conversation IDs and compare the current page to the total number of pages
            response_json = response.json()
            new_conversation_ids = [conv["id"] for conv in response_json["conversations"] if conv["source"]["type"] == "conversation"]
            conversation_ids += new_conversation_ids
            if current_page > 1:
                current_page -= 1
            else:
                break
        else:
            logger.info("API query failed, backing off and trying again.")
            failed_attempts += 1
            time.sleep(backoff)
    else:
        logger.error("Unable to query API, raising exception.")
        raise APIErrorException
    return conversation_ids


def get_conversation_details(conversation_id: str, retries: int = 5, backoff=5) -> dict:
    """Use the API to return a dictionary that represents the JSON containing the details of the conversation history"""
    url = f"https://api.intercom.io/conversations/{conversation_id}"

    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {os.environ['INTERCOM_API_TOKEN']}"
    }
    # If we fail when querying the API, we will try again until we reach the retry limit
    failed_attempts = 0
    while failed_attempts <= retries:
        response = requests.get(url, headers=headers)
        if response.headers['Status'] == "200 OK":
            return response.json()
        else:
            logger.info("API query failed, backing off and trying again.")
            failed_attempts += 1
            time.sleep(backoff)
    else:
        logger.error("Unable to query API, raising exception.")
        raise APIErrorException
