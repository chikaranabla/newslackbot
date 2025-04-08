import json
import traceback
from typing import Mapping
from werkzeug import Request, Response
from dify_plugin import Endpoint
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NewSlackBotEndpoint(Endpoint):
    def _invoke(self, r: Request, values: Mapping, settings: Mapping) -> Response:
        logger.debug(f"Received request: Method={r.method}, Path={r.path}")
        logger.debug(f"Request headers: {r.headers}")

        try:
            raw_data = r.get_data(as_text=True)
            logger.debug(f"Raw request body: {raw_data}")
        except Exception as e:
            logger.error(f"Failed to get raw request body: {e}", exc_info=True)
            raw_data = None

        retry_num = r.headers.get("X-Slack-Retry-Num")
        allow_retry = settings.get("allow_retry", False)
        logger.debug(f"Allow Retry setting: {allow_retry}")
        if not allow_retry and (r.headers.get("X-Slack-Retry-Reason") == "http_timeout" or ((retry_num is not None and int(retry_num) > 0))):
            logger.info("Ignoring Slack retry request based on headers.")
            return Response(status=200, response="ok")

        data = None
        try:
            if raw_data and raw_data.strip():
                data = json.loads(raw_data)
            else:
                logger.warning("Request body is empty.")
                return Response(status=200, response="ok")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}", exc_info=True)
            return Response(status=400, response="Bad Request: Invalid JSON.")
        except Exception as e:
            logger.error(f"Unexpected error during JSON parsing: {e}", exc_info=True)
            return Response(status=500, response="Internal Server Error during JSON parsing.")

        if not isinstance(data, dict):
            logger.warning(f"Parsed data is not a dictionary. Type: {type(data)}, Data: {data}")
            return Response(status=400, response="Bad Request: Expected JSON object.")

        logger.info(f"Parsed Slack event data: {json.dumps(data, indent=2, ensure_ascii=False)}")

        if data.get("type") == "url_verification":
            challenge_code = data.get("challenge")
            if challenge_code:
                logger.info(f"Handling URL verification, challenge code: {challenge_code}")
                return Response(
                    response=json.dumps({"challenge": challenge_code}),
                    status=200,
                    content_type="application/json"
                )
            else:
                logger.warning("URL verification request received without challenge code.")
                return Response(status=400, response="Bad Request: Missing challenge code.")

        if data.get("type") == "event_callback":
            event = data.get("event")
            if not event or not isinstance(event, dict):
                logger.warning("No 'event' field or invalid format in event_callback.")
                return Response(status=200, response="ok")

            event_type = event.get("type")
            logger.info(f"Received event type: {event_type}")

            if event_type == "app_mention":
                message = event.get("text", "")
                user_id = event.get("user")
                channel_id = event.get("channel")
                ts = event.get("ts")
                thread_ts = event.get("thread_ts", ts)

                if not all([user_id, channel_id, ts]):
                     logger.warning(f"Missing user, channel, or ts in app_mention event: {event}")
                     return Response(status=200, response="ok")

                logger.info(f"Received app_mention in channel {channel_id} from user {user_id}. Message TS: {ts}, Thread TS: {thread_ts}. Text: {message}")

                query_text = ""
                bot_user_id = data.get("authorizations", [{}])[0].get("user_id")
                api_app_id = data.get("api_app_id")

                if bot_user_id:
                    mention_prefix = f"<@{bot_user_id}>"
                    logger.debug(f"Using mention prefix based on authorization user_id: {mention_prefix}")
                elif api_app_id:
                     logger.warning(f"Could not find bot user_id in authorizations. Using api_app_id {api_app_id} for logging.")
                     mention_prefix = None
                else:
                    logger.warning("Could not determine bot's user_id or api_app_id from event data.")
                    mention_prefix = None

                processed_message = message.strip()
                if mention_prefix and processed_message.startswith(mention_prefix):
                    query_text = processed_message[len(mention_prefix):].strip()
                    logger.info(f"Extracted query using bot_user_id prefix: '{query_text}'")
                elif mention_prefix is None and processed_message.startswith("<@"):
                    parts = processed_message.split(">", 1)
                    if len(parts) > 1:
                        query_text = parts[1].strip()
                        logger.info(f"Extracted query using generic mention check: '{query_text}'")
                    else:
                         query_text = ""
                         logger.info("Extracted empty query (generic mention only).")
                else:
                    logger.warning(f"app_mention event text does not start with expected mention. BotPrefix: {mention_prefix}, Text: {message}")
                    return Response(status=200, response="ok")


                dify_app_config = settings.get("app")
                bot_token = settings.get("bot_token")

                if not bot_token:
                     logger.error("Bot Token is not configured in the plugin settings.")
                     return Response(status=500, response="Bot Token is not configured.")

                if not dify_app_config:
                    error_message = "Dify App is not configured in the plugin settings."
                    logger.error(error_message)
                    try:
                        client = WebClient(token=bot_token)
                        client.chat_postMessage(channel=channel_id, text=error_message, thread_ts=thread_ts)
                    except Exception as slack_e:
                       logger.error(f"Could not notify user about Dify App config error: {slack_e}", exc_info=True)
                    return Response(status=500, response=error_message)

                if not isinstance(dify_app_config, dict):
                    error_message = f"Invalid Dify app configuration format: {dify_app_config}"
                    logger.error(error_message)
                    try:
                        client = WebClient(token=bot_token)
                        client.chat_postMessage(channel=channel_id, text=error_message, thread_ts=thread_ts)
                    except Exception as slack_e:
                       logger.error(f"Could not notify user about invalid Dify App config format: {slack_e}", exc_info=True)
                    return Response(status=500, response=error_message)

                dify_app_id = dify_app_config.get("app_id")
                if not dify_app_id:
                     error_message = "Could not find app_id in the Dify app configuration."
                     logger.error(error_message)
                     try:
                         client = WebClient(token=bot_token)
                         client.chat_postMessage(channel=channel_id, text=error_message, thread_ts=thread_ts)
                     except Exception as slack_e:
                        logger.error(f"Could not notify user about missing app_id: {slack_e}", exc_info=True)
                     return Response(status=500, response=error_message)

                logger.info(f"Invoking Dify app {dify_app_id} with query: '{query_text}' (Memory/conversation_id disabled)")

                # Log user_id for reference, but don't pass it or conversation_id
                user_id_for_dify = f"slack-user-{user_id}"
                logger.debug(f"(User for logging: {user_id_for_dify})")

                try:
                    # ★★★ Call Dify API with basic arguments only ★★★
                    response_from_dify = self.session.app.chat.invoke(
                        app_id=dify_app_id,
                        query=query_text,
                        inputs={},
                        response_mode="blocking"
                        # user argument removed
                        # conversation_id argument removed
                    )

                    logger.debug(f"Raw response from Dify: {response_from_dify}")

                    dify_answer = response_from_dify.get("answer")
                    if dify_answer:
                        logger.info(f"Received answer from Dify (length: {len(dify_answer)}): {dify_answer[:200]}...")
                    else:
                        logger.warning(f"Received empty or null answer from Dify. Full response: {response_from_dify}")
                        dify_answer = "(No response received)"

                    try:
                        client = WebClient(token=bot_token)
                        result = client.chat_postMessage(
                            channel=channel_id,
                            text=dify_answer,
                            thread_ts=thread_ts
                        )
                        logger.info(f"Posted message to Slack: {result.get('ts')}")
                        return Response(
                            status=200,
                            response=json.dumps({"status": "success", "message_ts": result.get('ts')}, ensure_ascii=False),
                            content_type="application/json"
                        )
                    except SlackApiError as e:
                        error_detail = f"Slack API Error posting message: {e.response['error']}"
                        logger.error(error_detail, exc_info=True)
                        return Response(status=500, response=json.dumps({"status": "error", "message": error_detail}), content_type="application/json")

                except Exception as e:
                    error_trace = traceback.format_exc()
                    logger.error(f"Error invoking Dify app or processing response:\n{error_trace}")
                    try:
                        client = WebClient(token=bot_token)
                        error_message_to_user = f"Sorry, an error occurred while processing your request with Dify. Please check the application logs."
                        client.chat_postMessage(channel=channel_id, text=error_message_to_user, thread_ts=thread_ts)
                    except Exception as slack_e:
                        logger.error(f"Could not notify user about Dify invocation error: {slack_e}", exc_info=True)

                    return Response(
                        status=500,
                        response=json.dumps({"status": "error", "message": "Internal Server Error during Dify invocation", "detail": str(e)}),
                        content_type="application/json",
                    )

            # elif event_type == "message.im":
            #    logger.info("Handling message.im event...")
            #    # Add DM handling logic here
            #    return Response(status=200, response="ok")

            else:
                logger.info(f"Ignoring unsupported event type: {event_type}")
                return Response(status=200, response="ok")

        else:
            top_level_type = data.get('type', 'Unknown type')
            logger.info(f"Ignoring top-level event type: {top_level_type}")
            return Response(status=200, response="ok")