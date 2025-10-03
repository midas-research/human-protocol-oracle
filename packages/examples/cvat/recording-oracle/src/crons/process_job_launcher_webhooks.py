import logging

from sqlalchemy.orm import Session
from human_protocol_sdk.constants import Status as EscrowStatus

import src.services.webhook as oracle_db_service
from src.chain.escrow import validate_escrow
from src.core.config import Config
from src.core.types import JobLauncherEventTypes, OracleWebhookTypes
from src.crons._utils import cron_job, handle_webhook
from src.db.utils import ForUpdateParams
from src.handlers.validation import cancel_validate_results
from src.log import ROOT_LOGGER_NAME
from src.models.webhook import Webhook

module_logger_name = f"{ROOT_LOGGER_NAME}.cron.webhook"


@cron_job(module_logger_name)
def process_incoming_job_launcher_webhooks(logger: logging.Logger, session: Session):
    webhooks = oracle_db_service.inbox.get_pending_webhooks(
        session,
        OracleWebhookTypes.job_launcher,
        limit=Config.cron_config.process_job_launcher_webhooks_chunk_size,
        for_update=ForUpdateParams(skip_locked=True),
    )

    for webhook in webhooks:
        with handle_webhook(logger, session, webhook, queue=oracle_db_service.inbox):
            handle_job_launcher_event(webhook, db_session=session)


def handle_job_launcher_event(webhook: Webhook, *, db_session: Session):
    assert webhook.type == OracleWebhookTypes.job_launcher

    match webhook.event_type:
        case JobLauncherEventTypes.cancellation_requested:
            validate_escrow(webhook.chain_id, webhook.escrow_address,  accepted_states=[EscrowStatus.ToCancel],)

            cancel_validate_results(
                escrow_address=webhook.escrow_address,
                chain_id=webhook.chain_id,
                db_session=db_session,
            )

        case _:
            raise AssertionError(f"Unknown job launcher event {webhook.event_type}")

