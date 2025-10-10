import io
import os
from collections import Counter
from logging import Logger

from sqlalchemy.orm import Session

import src.core.annotation_meta as annotation
import src.core.validation_meta as validation
import src.services.webhook as oracle_db_service
from src.chain import escrow
from src.core.config import Config
from src.core.manifest import TaskManifest, parse_manifest
from src.core.oracle_events import (
    RecordingOracleEvent_JobCompleted,
    RecordingOracleEvent_SubmissionRejected,
    RecordingOracleEvent_JobCancelled
)
from src.core.storage import (
    compose_results_bucket_filename as compose_annotation_results_bucket_filename,
)
from src.core.types import OracleWebhookTypes
from src.core.validation_errors import TooFewGtError
from src.core.validation_results import ValidationFailure, ValidationSuccess
from src.handlers.process_intermediate_results import (
    parse_annotation_metafile,
    process_intermediate_results,
    serialize_validation_meta,
    filter_jobs_results_validation_meta,
)
from src.log import ROOT_LOGGER_NAME
from src.services.cloud import make_client as make_cloud_client
from src.services.cloud.utils import BucketAccessInfo
from src.utils.assignments import compute_resulting_annotations_hash
from src.utils.logging import NullLogger, get_function_logger

module_logger_name = f"{ROOT_LOGGER_NAME}.cron.webhook"


class _TaskValidator:
    def __init__(
        self, escrow_address: str, chain_id: int, manifest: TaskManifest, db_session: Session, is_cancellation_flow: bool = False
    ) -> None:
        self.escrow_address = escrow_address
        self.chain_id = chain_id
        self.manifest = manifest
        self.db_session = db_session
        self.is_cancellation_flow = is_cancellation_flow
        self.logger: Logger = NullLogger()

        self.data_bucket = BucketAccessInfo.parse_obj(Config.exchange_oracle_storage_config)

        self.annotation_meta: annotation.AnnotationMeta | None = None
        self.merged_annotations: bytes | None = None

    def set_logger(self, logger: Logger):
        self.logger = logger

    def check_file_existence(self) -> bool:
        data_bucket_client = make_cloud_client(self.data_bucket)

        annotation_meta_file_path = compose_annotation_results_bucket_filename(
            self.escrow_address,
            self.chain_id,
            annotation.ANNOTATION_RESULTS_METAFILE_NAME,
        )

        resulting_annotations_file_path = compose_annotation_results_bucket_filename(
            self.escrow_address,
            self.chain_id,
            annotation.RESULTING_ANNOTATIONS_FILE,
        )

        # if both files exist, return True else False
        return data_bucket_client.file_exists(annotation_meta_file_path) and data_bucket_client.file_exists(
            resulting_annotations_file_path
        )

    def _download_results_meta(self):
        data_bucket_client = make_cloud_client(self.data_bucket)

        annotation_meta_path = compose_annotation_results_bucket_filename(
            self.escrow_address,
            self.chain_id,
            annotation.ANNOTATION_RESULTS_METAFILE_NAME,
        )
        annotation_metafile_data = data_bucket_client.download_file(annotation_meta_path)
        self.annotation_meta = parse_annotation_metafile(io.BytesIO(annotation_metafile_data))

    def _download_annotations(self):
        assert self.annotation_meta is not None

        data_bucket_client = make_cloud_client(self.data_bucket)
        exchange_oracle_merged_annotation_path = compose_annotation_results_bucket_filename(
            self.escrow_address,
            self.chain_id,
            annotation.RESULTING_ANNOTATIONS_FILE,
        )
        merged_annotations = data_bucket_client.download_file(
            exchange_oracle_merged_annotation_path
        )
        self.merged_annotations = merged_annotations

    def _download_results(self):
        self._download_results_meta()
        self._download_annotations()

    ValidationResult = ValidationSuccess | ValidationFailure

    def _process_annotation_results(self) -> ValidationResult:
        assert self.annotation_meta is not None
        assert self.merged_annotations is not None

        # TODO: refactor further
        return process_intermediate_results(
            session=self.db_session,
            escrow_address=self.escrow_address,
            chain_id=self.chain_id,
            meta=self.annotation_meta,
            merged_annotations=io.BytesIO(self.merged_annotations),
            manifest=self.manifest,
            logger=self.logger,
            is_cancellation_flow=self.is_cancellation_flow,
        )

    def validate(self):
        self._download_results()

        validation_result = self._process_annotation_results()

        self._handle_validation_result(validation_result)

    def _compose_validation_results_bucket_filename(self, filename: str) -> str:
        return f"{self.escrow_address}@{self.chain_id}/{filename}"

    _LOW_QUALITY_REASON_MESSAGE_TEMPLATE = (
        "Annotation quality ({}) is below the required threshold ({})"
    )

    def _handle_validation_result(self, validation_result: ValidationResult):
        logger = self.logger
        escrow_address = self.escrow_address
        chain_id = self.chain_id
        db_session = self.db_session

        if isinstance(validation_result, ValidationSuccess):
            logger.info(
                f"Validation for escrow_address={escrow_address}: successful, "
                f"average annotation quality is {validation_result.average_quality * 100:.2f}%"
            )

            recor_merged_annotations_path = self._compose_validation_results_bucket_filename(
                validation.RESULTING_ANNOTATIONS_FILE,
            )

            recor_validation_meta_path = self._compose_validation_results_bucket_filename(
                validation.VALIDATION_METAFILE_NAME,
            )
            total_jobs_length = validation_result.validation_meta.get_jobs_length()
            threshold = self.manifest.validation.min_quality
            validation_meta = filter_jobs_results_validation_meta(validation_result.validation_meta, threshold=threshold)
            passed_jobs_length = validation_meta.get_jobs_length()
            logger.info(
                f"Out of {total_jobs_length} jobs, {passed_jobs_length} passed the quality threshold of {threshold * 100:.2f}%"
            )

            validation_metafile = serialize_validation_meta(validation_meta)

            storage_client = make_cloud_client(BucketAccessInfo.parse_obj(Config.storage_config))

            total_escrow_fund_amount = escrow.get_escrow_fund_amount(chain_id, escrow_address)
            job_fund_amount = total_escrow_fund_amount / total_jobs_length
            logger.info(
                f"Total escrow fund amount is {total_escrow_fund_amount} wei, "
                f"each job is funded with {job_fund_amount} wei"
            )

            # TODO: add encryption
            storage_client.create_file(
                recor_merged_annotations_path,
                validation_result.resulting_annotations,
            )
            storage_client.create_file(
                recor_validation_meta_path,
                validation_metafile,
            )

            escrow.store_results(
                chain_id,
                escrow_address,
                Config.storage_config.bucket_url() + os.path.dirname(recor_merged_annotations_path),  # noqa: PTH120
                compute_resulting_annotations_hash(validation_result.resulting_annotations),
                funds_to_reserve=(job_fund_amount * passed_jobs_length),
            )

            if self.is_cancellation_flow:
                oracle_db_service.outbox.create_webhook(
                    db_session,
                    escrow_address,
                    chain_id,
                    OracleWebhookTypes.reputation_oracle,
                    event=RecordingOracleEvent_JobCancelled(),
                )
            else:
                oracle_db_service.outbox.create_webhook(
                    db_session,
                    escrow_address,
                    chain_id,
                    OracleWebhookTypes.reputation_oracle,
                    event=RecordingOracleEvent_JobCompleted(),
                )
                oracle_db_service.outbox.create_webhook(
                    db_session,
                    escrow_address,
                    chain_id,
                    OracleWebhookTypes.exchange_oracle,
                    event=RecordingOracleEvent_JobCompleted(),
                )

        elif isinstance(validation_result, ValidationFailure):
            error_type_counts = Counter(
                type(e).__name__ for e in validation_result.rejected_jobs.values()
            )
            logger.info(
                f"Validation for escrow_address={escrow_address} failed, "
                f"rejected {len(validation_result.rejected_jobs)} jobs. "
                f"Problems: {dict(error_type_counts)}"
            )

            job_id_to_assignment_id = {
                job_meta.job_id: job_meta.assignment_id for job_meta in self.annotation_meta.jobs
            }

            oracle_db_service.outbox.create_webhook(
                db_session,
                escrow_address,
                chain_id,
                OracleWebhookTypes.exchange_oracle,
                event=RecordingOracleEvent_SubmissionRejected(
                    # TODO: send all assignments, handle rejection reason in Exchange Oracle
                    assignments=[
                        RecordingOracleEvent_SubmissionRejected.RejectedAssignmentInfo(
                            assignment_id=job_id_to_assignment_id[rejected_job_id],
                            reason=self._LOW_QUALITY_REASON_MESSAGE_TEMPLATE.format(
                                validation_result.job_results[rejected_job_id],
                                self.manifest.validation.min_quality,
                            ),
                        )
                        for rejected_job_id, reason in validation_result.rejected_jobs.items()
                        if not isinstance(reason, TooFewGtError)
                    ]
                ),
            )
        else:
            raise TypeError(f"Unexpected validation result {type(validation_result)=}")


def validate_results(
    escrow_address: str,
    chain_id: int,
    db_session: Session,
):
    logger = get_function_logger(module_logger_name)

    manifest = parse_manifest(escrow.get_escrow_manifest(chain_id, escrow_address))

    validator = _TaskValidator(
        escrow_address=escrow_address, chain_id=chain_id, manifest=manifest, db_session=db_session
    )
    validator.set_logger(logger)
    validator.validate()

def cancel_validate_results(
    escrow_address: str,
    chain_id: int,
    db_session: Session,
):
    logger = get_function_logger(module_logger_name)

    manifest = parse_manifest(escrow.get_escrow_manifest(chain_id, escrow_address))

    validator = _TaskValidator(
        escrow_address=escrow_address, chain_id=chain_id, manifest=manifest, db_session=db_session, is_cancellation_flow=True
    )
    validator.set_logger(logger)
    if validator.check_file_existence():
        validator.validate()
    else:
        logger.info(f"No annotation results found for {escrow_address}, skipping validation")