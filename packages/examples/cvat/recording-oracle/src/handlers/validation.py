import io
import os
import json
import zipfile
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
    RecordingOracleEvent_ProjectRelaunched,
)
from src.core.storage import (
    compose_results_bucket_filename as compose_annotation_results_bucket_filename,
)
from src.core.types import OracleWebhookTypes, TaskTypes
from src.core.validation_errors import TooFewGtError
from src.core.validation_results import ValidationFailure, ValidationSuccess
from src.handlers.process_intermediate_results import (
    parse_annotation_metafile,
    process_intermediate_results,
    serialize_validation_meta,
)
from src.log import ROOT_LOGGER_NAME
from src.services.cloud import make_client as make_cloud_client
from src.services.cloud.utils import BucketAccessInfo
from src.utils.assignments import compute_resulting_annotations_hash
from src.utils.logging import NullLogger, get_function_logger

module_logger_name = f"{ROOT_LOGGER_NAME}.cron.webhook"


class _TaskValidator:
    def __init__(
        self, escrow_address: str, chain_id: int, manifest: TaskManifest, db_session: Session
    ) -> None:
        self.escrow_address = escrow_address
        self.chain_id = chain_id
        self.manifest = manifest
        self.db_session = db_session
        self.logger: Logger = NullLogger()

        self.data_bucket = BucketAccessInfo.parse_obj(Config.exchange_oracle_storage_config)

        self.annotation_meta: annotation.AnnotationMeta | None = None
        self.merged_annotations: bytes | None = None
        self.merged_zip_annotations = []
        self.merged_json_data = {
            "jobs": [],
            "results": []
        }

    def set_logger(self, logger: Logger):
        self.logger = logger

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

    def _parse_filedata(self, file_data: bytes, file_type: str):

        if file_type == 'zip':
            with zipfile.ZipFile(io.BytesIO(file_data)) as archive:
                if "annotations.json" not in archive.namelist():
                    return

                with archive.open("annotations.json") as json_file:
                    annotations = json.load(json_file)

                    if len(self.merged_zip_annotations) == 0:
                        # First array: initialize merged with label as a list
                        for item in annotations:
                            self.merged_zip_annotations.append({
                                'audio_file': item['audio_file'],
                                'start': item['start'],
                                'end': item['end'],
                                'label': [item['label']]
                            })
                    else:
                        # Merge new labels into existing merged array
                        for i, item in enumerate(annotations):
                            self.merged_zip_annotations[i]['label'].append(item['label'])

        elif file_type == 'json':
            try:
                annotations = json.loads(file_data)

                # Compute offsets based on current merged data length
                job_offset = len(self.merged_json_data["jobs"])
                result_offset = len(self.merged_json_data["results"])

                # Adjust and merge jobs
                new_jobs = []
                for job in annotations.get("jobs", []):
                    adjusted_job = {
                        **job,
                        "job_id": job["job_id"] + job_offset,
                        "final_result_id": job["final_result_id"] + result_offset
                    }
                    new_jobs.append(adjusted_job)

                # Adjust and merge results
                new_results = []
                for result in annotations.get("results", []):
                    adjusted_result = {
                        **result,
                        "id": result["id"] + result_offset,
                        "job_id": result["job_id"] + job_offset
                    }
                    new_results.append(adjusted_result)

                # Merge into global store
                self.merged_json_data["jobs"].extend(new_jobs)
                self.merged_json_data["results"].extend(new_results)

            except Exception as e:
                print(f"Failed to parse JSON file: {e}")

        else:
            print(f"Unsupported file type: {file_type}")


    def _handle_validation_result(self, validation_result: ValidationResult):
        logger = self.logger
        escrow_address = self.escrow_address
        chain_id = self.chain_id
        db_session = self.db_session
        manifest = self.manifest
        project_relaunch = Config.cvat_config.relaunch_times # 0 in default scenario

        if isinstance(validation_result, ValidationSuccess):
            logger.info(
                f"Validation for escrow_address={escrow_address}: successful, "
                f"average annotation quality is {validation_result.average_quality * 100:.2f}%"
            )
            resulting_annotation_filename = validation.RESULTING_ANNOTATIONS_FILE
            validation_metafile_filename = validation.VALIDATION_METAFILE_NAME
            resulting_annotations = validation_result.resulting_annotations
            validation_metafile = serialize_validation_meta(validation_result.validation_meta)

            webhooks = oracle_db_service.inbox.get_job_finished_webhooks_by_escrow(
                db_session,
                escrow_address,
                chain_id,
            )

            if(manifest.annotation.type == TaskTypes.audio_attribute_annotation and len(webhooks) < project_relaunch):
                relaunch_escow_times = len(webhooks) + 1
                resulting_annotation_filename = f"{relaunch_escow_times}_{resulting_annotation_filename}"
                validation_metafile_filename = f"{relaunch_escow_times}_{validation_metafile_filename}"

            recor_merged_annotations_path = self._compose_validation_results_bucket_filename(
                resulting_annotation_filename,
            )

            recor_validation_meta_path = self._compose_validation_results_bucket_filename(
                validation_metafile_filename,
            )

            storage_client = make_cloud_client(BucketAccessInfo.parse_obj(Config.storage_config))

            if(manifest.annotation.type == TaskTypes.audio_attribute_annotation and project_relaunch > 0 and len(webhooks) >= project_relaunch):
                recor_files = storage_client.list_files()
                # Filter for files related to the specific address
                related_files = [
                    f for f in recor_files if f.startswith(escrow_address + '@')
                ]

                for file in related_files:
                    file_data = storage_client.download_file(file)
                    file_type = file.split('.')[-1]  # Get the extension safely
                    self._parse_filedata(file_data, file_type)

                # create resulting_annotations
                buffer = io.BytesIO()

                with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    annotations_json = json.dumps(self.merged_zip_annotations, indent=2)
                    zipf.writestr('annotations.json', annotations_json)

                # Get bytes of the resulting zip
                resulting_annotations = buffer.getvalue()
                validation_metafile = serialize_validation_meta(self.merged_json_data)

            # TODO: add encryption
            storage_client.create_file(
                recor_merged_annotations_path,
                resulting_annotations,
            )
            storage_client.create_file(
                recor_validation_meta_path,
                validation_metafile,
            )

            if(manifest.annotation.type == TaskTypes.audio_attribute_annotation and project_relaunch > 0 and len(webhooks) <= project_relaunch):
                from src.services.validation import get_task_by_escrow_address, delete_task

                task = get_task_by_escrow_address(db_session, escrow_address)
                if task:
                    delete_task(db_session, task.id)
                oracle_db_service.outbox.create_webhook(
                    db_session,
                    escrow_address,
                    chain_id,
                    OracleWebhookTypes.exchange_oracle,
                    event=RecordingOracleEvent_ProjectRelaunched(),
                )
            else:
                escrow.store_results(
                    chain_id,
                    escrow_address,
                    Config.storage_config.bucket_url() + os.path.dirname(recor_merged_annotations_path),  # noqa: PTH120
                    compute_resulting_annotations_hash(resulting_annotations),
                )

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
