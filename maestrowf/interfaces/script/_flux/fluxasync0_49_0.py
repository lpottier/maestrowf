import logging
from math import ceil
import os
import concurrent.futures

from maestrowf.abstracts.enums import (
    CancelCode,
    JobStatusCode,
    State,
    StepPriority,
    SubmissionCode,
)
from maestrowf.abstracts.interfaces.flux import FluxInterface
from .flux0_49_0 import FluxInterface_0490

LOGGER = logging.getLogger(__name__)

try:
    import flux
except ImportError:
    LOGGER.info("Failed to import Flux. Continuing.")


# def submit_cb(fut, flux_handle):
#         # when this callback fires, the jobid will be ready
#         jobid = fut.get_id()
#         # Create a future representing the result of the job
#         result_fut = flux.job.result_async(flux_handle, jobid)
#         # attach a callback to fire when the job finishes
#         result_fut.then(result_cb)

# def result_cb(fut):
#         job = fut.get_info()
#         result = job.result.lower()
#         print(f"{job.id}: {result} with returncode {job.returncode}")

class FluxInterfaceAsync_0490(FluxInterface_0490):
    # This utility class is for Flux 0.49.0
    key = "0.49.0-async"
    # executor = flux.job.FluxExecutor()

    # @classmethod
    # def submit(
    #     cls,
    #     nodes,
    #     procs,
    #     cores_per_task,
    #     path,
    #     cwd,
    #     walltime,
    #     ngpus=0,
    #     job_name=None,
    #     force_broker=True,
    #     urgency=StepPriority.MEDIUM,
    #     waitable=False
    # ):
    #     try:
    #         # TODO: add better error handling/throwing in the class func
    #         # to enable more uniform detection/messaging when connection fails
    #         # to deal with both missing uri in allocations on non-flux machines
    #         cls.connect_to_flux()

    #         # NOTE: This previously placed everything under a broker. However,
    #         # if there's a job that schedules items to Flux, it will schedule
    #         # all new jobs to the sub-broker. Sometimes this is desired, but
    #         # it's incorrect to make that the general case. If we are asking
    #         # for a single node, don't use a broker -- but introduce a flag
    #         # that can force a single node to run in a broker.

    #         if force_broker:
    #             LOGGER.debug(
    #                 "Launch under Flux sub-broker. [force_broker=%s, "
    #                 "nodes=%d]",
    #                 force_broker,
    #                 nodes,
    #             )
    #             ngpus_per_slot = int(ceil(ngpus / nodes))
    #             jobspec = flux.job.JobspecV1.from_nest_command(
    #                 [path],
    #                 num_nodes=nodes,
    #                 cores_per_slot=cores_per_task,
    #                 num_slots=procs,
    #                 gpus_per_slot=ngpus_per_slot,
    #             )
    #         else:
    #             LOGGER.debug(
    #                 "Launch under root Flux broker. [force_broker=%s, "
    #                 "nodes=%d]",
    #                 force_broker,
    #                 nodes,
    #             )
    #             jobspec = flux.job.JobspecV1.from_command(
    #                 [path],
    #                 num_tasks=procs,
    #                 num_nodes=nodes,
    #                 cores_per_task=cores_per_task,
    #                 gpus_per_task=ngpus,
    #             )

    #         LOGGER.debug("Handle address -- %s", hex(id(cls.flux_handle)))
    #         if job_name:
    #             jobspec.setattr("system.job.name", job_name)
    #         jobspec.cwd = cwd
    #         jobspec.environment = dict(os.environ)

    #         if walltime > 0:
    #             jobspec.duration = walltime

    #         jobspec.stdout = f"{job_name}.{{{{id}}}}.out"
    #         jobspec.stderr = f"{job_name}.{{{{id}}}}.err"

    #         # Submit our job spec.

    #         # with flux.job.FluxExecutor() as executor:
    #         #         futs = [executor.submit(jobspec, waitable=waitable, urgency=urgency) for _ in range(5)]
    #         #         for f in concurrent.futures.as_completed(futs):
    #         #                 print(f.result())

    #         ftr_jobid = flux.job.submit_async(
    #             cls.flux_handle, jobspec, waitable=waitable, urgency=urgency
    #         )
    #         # ftr_jobid.then(submit_cb, cls.flux_handle)

    #         return ftr_jobid

    #     except ConnectionResetError as exception:
    #         LOGGER.error("Submission failed -- Message (%s).",
    #                      exception,
    #                      exc_info=True)
    #         jobid = -1
    #         retcode = -2
    #         submit_status = SubmissionCode.ERROR
    #     except Exception as exception:
    #         LOGGER.error("Submission failed -- Message (%s).",
    #                      exception,
    #                      exc_info=True)
    #         jobid = -1
    #         retcode = -1
    #         submit_status = SubmissionCode.ERROR

    #     return jobid, retcode, submit_status

    @classmethod
    def get_jobspec(
        cls,
        nodes,
        procs,
        cores_per_task,
        path,
        cwd,
        walltime,
        ngpus=0,
        job_name=None,
        force_broker=True,
        urgency=StepPriority.MEDIUM,
        waitable=False
    ):
        if force_broker:
            LOGGER.debug(
                "Launch under Flux sub-broker. [force_broker=%s, "
                "nodes=%d]",
                force_broker,
                nodes,
            )
            ngpus_per_slot = int(ceil(ngpus / nodes))
            jobspec = flux.job.JobspecV1.from_nest_command(
                [path],
                num_nodes=nodes,
                cores_per_slot=cores_per_task,
                num_slots=procs,
                gpus_per_slot=ngpus_per_slot,
            )
        else:
            LOGGER.debug(
                "Launch under root Flux broker. [force_broker=%s, "
                "nodes=%d]",
                force_broker,
                nodes,
            )
            jobspec = flux.job.JobspecV1.from_command(
                [path],
                num_tasks=procs,
                num_nodes=nodes,
                cores_per_task=cores_per_task,
                gpus_per_task=ngpus,
            )

        if job_name:
            jobspec.setattr("system.job.name", job_name)
        jobspec.cwd = cwd
        jobspec.environment = dict(os.environ)

        if walltime > 0:
            jobspec.duration = walltime

        jobspec.stdout = f"{job_name}.{{{{id}}}}.out"
        jobspec.stderr = f"{job_name}.{{{{id}}}}.err"

        return jobspec


    @classmethod
    def submit(
        cls,
        executor,
        jobpsecs,
        urgencies,
        waitables
    ):
        cls.connect_to_flux()
        # Submit our job spec.
        res = []
        # executor = flux.job.FluxExecutor()
        futs = [executor.submit(jobspec, waitable=waitable, urgency=urgency) for jobspec, waitable, urgency in zip(jobpsecs, waitables, urgencies)]
        # we shutdown without waiting for future so we can get ASAP their JobID
        # executor.shutdown(wait=False, cancel_futures=False)
        for f in futs:
            LOGGER.debug(f"Waiting for {f} {f._state}")
            jobid = f.jobid()
            job_meta = flux.job.get_job(cls.flux_handle, jobid)
            LOGGER.debug(f"Got step name => {jobid.f58plain} => {job_meta['name']}")
            # yield str(jobid.f58plain), job_meta["name"]
            res.append([str(jobid.f58plain), job_meta["name"]])

        LOGGER.debug(f"Returning after {len(futs)} jobs got scheduled")
        return res

    # @classmethod
    # def get_statuses(cls, joblist):
    #     # We need to import flux here, as it may not be installed on
    #     # all systems.
    #     cls.connect_to_flux()

    #     LOGGER.debug("Flux handle address -- %s", hex(id(cls.flux_handle)))

    #     # Reconstruct JobID instances from the str form of the Base58 id:
    #     # NOTE: cannot pickle JobID instances, so must store as strings and
    #     # reconstruct for use
    #     jobs_rpc = flux.job.list.JobList(
    #         cls.flux_handle,
    #         ids=[flux.job.JobID(jid) for jid in joblist])

    #     statuses = {}
    #     for jobinfo in jobs_rpc.jobs():
    #         LOGGER.debug(f"Checking status of job with id {str(jobinfo.id.f58)}")
    #         statuses[str(jobinfo.id.f58)] = cls.state(jobinfo.status_abbrev)

    #     chk_status = JobStatusCode.OK
    #     #  Print all errors accumulated in JobList RPC:
    #     try:
    #         for err in jobs_rpc.errors:
    #             chk_status = JobStatusCode.ERROR
    #             LOGGER.error("Error in JobList RPC %s", err)
    #     except EnvironmentError:
    #         pass

    #     return chk_status, statuses

